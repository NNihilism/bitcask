package bitcask

import (
	art "bitcask/ds"
	"bitcask/logfile"
	"bitcask/options"
	"bitcask/util"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type archivedFiles map[uint32]*logfile.LogFile

type (
	BitcaskDB struct {
		activateLogFile map[DataType]*logfile.LogFile
		archivedLogFile map[DataType]archivedFiles
		discards        map[DataType]*discard
		fidMap          map[DataType][]uint32 // only used at startup, never change even though fid change.
		strIndex        *strIndex
		opts            options.Options
		mu              *sync.RWMutex
		gcState         int32
	}
	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}
	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}
	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}
)

var (
	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrKeyNotFound key not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrInvalidTimeDuration = errors.New("invalid time duration")

	// ErrIntegerOverflow overflows int64 limitations
	ErrIntegerOverflow = errors.New("increment or decrement overflow")
)

// DataType Define the data structure type.
type DataType = int8

const (
	LogFileTypeNum  = 1
	discardFilePath = "DISCARD"
)

// Support String right now.
const (
	String DataType = iota
)

// Open a bitckaskdb instance. You must call close after using it.
func Open(opts options.Options) (*BitcaskDB, error) {
	// create the dir if the path does not exist
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			log.Println("Failed to create dir")
			return nil, err
		}
	}

	db := &BitcaskDB{
		activateLogFile: make(map[DataType]*logfile.LogFile),
		archivedLogFile: make(map[DataType]archivedFiles),
		opts:            opts,
		strIndex:        newStrsIndex(),
		mu:              new(sync.RWMutex),
	}

	if err := db.loadLogFile(); err != nil {
		log.Println("Failed to load log file")
		return nil, err
	}

	if err := db.LoadIndexFromLogFiles(); err != nil {
		return nil, err
	}

	if err := db.initDiscard(); err != nil {
		return nil, err
	}

	go db.handleLogFileGC()

	return db, nil
}

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func (db *BitcaskDB) getActiveLogFile(dataType DataType) *logfile.LogFile {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activateLogFile[dataType]
}

func (db *BitcaskDB) getArchivedLogFile(dataType DataType, fid uint32) *logfile.LogFile {
	var lf *logfile.LogFile
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.archivedLogFile[dataType] != nil {
		lf = db.archivedLogFile[dataType][fid]
	}
	return lf
}

func (db *BitcaskDB) loadLogFile() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fileInfos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	fidMap := make(map[DataType][]uint32)

	for _, file := range fileInfos {
		// the file name format is log.strs.[id]
		if strings.HasPrefix(file.Name(), logfile.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fid, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(logfile.FileTypesMap[splitNames[1]])
			fidMap[typ] = append(fidMap[typ], uint32(fid))
		}
	}

	db.fidMap = fidMap

	for dataType, fids := range db.fidMap {
		if db.archivedLogFile[dataType] == nil {
			db.archivedLogFile[dataType] = make(archivedFiles)
		}
		if len(fids) == 0 {
			continue
		}

		sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })

		fType := logfile.FileType(dataType)
		for i, fid := range fids {
			lf, err := logfile.GetLogFile(db.opts.DBPath, fType, fid, db.opts.LogFileSizeThreshold)
			if err != nil {
				return err
			}
			// latest one is active log file.
			if i == len(fids)-1 {
				db.activateLogFile[dataType] = lf
			} else {
				db.archivedLogFile[dataType][fid] = lf
			}
		}
	}

	return nil
}

func (db *BitcaskDB) writeLogEntry(ent *logfile.LogEntry, dataType DataType) (*valuePos, error) {
	if err := db.initLogFile(dataType); err != nil {
		log.Println("Failed to initLogFile")
		return nil, err
	}
	activeLogFile := db.activateLogFile[dataType]
	if activeLogFile == nil {
		return nil, ErrLogFileNotFound
	}
	entryBuf, eSize := logfile.EncodeEntry(ent)
	opts := db.opts
	if int64(eSize)+activeLogFile.WriteAt > db.opts.LogFileSizeThreshold {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
		db.mu.Lock()
		// save the old log file in archived files.
		activeFileId := activeLogFile.Fid
		if db.archivedLogFile[dataType] == nil {
			db.archivedLogFile[dataType] = make(archivedFiles)
		}
		db.archivedLogFile[dataType][activeFileId] = activeLogFile

		// open a new log file.
		lf, err := logfile.GetLogFile(opts.DBPath, logfile.FileType(dataType), activeFileId+1, db.opts.LogFileSizeThreshold)
		if err != nil {
			db.mu.Unlock()
			return nil, err
		}
		db.activateLogFile[dataType] = lf
		db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
		activeLogFile = lf
		db.mu.Unlock()
	}
	offset := atomic.LoadInt64(&activeLogFile.WriteAt)
	if err := activeLogFile.Write(entryBuf); err != nil {
		return nil, err
	}

	if opts.Sync {
		if err := activeLogFile.Sync(); err != nil {
			return nil, err
		}
	}
	return &valuePos{activeLogFile.Fid, offset, eSize}, nil
}

func (db *BitcaskDB) initLogFile(dataType DataType) error {
	if db.activateLogFile[dataType] != nil {
		return nil
	}
	opts := db.opts
	lf, err := logfile.GetLogFile(opts.DBPath, logfile.FileType(dataType), logfile.InitialLogFileId, opts.LogFileSizeThreshold)
	if err != nil {
		return err
	}
	db.discards[dataType].setTotal(lf.Fid, uint32(opts.LogFileSizeThreshold))
	db.activateLogFile[dataType] = lf
	return nil
}

func (db *BitcaskDB) initDiscard() error {
	discardPath := filepath.Join(db.opts.DBPath, discardFilePath)
	if !util.PathExist(discardPath) {
		if err := os.MkdirAll(discardPath, os.ModePerm); err != nil {
			return err
		}
	}

	discards := make(map[DataType]*discard)
	for i := String; i < LogFileTypeNum; i++ {
		name := logfile.FileNamesMap[logfile.FileType(i)] + discardFileName
		d, err := newDiscard(discardPath, name, db.opts.DiscardBufferSize)
		if err != nil {
			log.Printf("init discard err:%v", err)
			return err
		}
		discards[i] = d
	}
	db.discards = discards
	return nil
}

func (db *BitcaskDB) handleLogFileGC() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}

	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()

	quitSig := make(chan os.Signal, 1) // Signal send but do not block for it, channel with buffer is necessary
	// signal.Notify(quitSig, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	signal.Notify(quitSig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	log.Println("FileGC started successfully")

	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				log.Println("log file gc is running, skip it")
				break
			}

			for i := String; i < LogFileTypeNum; i++ {
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1, int(db.opts.LogFileGCRatio))
					if err != nil {
						log.Printf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(i)

			}
		case <-quitSig:
			log.Println("FileGC quit sig...")
			return
		}
	}
}

func (db *BitcaskDB) doRunGC(dataType DataType, specifiedFid int, gcRatio int) error {
	atomic.AddInt32(&db.gcState, 1)
	defer atomic.AddInt32(&db.gcState, -1)

	maybeRewriteStrs := func(logEntry *logfile.LogEntry, fid uint32, offset int64) error {
		db.strIndex.mu.Lock()
		defer db.strIndex.mu.Unlock()

		if logEntry.Type == logfile.TypeDelete {
			return nil
		}
		ts := time.Now().Unix()
		if logEntry.ExpiredAt != 0 && logEntry.ExpiredAt < ts {
			return nil
		}

		rawVal := db.strIndex.idxTree.Get(logEntry.Key)
		if rawVal == nil {
			return nil
		}
		idxNode, _ := rawVal.(*indexNode)
		if idxNode == nil {
			return nil
		}

		if idxNode.fid == fid && idxNode.offset == offset {
			pos, err := db.writeLogEntry(logEntry, String)
			if err != nil {
				return err
			}
			// false : the archivedFile will be deleted. Do not need to call sendDiscard()
			if err = db.updateIndexTree(db.strIndex.idxTree, logEntry, pos, false, String); err != nil {
				return err
			}
		}
		return nil
	}

	activateFile := db.getActiveLogFile(dataType)
	if activateFile == nil {
		return nil
	}

	if err := db.discards[dataType].sync(); err != nil {
		return err
	}

	ccl, err := db.discards[dataType].getCCL(activateFile.Fid, db.opts.LogFileGCRatio)
	if err != nil {
		log.Printf("doRunGC err:%v", err)
		return err
	}

	for _, fid := range ccl {
		if specifiedFid >= 0 && uint32(specifiedFid) != fid {
			continue
		}

		archivedFile := db.getArchivedLogFile(dataType, fid)
		if archivedFile == nil {
			continue
		}

		for {
			var offset int64
			logEntry, eSize, err := archivedFile.ReadLogEntry(offset)
			if err != nil {
				if err == logfile.ErrEndOfEntry || err == io.EOF {
					break
				}
				return err
			}

			switch dataType {
			case String:
				err = maybeRewriteStrs(logEntry, fid, offset)
			}

			if err != nil {
				return err
			}

			offset += eSize
		}

		// delete older log file.
		db.mu.Lock()
		delete(db.archivedLogFile[dataType], fid)
		err = archivedFile.Delete()
		fmt.Printf("delete archived file err:%v", err)
		db.mu.Unlock()
		// clear discard state.
		db.discards[dataType].clear(fid)
	}

	return nil
}

func (db *BitcaskDB) sendDiscard(oldVal interface{}, updated bool, dType DataType) {
	if oldVal == nil || !updated {
		return
	}
	idxNode, _ := oldVal.(*indexNode)
	if idxNode == nil {
		return
	}
	select {
	case db.discards[dType].valChan <- idxNode:
	default:
		log.Println("send to discard chan fail!")
	}
}
