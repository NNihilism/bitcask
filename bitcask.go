package bitcask

import (
	art "bitcask/ds"
	"bitcask/logfile"
	"bitcask/options"
	"bitcask/util"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type archivedFiles map[uint32]*logfile.LogFile

type (
	BitcaskDB struct {
		activateLogFile map[DataType]*logfile.LogFile
		archivedLogFile map[DataType]archivedFiles
		fidMap          map[DataType][]uint32 // only used at startup, never change even though fid change.
		strIndex        *strIndex
		opts            options.Options
		mu              *sync.RWMutex
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
)

// DataType Define the data structure type.
type DataType = int8

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

	return db, nil
}

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
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

func (db *BitcaskDB) LoadIndexFromLogFiles() error {
	iteratorAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()
		fids := db.fidMap[dataType]
		if len(fids) == 0 {
			return
		}
		sort.Slice(fids, func(i, j int) bool { return fids[i] < fids[j] })

		for i, fid := range fids {

			var logFile *logfile.LogFile
			if i == len(fids)-1 {
				logFile = db.activateLogFile[dataType]
			} else {
				logFile = db.archivedLogFile[dataType][fid]
			}
			if logFile == nil {
				log.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				entry, eSize, err := logFile.ReadLogEntry(offset)
				if err != nil {
					if err == io.EOF || err == logfile.ErrEndOfEntry {
						break
					}
					log.Fatalf("read log entry from file err, failed to open db")
				}

				pos := &valuePos{fid: fid, offset: offset}
				db.buildIndex(dataType, entry, pos)
				offset += eSize
			}
			// set latest log file`s WriteAt.
			if i == len(fids)-1 {
				atomic.StoreInt64(&logFile.WriteAt, offset)
			}
		}
	}
	wg := new(sync.WaitGroup)
	wg.Add(logfile.LogFileTypeNum)
	for i := 0; i < logfile.LogFileTypeNum; i++ {
		go iteratorAndHandle(DataType(i), wg)
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
		activeLogFile = lf
		db.mu.Unlock()
	}
	offset := atomic.LoadInt64(&activeLogFile.WriteAt)
	if err := activeLogFile.WriteLogEntry(entryBuf); err != nil {
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
	lf, err := logfile.GetLogFile(db.opts.DBPath, logfile.FileType(dataType), logfile.InitialLogFileId, db.opts.LogFileSizeThreshold)
	if err != nil {
		return err
	}
	db.activateLogFile[dataType] = lf
	return nil
}
