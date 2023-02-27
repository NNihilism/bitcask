package bitcask

import (
	art "bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/ds/zset"
	"bitcaskDB/internal/log"
	"bitcaskDB/internal/logfile"
	"bitcaskDB/internal/options"
	"bitcaskDB/internal/util"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
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
		listIndex       *listIndex
		hashIndex       *hashIndex // Hash indexes.
		setIndex        *setIndex  // Set indexes.
		zsetIndex       *zsetIndex // Sorted set indexes.
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
	listIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}
	hashIndex struct {
		mu    *sync.RWMutex
		trees map[string]*art.AdaptiveRadixTree
	}
	setIndex struct {
		mu      *sync.RWMutex
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		murhash *util.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
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

	// ErrWrongIndex index is out of range
	ErrWrongIndex = errors.New("index is out of range")

	// ErrWrongValueType value is not a number
	ErrWrongValueType = errors.New("value is not an integer")
)

// DataType Define the data structure type.
type DataType = int8

const (
	LogFileTypeNum   = 5
	discardFilePath  = "DISCARD"
	initialListSeq   = math.MaxUint32 / 2
	encodeHeaderSize = 10
)

// Open a bitckaskdb instance. You must call close after using it.
func Open(opts options.Options) (*BitcaskDB, error) {
	// create the dir if the path does not exist
	// TODO 如果路径存在，是否需要删除原路径文件
	log.Info("path:", opts.DBPath)
	if !util.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			log.Errorf("Failed to create dir")
			return nil, err
		}
	}

	db := &BitcaskDB{
		activateLogFile: make(map[DataType]*logfile.LogFile),
		archivedLogFile: make(map[DataType]archivedFiles),
		opts:            opts,
		strIndex:        newStrsIndex(),
		listIndex:       newListIndex(),
		hashIndex:       newHashIndex(),
		setIndex:        newSetIndex(),
		zsetIndex:       newZSetIndex(),
		mu:              new(sync.RWMutex),
	}

	if err := db.loadLogFile(); err != nil {
		log.Errorf("load log file err : %v", err)
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
func newListIndex() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}
func newHashIndex() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIndex() *setIndex {
	return &setIndex{
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

func newZSetIndex() *zsetIndex {
	return &zsetIndex{
		murhash: util.NewMurmur128(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
		indexes: zset.New(),
	}
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
		log.Errorf("init log file err : %v", err)
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
			log.Errorf("init discard err:%v", err)
			return err
		}
		discards[i] = d
	}
	db.discards = discards
	return nil
}

func (db *BitcaskDB) encodeKey(key, subKey []byte) []byte {
	header := make([]byte, encodeHeaderSize)
	var index int
	index += binary.PutUvarint(header[index:], uint64(len(key)))
	index += binary.PutUvarint(header[index:], uint64(len(subKey)))
	length := len(key) + len(subKey)
	if length > 0 {
		buf := make([]byte, length+index)
		copy(buf, header[:index])
		copy(buf[index:index+len(key)], key)
		copy(buf[index+len(key):], subKey)
		// fmt.Println("buf:", string(buf))
		return buf
	}
	return header[:index]
}

func (db *BitcaskDB) decodeKey(buf []byte) ([]byte, []byte) {
	/*
		var index int
		keySize, i := binary.Varint(key[index:])
		index += i
		_, i = binary.Varint(key[index:])
		index += i
		sep := index + int(keySize)
		return key[index:sep], key[sep:]
	*/
	var offset int
	keySize, n := binary.Uvarint(buf[offset:])
	offset += n
	_, n = binary.Uvarint(buf[offset:])
	offset += n

	sep := offset + int(keySize)
	key := append([]byte{}, buf[offset:sep]...)
	subKey := append([]byte{}, buf[sep:]...)

	return key, subKey

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

	// log.Info("FileGC started successfully")

	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&db.gcState) > 0 {
				log.Info("log file gc is running, skip it")
				break
			}

			for i := String; i < LogFileTypeNum; i++ {
				go func(dataType DataType) {
					err := db.doRunGC(dataType, -1)
					if err != nil {
						log.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(i)

			}
		case <-quitSig:
			log.Info("FileGC quit sig...")
			return
		}
	}
}

func (db *BitcaskDB) doRunGC(dataType DataType, specifiedFid int) error {
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

	maybeRewriteList := func(logEntry *logfile.LogEntry, fid uint32, offset int64) error {
		db.listIndex.mu.Lock()
		defer db.listIndex.mu.Unlock()

		if logEntry.Type == logfile.TypeDelete {
			return nil
		}

		treeKey := logEntry.Key
		if logEntry.Type != logfile.TypeListMeta {
			treeKey, _ = db.decodeListKey(treeKey)
		}

		idxTree := db.listIndex.trees[string(treeKey)]
		if idxTree == nil {
			return nil
		}
		idxValue := idxTree.Get(logEntry.Key)
		if idxValue == nil {
			return nil
		}
		idxNode, _ := idxValue.(*indexNode)
		if idxNode != nil && idxNode.fid == fid && idxNode.offset == offset {
			valuePos, err := db.writeLogEntry(logEntry, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, logEntry, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteHash := func(logEntry *logfile.LogEntry, fid uint32, offset int64) error {
		db.hashIndex.mu.Lock()
		defer db.hashIndex.mu.Unlock()

		if logEntry.Type == logfile.TypeDelete {
			return nil
		}

		key, _ := db.decodeKey(logEntry.Key)
		idxTree := db.hashIndex.trees[string(key)]
		if idxTree == nil {
			return nil
		}
		idxValue := idxTree.Get(logEntry.Key)
		if idxValue == nil {
			return nil
		}
		idxNode, _ := idxValue.(*indexNode)
		if idxNode != nil && idxNode.fid == fid && idxNode.offset == offset {
			valuePos, err := db.writeLogEntry(logEntry, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, logEntry, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteSets := func(logEntry *logfile.LogEntry, fid uint32, offset int64) error {
		db.setIndex.mu.Lock()
		defer db.setIndex.mu.Unlock()

		if logEntry.Type == logfile.TypeDelete {
			return nil
		}

		idxTree := db.setIndex.trees[string(logEntry.Key)]
		if idxTree == nil {
			return nil
		}

		if err := db.setIndex.murhash.Write(logEntry.Value); err != nil {
			fmt.Println("err")
			return err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		idxValue := idxTree.Get(sum)
		if idxValue == nil {
			return nil
		}
		idxNode, _ := idxValue.(*indexNode)
		if idxNode != nil && idxNode.fid == fid && idxNode.offset == offset {
			valuePos, err := db.writeLogEntry(logEntry, List)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, logEntry, valuePos, false, List); err != nil {
				return err
			}
		}
		return nil
	}

	maybeRewriteZSet := func(logEntry *logfile.LogEntry, fid uint32, offset int64) error {
		db.zsetIndex.mu.Lock()
		defer db.zsetIndex.mu.Unlock()

		if logEntry.Type == logfile.TypeDelete {
			return nil
		}

		key, _ := db.decodeKey(logEntry.Key)
		idxTree := db.zsetIndex.trees[string(key)]
		if idxTree == nil {
			return nil
		}

		if err := db.zsetIndex.murhash.Write(logEntry.Value); err != nil {
			return err
		}
		sum := db.zsetIndex.murhash.EncodeSum128()
		db.zsetIndex.murhash.Reset()

		idxValue := idxTree.Get(sum)
		if idxValue == nil {
			return nil
		}
		idxNode := idxValue.(*indexNode)
		if idxNode != nil && idxNode.fid == fid && idxNode.offset == offset {
			valuePos, err := db.writeLogEntry(logEntry, ZSet)
			if err != nil {
				return err
			}
			if err = db.updateIndexTree(idxTree, logEntry, valuePos, false, ZSet); err != nil {
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
		log.Errorf("doRunGC err:%v", err)
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
			case List:
				err = maybeRewriteList(logEntry, fid, offset)
			case Hash:
				err = maybeRewriteHash(logEntry, fid, offset)
			case Set:
				err = maybeRewriteSets(logEntry, fid, offset)
			case ZSet:
				err = maybeRewriteZSet(logEntry, fid, offset)
			}

			if err != nil {
				return err
			}

			offset += eSize
		}

		// delete older log file.
		db.mu.Lock()
		delete(db.archivedLogFile[dataType], fid)
		if err = archivedFile.Delete(); err != nil {
			fmt.Printf("delete archived file err:%v", err)
		}
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
		log.Error("send to discard chan fail!")
	}
}

func (db *BitcaskDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// close and sync the active file.
	for _, activateFile := range db.activateLogFile {
		if err := activateFile.Sync(); err != nil {
			return err
		}
		if err := activateFile.Close(); err != nil {
			return err
		}
	}

	// close the archived files.
	for _, archived := range db.archivedLogFile {
		for _, file := range archived {
			if err := file.Close(); err != nil {
				return err
			}
		}
	}

	// close the mmap.
	for _, discard := range db.discards {
		if err := discard.sync(); err != nil {
			return err
		}
		if err := discard.close(); err != nil {
			return err
		}
	}
	// close discard channel.
	for _, dis := range db.discards {
		dis.closeChan()
	}
	db.strIndex = nil

	return nil
}
