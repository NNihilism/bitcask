package bitcask

import (
	art "bitcask/ds"
	"bitcask/logfile"
	"bitcask/options"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func (db *BitcaskDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	}
}

func (db *BitcaskDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
	ts := time.Now().Unix()
	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
		db.strIndex.idxTree.Delete(ent.Key)
		return
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = ent.Value
	}
	db.strIndex.idxTree.Put(ent.Key, idxNode)
}

func (db *BitcaskDB) updateIndexTree(idxTree *art.AdaptiveRadixTree,
	entry *logfile.LogEntry, pos *valuePos, sendDiscard bool, dType DataType) error {

	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = entry.Value
	}
	if entry.ExpiredAt != 0 {
		idxNode.expiredAt = entry.ExpiredAt
	}
	oldVal, updated := idxTree.Put(entry.Key, idxNode)
	if sendDiscard {
		db.sendDiscard(oldVal, updated, dType)
	}
	return nil
}

func (db *BitcaskDB) getVal(idxTree *art.AdaptiveRadixTree, key []byte, dataType DataType) ([]byte, error) {
	rawData := idxTree.Get(key)
	if rawData == nil {
		return nil, ErrKeyNotFound
	}
	idxNode := rawData.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt < ts {
		return nil, ErrKeyNotFound
	}

	if db.opts.IndexMode == options.KeyValueMemMode {
		return idxNode.value, nil
	}

	lf := db.activateLogFile[dataType]
	if lf.Fid != idxNode.fid {
		lf = db.archivedLogFile[dataType][idxNode.fid]
	}
	if lf == nil {
		return nil, ErrKeyNotFound
	}
	logEntry, _, err := lf.ReadLogEntry(idxNode.offset)
	if err != nil {
		return nil, err
	}
	if logEntry.Type == logfile.TypeDelete || (logEntry.ExpiredAt != 0 && logEntry.ExpiredAt < ts) {
		return nil, ErrKeyNotFound
	}

	return logEntry.Value, nil
}

func (db *BitcaskDB) getIndexNode(idxTree *art.AdaptiveRadixTree, key []byte, dataType DataType) (*indexNode, error) {
	rawData := idxTree.Get(key)
	if rawData == nil {
		return nil, ErrKeyNotFound
	}
	idxNode := rawData.(*indexNode)
	if idxNode == nil {
		return nil, ErrKeyNotFound
	}

	ts := time.Now().Unix()
	if idxNode.expiredAt != 0 && idxNode.expiredAt < ts {
		return nil, ErrKeyNotFound
	}
	return idxNode, nil
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
	wg.Add(LogFileTypeNum)
	for i := 0; i < LogFileTypeNum; i++ {
		go iteratorAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}
