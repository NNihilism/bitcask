package bitcask

import (
	"bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/logfile"
	"bitcaskDB/internal/options"
	"bitcaskDB/internal/util"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Support String right now.
const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

func (db *BitcaskDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {
	switch dataType {
	case String:
		db.buildStrsIndex(ent, pos)
	case List:
		db.buildListIndex(ent, pos)
	case Hash:
		db.buildHashIndex(ent, pos)
	case Set:
		db.buildSetIndex(ent, pos)
	case ZSet:
		db.buildZSetIndex(ent, pos)
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

func (db *BitcaskDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
	key := ent.Key
	if ent.Type != logfile.TypeListMeta {
		key = key[4:]
	}

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.listIndex.trees[string(key)]
	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxNode)

}

func (db *BitcaskDB) buildHashIndex(ent *logfile.LogEntry, pos *valuePos) {
	encKey := ent.Key
	// fmt.Println(len(encKey))
	key, _ := db.decodeKey(encKey)

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}

	idxTree := db.hashIndex.trees[string(key)]

	if ent.Type == logfile.TypeDelete {
		idxTree.Delete(ent.Key)
		return
	}
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(ent.Key, idxNode)
}

func (db *BitcaskDB) buildSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	if db.setIndex.trees[string(ent.Key)] == nil {
		db.setIndex.trees[string(ent.Key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(ent.Key)]

	if err := db.setIndex.murhash.Write(ent.Value); err != nil {
		log.Fatalf("fail to write murmur hash: %v", err)
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	if ent.Type == logfile.TypeDelete {
		// In ROSEDB, the author code as follow:
		// idxTree.Delete(ent.Value)
		idxTree.Delete(sum)
	}

	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = ent.Value
	}
	if ent.ExpiredAt != 0 {
		idxNode.expiredAt = ent.ExpiredAt
	}
	idxTree.Put(sum, idxNode)
}

func (db *BitcaskDB) buildZSetIndex(ent *logfile.LogEntry, pos *valuePos) {
	// type == delete :	key----key, value----sum
	if ent.Type == logfile.TypeDelete {
		db.zsetIndex.indexes.ZRem(string(ent.Key), string(ent.Value))
		if db.zsetIndex.trees[string(ent.Key)] != nil {
			db.zsetIndex.trees[string(ent.Key)].Delete(ent.Value)
		}
		return
	}

	// type != delete : key----key+score, value----member
	// node := &indexNode{fid:}
	key, scoreBuf := db.decodeKey(ent.Key)
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	if err := db.zsetIndex.murhash.Write(ent.Value); err != nil {
		return
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = ent.Value
	}
	idxTree.Put(sum, idxNode)

	score, err := util.StrToFloat64(string(scoreBuf))
	if err != nil {
		return
	}

	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
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
	if idxNode.expiredAt != 0 && idxNode.expiredAt <= ts {
		// I should probably delete the node this time...
		return nil, ErrKeyNotFound
	}

	if db.opts.IndexMode == options.KeyValueMemMode {
		return idxNode.value, nil
	}

	lf := db.activateLogFile[dataType]
	if lf.Fid != idxNode.fid {
		lf = db.getArchivedLogFile(dataType, idxNode.fid)
		// lf = db.archivedLogFile[dataType][idxNode.fid]
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
				pos := &valuePos{fid: fid, offset: offset, entrySize: int(eSize)}
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
