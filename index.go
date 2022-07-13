package bitcask

import (
	art "bitcask/ds"
	"bitcask/logfile"
	"bitcask/options"
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

func (db *BitcaskDB) updateIndexTree(idxTree *art.AdaptiveRadixTree, entry *logfile.LogEntry, pos *valuePos) error {
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	// in KeyValueMemMode, both key and value will store in memory.
	if db.opts.IndexMode == options.KeyValueMemMode {
		idxNode.value = entry.Value
	}
	if entry.ExpiredAt != 0 {
		idxNode.expiredAt = entry.ExpiredAt
	}
	idxTree.Put(entry.Key, idxNode)
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
