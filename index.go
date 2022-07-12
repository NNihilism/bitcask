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
