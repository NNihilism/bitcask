package bitcask

import (
	"bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/logfile"
	"bitcaskDB/internal/util"
)

// ZAdd adds the specified member with the specified score to the sorted set stored at key.
func (db *BitcaskDB) ZAdd(key []byte, score float64, member []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	scoreBuf := []byte(util.Float64ToStr(score))
	zsetKey := db.encodeKey(key, scoreBuf)
	ent := &logfile.LogEntry{Key: zsetKey, Value: member}
	pos, err := db.writeLogEntry(ent, ZSet)
	if err != nil {
		return err
	}
	ent.Key = sum // Change the key...Otherwise, you will have trouble deleting nodes
	if err := db.updateIndexTree(idxTree, ent, pos, true, ZSet); err != nil {
		return err
	}
	db.zsetIndex.indexes.ZAdd(string(key), score, string(sum))
	return nil
}

// ZScore returns the score of member in the sorted set at key.
func (db *BitcaskDB) ZScore(key, member []byte) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return false, 0
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	return db.zsetIndex.indexes.ZScore(string(key), string(sum))
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (db *BitcaskDB) ZRem(key, member []byte) error {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	ok := db.zsetIndex.indexes.ZRem(string(key), string(sum))
	if !ok {
		return nil
	}

	oldVal, updated := idxTree.Delete(sum)
	db.sendDiscard(oldVal, updated, ZSet)

	// The key(just key) here is different from the key(key-score) in writing
	entry := &logfile.LogEntry{Key: key, Value: sum, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, ZSet)
	if err != nil {
		return err
	}
	node := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	db.sendDiscard(node, true, ZSet)

	return nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (db *BitcaskDB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.indexes.ZCard(string(key))
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (db *BitcaskDB) ZRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, false)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
func (db *BitcaskDB) ZRevRange(key []byte, start, stop int) ([][]byte, error) {
	return db.zRangeInternal(key, start, stop, true)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (db *BitcaskDB) ZRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, false)

}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *BitcaskDB) ZRevRank(key []byte, member []byte) (ok bool, rank int) {
	return db.zRankInternal(key, member, true)
}

func (db *BitcaskDB) zRangeInternal(key []byte, start, stop int, rev bool) ([][]byte, error) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.zsetIndex.trees[string(key)]

	var res [][]byte
	var values []interface{}
	if rev {
		values = db.zsetIndex.indexes.ZRevRange(string(key), start, stop)
	} else {
		values = db.zsetIndex.indexes.ZRange(string(key), start, stop)
	}
	for _, val := range values {
		v, _ := val.(string)
		if val, err := db.getVal(idxTree, []byte(v), ZSet); err != nil {
			return nil, err
		} else {
			res = append(res, val)
		}
	}
	return res, nil
}

func (db *BitcaskDB) zRankInternal(key []byte, member []byte, rev bool) (ok bool, rank int) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.zsetIndex.trees[string(key)] == nil {
		return
	}

	if err := db.zsetIndex.murhash.Write(member); err != nil {
		return
	}
	sum := db.zsetIndex.murhash.EncodeSum128()
	db.zsetIndex.murhash.Reset()

	var result int64
	if rev {
		result = db.zsetIndex.indexes.ZRevRank(string(key), string(sum))
	} else {
		result = db.zsetIndex.indexes.ZRank(string(key), string(sum))
	}
	if result != -1 {
		ok = true
		rank = int(result)
	}
	return
}

// ZScore returns the score of member in the sorted set at key.
func (db *BitcaskDB) ZKeys() [][]byte {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return db.zsetIndex.indexes.ZKeys()
}

// ZScore returns the score of member in the sorted set at key.
func (db *BitcaskDB) ZMembers(key []byte) [][]byte {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	return util.StrArrToByteArr(db.zsetIndex.indexes.ZMembers(string(key)))
}
