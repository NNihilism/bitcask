package bitcask

import (
	art "bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/logfile"
	"encoding/binary"
)

// LPush insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *BitcaskDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, true); err != nil {
			return err
		}
	}
	return nil
}

// RPush insert all the specified values at the tail of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
func (db *BitcaskDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, false); err != nil {
			return err
		}
	}
	return nil
}

// LPushX insert specified values at the head of the list stored at key,
// only if key already exists and holds a list.
// In contrary to LPUSH, no operation will be performed when key does not yet exist.
func (db *BitcaskDB) LPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, true); err != nil {
			return err
		}
	}
	return nil
}

// RPushX insert specified values at the tail of the list stored at key,
// only if key already exists and holds a list.
// In contrary to RPUSH, no operation will be performed when key does not yet exist.
func (db *BitcaskDB) RPushX(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.listIndex.trees[string(key)] == nil {
		return ErrKeyNotFound
	}

	for _, value := range values {
		if err := db.pushInternal(key, value, false); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *BitcaskDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, true)
}

// RPop Removes and returns the last elements of the list stored at key.
func (db *BitcaskDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popInternal(key, false)
}

// LMove atomically returns and removes the first/last element of the list stored at source,
// and pushes the element at the first/last element of the list stored at destination.
func (db *BitcaskDB) LMove(srcKey, dstKey []byte, srcIsLeft, dstIsLeft bool) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	val, err := db.popInternal(srcKey, srcIsLeft)

	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	if db.listIndex.trees[string(dstKey)] == nil {
		db.listIndex.trees[string(dstKey)] = art.NewART()
	}
	if err = db.pushInternal(dstKey, val, dstIsLeft); err != nil {
		return nil, err
	}

	return val, nil
}

// LLen returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
func (db *BitcaskDB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}

	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return -1
	}
	return int(tailSeq - headSeq - 1)
}

// LIndex returns the element at index in the list stored at key.
// If index is out of range, it returns nil.
func (db *BitcaskDB) LIndex(key []byte, index int) ([]byte, error) {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}

	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return nil, err
	}
	if seq >= tailSeq || seq <= headSeq {
		return nil, ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// LSet Sets the list element at index to element.
func (db *BitcaskDB) LSet(key []byte, index int, value []byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return ErrKeyNotFound
	}

	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return err
	}
	seq, err := db.listSequence(headSeq, tailSeq, index)
	if err != nil {
		return err
	}
	if seq >= tailSeq || seq <= headSeq {
		return ErrWrongIndex
	}

	encKey := db.encodeListKey(key, seq)
	ent := &logfile.LogEntry{Key: encKey, Value: value}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	return db.updateIndexTree(idxTree, ent, pos, true, List)
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element
// of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
func (db *BitcaskDB) LRange(key []byte, start, end int) (values [][]byte, err error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		err = ErrKeyNotFound
		return
	}

	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return
	}

	startSeq, err := db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return
	}

	endSeq, err := db.listSequence(headSeq, tailSeq, start)
	if err != nil {
		return
	}

	// normalize startSeq
	if startSeq <= headSeq {
		startSeq = headSeq + 1
	}
	// normalize endSeq
	if endSeq >= tailSeq {
		endSeq = tailSeq - 1
	}

	if startSeq >= tailSeq || endSeq <= headSeq || startSeq > endSeq {
		err = ErrWrongIndex
		return
	}

	for seq := startSeq; seq <= endSeq; seq++ {
		var val []byte
		encKey := db.encodeListKey(key, seq)
		val, err = db.getVal(idxTree, encKey, List)
		if err != nil {
			return
		}
		values = append(values, val)
	}

	return

}

func (db *BitcaskDB) popInternal(key []byte, isLeft bool) ([]byte, error) {
	idxTree := db.listIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}

	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return nil, err
	}

	// reset
	if tailSeq-headSeq-1 <= 0 {
		headSeq = initialListSeq
		tailSeq = headSeq + 1
		err = db.saveListMeta(idxTree, key, headSeq, tailSeq)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	seq := headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}

	encKey := db.encodeListKey(key, seq) // seq(len = 4) + key
	val, err := db.getVal(idxTree, encKey, List)
	if err != nil {
		return nil, err
	}

	// update
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	db.saveListMeta(idxTree, key, headSeq, tailSeq)

	// delete
	oldVal, updated := idxTree.Delete(encKey)
	db.sendDiscard(oldVal, updated, List)

	ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return nil, err
	}
	// delete itself
	_, eSize := logfile.EncodeEntry(ent)
	idxNode := &indexNode{fid: pos.fid, entrySize: eSize}
	db.sendDiscard(idxNode, updated, List)

	return val, nil
}

func (db *BitcaskDB) pushInternal(key []byte, val []byte, isLeft bool) error {
	idxTree := db.listIndex.trees[string(key)]
	headSeq, tailSeq, err := db.ListMeta(idxTree, key)
	if err != nil {
		return err
	}

	seq := headSeq
	if !isLeft {
		seq = tailSeq
	}

	encKey := db.encodeListKey(key, seq) // seq(len = 4) + key
	ent := &logfile.LogEntry{Key: encKey, Value: val}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(idxTree, ent, pos, true, List)
	if err != nil {
		return err
	}

	if isLeft {
		headSeq--
	} else {
		tailSeq++
	}

	return db.saveListMeta(idxTree, key, headSeq, tailSeq)
}

// ListMeta Get the head/tail sequence of the list corresponding to the key
func (db *BitcaskDB) ListMeta(idxTree *art.AdaptiveRadixTree, key []byte) (uint32, uint32, error) {
	val, err := db.getVal(idxTree, key, List)
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = headSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:])
	}

	return headSeq, tailSeq, nil
}

func (db *BitcaskDB) encodeListKey(key []byte, seq uint32) []byte {
	buf := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(buf[:4], seq)
	copy(buf[4:], key)
	return buf
}

func (db *BitcaskDB) decodeListKey(buf []byte) ([]byte, uint32) {
	seq := binary.LittleEndian.Uint32(buf[:4])
	key := make([]byte, len(buf[4:]))
	copy(key, buf[4:])
	return key, seq
}

func (db *BitcaskDB) saveListMeta(idxTree *art.AdaptiveRadixTree, key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:], tailSeq)
	ent := &logfile.LogEntry{Key: key, Value: buf, Type: logfile.TypeListMeta}
	pos, err := db.writeLogEntry(ent, List)
	if err != nil {
		return err
	}
	return db.updateIndexTree(idxTree, ent, pos, true, List)
}

// listSequence just convert logical index to physical seq.
// whether physical seq is legal or not, just convert it
func (db *BitcaskDB) listSequence(headSeq, tailSeq uint32, index int) (uint32, error) {
	var seq uint32
	if index >= 0 {
		seq = headSeq + uint32(index) + 1
	} else {
		seq = tailSeq - uint32(-index)
	}
	return seq, nil
}
