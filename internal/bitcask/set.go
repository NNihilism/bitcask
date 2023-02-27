package bitcask

import (
	"bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/logfile"
	"bitcaskDB/internal/util"
)

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *BitcaskDB) SAdd(key []byte, members ...[]byte) (int, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(key)]

	var cnt int
	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}

		if err := db.setIndex.murhash.Write(mem); err != nil {
			return cnt, err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		// The elements in the set are unique
		idxNode := idxTree.Get(sum)
		if idxNode != nil {
			continue
		}

		ent := &logfile.LogEntry{Key: key, Value: mem}
		pos, err := db.writeLogEntry(ent, Set)
		if err != nil {
			return cnt, err
		}
		ent.Key = sum
		if err := db.updateIndexTree(idxTree, ent, pos, true, Set); err != nil {
			return cnt, err
		}
		cnt++
	}
	return cnt, nil
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *BitcaskDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	idxTree := db.setIndex.trees[string(key)]

	var values [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() && count > 0 {
		count--
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}

	for _, val := range values {
		if _, err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *BitcaskDB) SRem(key []byte, members ...[]byte) (int, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return 0, nil
	}

	var cnt int
	for _, mem := range members {
		if update, err := db.sremInternal(key, mem); err != nil {
			return cnt, err
		} else if update {
			cnt++
		}
	}
	return cnt, nil

}

// SIsMember returns if member is a member of the set stored at key.
func (db *BitcaskDB) SIsMember(key, member []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees[string(key)] == nil {
		return false
	}
	idxTree := db.setIndex.trees[string(key)]
	if err := db.setIndex.murhash.Write(member); err != nil {
		return false
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()
	node := idxTree.Get(sum)
	return node != nil
}

// SMembers returns all the members of the set value stored at key.
func (db *BitcaskDB) SMembers(key []byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	return db.sMembers(key)
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *BitcaskDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if db.setIndex.trees[string(key)] == nil {
		return 0
	}
	return db.setIndex.trees[string(key)].Size()
}

// SDiff returns the members of the set difference between the first set and
// all the successive sets. Returns error if no key is passed as a parameter.
func (db *BitcaskDB) SDiff(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}

	firstSet, err := db.sMembers(keys[0])
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return firstSet, nil
	}

	successiveSet := make(map[uint64]struct{})
	for _, key := range keys[1:] {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, mem := range members {
			h := util.MemHash(mem)
			if _, ok := successiveSet[h]; !ok {
				successiveSet[h] = struct{}{}
			}
		}
	}

	if len(successiveSet) == 0 {
		return firstSet, nil
	}

	var values [][]byte
	for _, mem := range firstSet {
		h := util.MemHash(mem)
		if _, ok := successiveSet[h]; !ok {
			values = append(values, mem)
		}
	}
	return values, nil
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (db *BitcaskDB) SUnion(keys ...[]byte) ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	if len(keys) == 1 {
		return db.sMembers(keys[0])
	}

	var values [][]byte
	set := make(map[uint64]struct{})
	for _, key := range keys {
		members, err := db.sMembers(key)
		if err != nil {
			return nil, err
		}
		for _, mem := range members {
			h := util.MemHash(mem)
			if _, ok := set[h]; !ok {
				set[h] = struct{}{}
				values = append(values, mem)
			}
		}
	}
	return values, nil
}

func (db *BitcaskDB) sremInternal(key []byte, member []byte) (bool, error) {
	idxTree := db.setIndex.trees[string(key)]

	if err := db.setIndex.murhash.Write(member); err != nil {
		return false, err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	oldVal, updated := idxTree.Delete(sum)
	if !updated {
		return false, nil
	}
	entry := &logfile.LogEntry{Key: key, Value: member, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return false, err
	}

	db.sendDiscard(oldVal, updated, Set)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	db.sendDiscard(idxNode, true, Set)
	return true, nil
}

// sMembers is a helper method to get all members of the given set key.
func (db *BitcaskDB) sMembers(key []byte) ([][]byte, error) {
	idxTree := db.setIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}
	var members [][]byte
	iter := idxTree.Iterator()
	for iter.HasNext() {
		node, _ := iter.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(idxTree, node.Key(), Set)
		if err != nil {
			return nil, err
		}
		members = append(members, val)
	}
	return members, nil
}

// GetStrsKeys get all stored keys of type String.
func (db *BitcaskDB) GetSetKeys() ([][]byte, error) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.setIndex.trees == nil {
		return nil, nil
	}

	var keys [][]byte
	for k := range db.setIndex.trees {
		keys = append(keys, []byte(k))
	}
	return keys, nil
}
