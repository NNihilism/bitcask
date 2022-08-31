package bitcask

import (
	"bitcask/ds/art"
	"bitcask/logfile"
	"fmt"
)

// SAdd add the specified members to the set stored at key.
// Specified members that are already a member of this set are ignored.
// If key does not exist, a new set is created before adding the specified members.
func (db *BitcaskDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.setIndex.trees[string(key)]

	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}

		if err := db.setIndex.murhash.Write(mem); err != nil {
			fmt.Println("err")
			return err
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
			return err
		}
		ent.Key = sum
		if err := db.updateIndexTree(idxTree, ent, pos, true, Set); err != nil {
			return err
		}
	}
	return nil
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
		if err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *BitcaskDB) SRem(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		return nil
	}

	for _, mem := range members {
		if err := db.sremInternal(key, mem); err != nil {
			return err
		}
	}
	return nil

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

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *BitcaskDB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	if db.setIndex.trees[string(key)] == nil {
		return 0
	}
	return db.setIndex.trees[string(key)].Size()
}

func (db *BitcaskDB) sremInternal(key []byte, member []byte) error {
	idxTree := db.setIndex.trees[string(key)]

	if err := db.setIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	oldVal, updated := idxTree.Delete(sum)
	if !updated {
		return nil
	}
	entry := &logfile.LogEntry{Key: key, Value: member, Type: logfile.TypeDelete}
	pos, err := db.writeLogEntry(entry, Set)
	if err != nil {
		return err
	}

	db.sendDiscard(oldVal, updated, Set)
	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: pos.entrySize}
	db.sendDiscard(idxNode, true, Set)
	return nil
}
