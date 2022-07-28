package bitcask

import (
	art "bitcask/ds"
	"bitcask/logfile"
	"errors"
)

// HSet sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
// Return num of elements in hash of the specified key.
// Multiple field-value pair is accepted. Parameter order should be like "key", "field", "value", "field", "value"...
func (db *BitcaskDB) HSet(key []byte, args ...[]byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if len(args) == 0 || len(args)&1 == 1 {
		return ErrWrongNumberOfArgs
	}

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	for i := 0; i < len(args); i += 2 {
		field, value := args[i], args[i+1]
		encKey := db.encodeKey(key, field)
		ent := &logfile.LogEntry{Key: encKey, Value: value}
		pos, err := db.writeLogEntry(ent, Hash)
		if err != nil {
			return err
		}
		/*
			In rosedb, the author rewrite the entrySize and update. I can't understand and feel unreasonable...
			Beacause the GCRatio is calculated as a percentage of invalid record size to total file size

			ent := &logfile.LogEntry{Key: field, Value: value}
			_, size := logfile.EncodeEntry(entry)
			valuePos.entrySize = size
			err = db.updateIndexTree(idxTree, ent, valuePos, true, Hash)
		*/
		if err = db.updateIndexTree(idxTree, ent, pos, true, Hash); err != nil {
			return err
		}
	}
	return nil
}

// HSetNX sets the given value only if the field doesn't exist.
// If the key doesn't exist, new hash is created.
// If field already exist, HSetNX doesn't have side effect.
func (db *BitcaskDB) HSetNX(key, field, value []byte) (bool, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]
	encKey := db.encodeKey(key, field)
	val, err := db.getVal(idxTree, encKey, Hash)
	if val != nil { // field already exist
		return false, nil
	}
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return false, err
	}

	ent := &logfile.LogEntry{Key: encKey, Value: value}
	pos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return false, err
	}
	err = db.updateIndexTree(idxTree, ent, pos, true, Hash)
	if err != nil {
		return false, err
	}
	return true, nil

}

// HGet returns the value associated with field in the hash stored at key.
// func (db *RoseDB) HGet(key, field []byte) ([]byte, error) {}
