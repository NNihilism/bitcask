package bitcask

import (
	art "bitcaskDB/internal/ds/art"
	"bitcaskDB/internal/logfile"
	"errors"
	"math"
	"strconv"
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
func (db *BitcaskDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return nil, nil
	}

	encKey := db.encodeKey(key, field)
	val, err := db.getVal(idxTree, encKey, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}

	return val, err
}

// HMGet returns the values associated with the specified fields in the hash stored at the key.
// For every field that does not exist in the hash, a nil value is returned.
// Because non-existing keys are treated as empty hashes,
// running HMGET against a non-existing key will return a list of nil values.
func (db *BitcaskDB) HMGet(key []byte, fields ...[]byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var vals [][]byte
	length := len(fields)

	idxTree := db.hashIndex.trees[string(key)]
	// key not exist
	if idxTree == nil {
		for i := 0; i < length; i++ {
			vals = append(vals, nil)
		}
		return vals, nil
	}

	for _, field := range fields {
		encKey := db.encodeKey(key, field)
		val, err := db.getVal(idxTree, encKey, Hash)
		if err == ErrKeyNotFound {
			vals = append(vals, nil)
		} else if err == nil {
			vals = append(vals, val)
		}
	}
	return vals, nil
}

// HDel removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns false.
func (db *BitcaskDB) HDel(key []byte, fields ...[]byte) (int, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return 0, nil
	}

	var count int
	for _, field := range fields {
		encKey := db.encodeKey(key, field)
		ent := &logfile.LogEntry{Key: encKey, Type: logfile.TypeDelete}
		pos, err := db.writeLogEntry(ent, Hash)
		if err != nil {
			return count, err
		}
		oldVal, updated := idxTree.Delete(encKey)
		if updated {
			count++
		}

		db.sendDiscard(oldVal, updated, Hash)
		// the delete operation is also invalid.
		_, eSize := logfile.EncodeEntry(ent)
		idxNode := &indexNode{fid: pos.fid, entrySize: eSize}
		db.sendDiscard(idxNode, updated, Hash)
	}
	return count, nil

}

// HExists returns whether the field exists in the hash stored at key.
// If the hash contains field, it returns true.
// If the hash does not contain field, or key does not exist, it returns false.
func (db *BitcaskDB) HExists(key, field []byte) (bool, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return false, nil
	}

	encKey := db.encodeKey(key, field)
	val, err := db.getVal(idxTree, encKey, Hash)
	if err != nil && err != ErrKeyNotFound {
		return false, err
	}

	return val != nil, nil
}

// HLen returns the number of fields contained in the hash stored at key.
func (db *BitcaskDB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}

	return idxTree.Size()
}

// HKeys returns all field names in the hash stored at key.
func (db *BitcaskDB) HKeys(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var fields [][]byte
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return fields, nil
	}

	iter := idxTree.Iterator()
	if iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return fields, err
		}
		encKey := node.Key()
		_, field := db.decodeKey(encKey)
		fields = append(fields, field)
	}
	return fields, nil
}

// HVals return all values in the hash stored at key.
func (db *BitcaskDB) HVals(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var values [][]byte
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return values, nil
	}

	iter := idxTree.Iterator()
	if iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return values, err
		}
		val, err := db.getVal(idxTree, node.Key(), Hash)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return values, err
		}
		values = append(values, val)
	}
	return values, nil
}

// HGetAll return all fields and values of the hash stored at key.
func (db *BitcaskDB) HGetAll(key []byte) ([][]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	var pairs [][]byte
	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return pairs, nil
	}

	var index int
	pairs = make([][]byte, idxTree.Size()*2)
	iter := idxTree.Iterator()
	if iter.HasNext() {
		node, err := iter.Next()
		if err != nil {
			return pairs, err
		}
		encKey := node.Key()
		_, field := db.decodeKey(encKey)
		val, err := db.getVal(idxTree, encKey, Hash)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return pairs, err
		}
		pairs[index], pairs[index+1] = field, val
		index += 2
	}
	return pairs[:index], nil
}

// HStrLen returns the string length of the value associated with field in the hash stored at key.
// If the key or the field do not exist, 0 is returned.
func (db *BitcaskDB) HStrLen(key, field []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	idxTree := db.hashIndex.trees[string(key)]
	if idxTree == nil {
		return 0
	}

	encKey := db.encodeKey(key, field)
	val, err := db.getVal(idxTree, encKey, Hash)
	if err != nil {
		return 0
	}
	return len(val)
}

// HScan iterates over a specified key of type Hash and finds its fields and values.
// Parameter prefix will match field`s prefix, and pattern is a regular expression that also matchs the field.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of fields and values, like [field1, value1, field2, value2, etc...].
// func (db *BitcaskDB) HScan(key []byte, prefix []byte, pattern string, count int) ([][]byte, error) {
// }

// HIncrBy increments the number stored at field in the hash stored at key by increment.
// If key does not exist, a new key holding a hash is created. If field does not exist
// the value is set to 0 before the operation is performed. The range of values supported
// by HINCRBY is limited to 64bit signed integers.
func (db *BitcaskDB) HIncrBy(key, field []byte, incr int64) (int64, error) {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	idxTree := db.hashIndex.trees[string(key)]

	encKey := db.encodeKey(key, field)
	val, err := db.getVal(idxTree, encKey, Hash)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if len(val) == 0 {
		val = []byte("0")
	}

	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, ErrWrongValueType
	}

	if (incr > 0 && valInt64 > 0 && valInt64 > math.MaxInt64-incr) ||
		(incr < 0 && valInt64 < 0 && valInt64 < math.MinInt64+incr) {
		return 0, ErrIntegerOverflow
	}
	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))

	ent := &logfile.LogEntry{Key: encKey, Value: val}
	pos, err := db.writeLogEntry(ent, Hash)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(idxTree, ent, pos, true, Hash)
	if err != nil {
		return 0, err
	}

	return valInt64, nil
}
