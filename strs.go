package bitcask

import (
	"bitcask/logfile"
	"errors"
	"time"
)

// Set set key to hold the string value. If key already holds a value, it is overwritten.
func (db *BitcaskDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// write the entry to log file
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// update index
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos)
}

// SetEX set key to hold the string value and set key to timeout after the given duration.
func (db *BitcaskDB) SetEX(key, value []byte, duration time.Duration) error {
	if duration < 0 {
		return ErrInvalidTimeDuration
	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// write the entry to log file
	expiredAt := time.Now().Add(duration).Unix()
	entry := &logfile.LogEntry{Key: key, Value: value, ExpiredAt: expiredAt}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// update index
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos)
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *BitcaskDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	// The key is already exist
	if val != nil {
		return nil
	}

	// write the entry to log file
	entry := &logfile.LogEntry{Key: key, Value: value}
	valuePos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	// update index
	return db.updateIndexTree(db.strIndex.idxTree, entry, valuePos)
}

// Set set key to hold the string value. If key already holds a value, it is overwritten.
func (db *BitcaskDB) MSet(args ...[]byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if len(args) == 0 || len(args)%2 != 0 {
		return ErrWrongNumberOfArgs
	}

	for i := 0; i < len(args)-1; i += 2 {
		// write the entry to log file
		entry := &logfile.LogEntry{Key: args[i], Value: args[i+1]}
		valuePos, err := db.writeLogEntry(entry, String)
		if err != nil {
			return err
		}
		// update index
		err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos)
		if err != nil {
			return err
		}
	}
	return nil
}

// Append appends the value at the end of the old value if key already exists.
// It will be similar to Set if key does not exist.
func (db *BitcaskDB) Append(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	oldVal, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if oldVal != nil {
		value = append(oldVal, value...)
	}

	entry := &logfile.LogEntry{Key: key, Value: value}
	pos, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}
	return db.updateIndexTree(db.strIndex.idxTree, entry, pos)
}

// Get get the value of key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *BitcaskDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

// Delete value at the given key.
func (db *BitcaskDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	entry := &logfile.LogEntry{Key: key, Type: logfile.TypeDelete}

	_, err := db.writeLogEntry(entry, String)
	if err != nil {
		return err
	}

	db.strIndex.idxTree.Delete(key)
	/*
		The part of Discard is ignored...
	*/
	return nil
}

// StrLen returns the length of the string value stored at key. If the key
// doesn't exist, it returns 0.
func (db *BitcaskDB) StrLen(key []byte) int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	value, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0
	}
	return len(value)
}

// Count returns the total number of keys of String.
func (db *BitcaskDB) Count() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if db.strIndex.idxTree == nil {
		return 0
	}
	return db.strIndex.idxTree.Size()
}

// Persist remove the expiration time for the given key.
func (db *BitcaskDB) Persist(key []byte) error {
	db.strIndex.mu.RLock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.RUnlock()
		return err
	}
	db.strIndex.mu.RUnlock()
	return db.Set(key, val)
}

// Expire set the expiration time for the given key.
func (db *BitcaskDB) Expire(key []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil {
		db.strIndex.mu.Unlock()
		return err
	}
	db.strIndex.mu.Unlock()
	return db.SetEX(key, val, duration)
}

// TTL get ttl(time to live) for the given key.
func (db *BitcaskDB) TTL(key []byte) (int64, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	idxNode, err := db.getIndexNode(db.strIndex.idxTree, key, String)
	if err != nil {
		return 0, err
	}

	var ttl int64
	if idxNode.expiredAt != 0 {
		ttl = idxNode.expiredAt - time.Now().Unix()
	}
	return ttl, nil
}
