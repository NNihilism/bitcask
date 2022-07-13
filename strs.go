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

func (db *BitcaskDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}

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
