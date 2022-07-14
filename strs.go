package bitcask

import (
	"bitcask/logfile"
	"bytes"
	"errors"
	"math"
	"strconv"
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

// incrDecrBy is a helper method for Incr, IncrBy, Decr, and DecrBy methods. It updates the key by incr.
func (db *BitcaskDB) incrDecrBy(key []byte, incr int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	val, err := db.getVal(db.strIndex.idxTree, key, String)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return 0, err
	}
	if bytes.Equal(val, nil) {
		val = []byte("0")
	}

	valInt64, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, err
	}

	if (valInt64 < 0 && incr < 0 && valInt64 < math.MinInt64-incr) ||
		(valInt64 > 0 && incr > 0 && valInt64 > math.MaxInt64-incr) {
		return 0, ErrIntegerOverflow
	}

	valInt64 += incr
	val = []byte(strconv.FormatInt(valInt64, 10))
	ent := &logfile.LogEntry{Key: key, Value: val}
	pos, err := db.writeLogEntry(ent, String)
	if err != nil {
		return 0, err
	}
	err = db.updateIndexTree(db.strIndex.idxTree, ent, pos)
	if err != nil {
		return 0, err
	}
	return valInt64, nil
}

// Decr decrements the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *BitcaskDB) Decr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -1)
}

// DecrBy decrements the number stored at key by decr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after decrementing the value.
func (db *BitcaskDB) DecrBy(key []byte, decr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, -decr)
}

// Incr increments the number stored at key by one. If the key does not exist,
// it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *BitcaskDB) Incr(key []byte) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, 1)
}

// IncrBy increments the number stored at key by incr. If the key doesn't
// exist, it is set to 0 before performing the operation. It returns ErrWrongKeyType
// error if the value is not integer type. Also, it returns ErrIntegerOverflow
// error if the value exceeds after incrementing the value.
func (db *BitcaskDB) IncrBy(key []byte, incr int64) (int64, error) {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	return db.incrDecrBy(key, incr)
}

// Scan iterates over all keys of type String and finds its value.
// Parameter prefix will match key`s prefix, and pattern is a regular expression that also matchs the key.
// Parameter count limits the number of keys, a nil slice will be returned if count is not a positive number.
// The returned values will be a mixed data of keys and values, like [key1, value1, key2, value2, etc...].
func (db *BitcaskDB) Scan(prefix []byte, pattern string, count int) ([][]byte, error) {
	return nil, nil
}
