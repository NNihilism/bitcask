package bitcask

import "bitcask/logfile"

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
	if err = db.updateIndexTree(db.strIndex.idxTree, entry, valuePos); err != nil {
		return err
	}
	return nil
}

func (db *BitcaskDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(db.strIndex.idxTree, key, String)
}
