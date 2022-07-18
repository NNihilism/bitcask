package ioselector

import (
	"os"
)

// FileIOSelector represents using standard file I/O.
type FileIOSelector struct {
	fd *os.File // system file descriptor.
}

func (fio *FileIOSelector) Write(b []byte, offset int64) (int, error) {
	return fio.fd.WriteAt(b, offset)
}

// NewFileIOSelector create a new file io selector.
func NewFileIOSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}
	return &FileIOSelector{fd: file}, nil
}

// Read a slice from offset.
// It returns the number of bytes read and any error encountered.
func (fio *FileIOSelector) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
func (fio *FileIOSelector) Sync() error {
	return fio.fd.Sync()
}

// Close closes the File, rendering it unusable for I/O.
// It will return an error if it has already been closed.
func (fio *FileIOSelector) Close() error {
	return fio.fd.Close()
}

// Delete delete the file.
// Must close it before delete, and will unmap if in MMapSelector.
func (fio *FileIOSelector) Delete() error {
	if err := fio.fd.Close(); err != nil {
		return err
	}
	return os.Remove(fio.fd.Name())
}
