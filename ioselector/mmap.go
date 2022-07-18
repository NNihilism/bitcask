package ioselector

import (
	"bitcask/mmap"
	"io"
	"os"
)

// MMapSelector represents using memory-mapped file I/O.
type MMapIOSelector struct {
	fd     *os.File
	buf    []byte
	bufLen int64
}

// NewMMapSelector create a new mmap selector.
func NewMMapSelector(fName string, fsize int64) (IOSelector, error) {
	if fsize < 0 {
		return nil, ErrInvalidFsize
	}
	fd, err := openFile(fName, fsize)
	if err != nil {
		return nil, err
	}

	buf, err := mmap.Mmap(fd, true, fsize)
	if err != nil {
		return nil, err
	}
	return &MMapIOSelector{fd: fd, buf: buf, bufLen: int64(len(buf))}, nil
}

// Write copy slice b into mapped region(buf) at offset.
func (lm *MMapIOSelector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(lm.buf[offset:], b), nil
}

// Read copy data from mapped region(buf) into slice b at offset.
func (lm *MMapIOSelector) Read(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if offset < 0 || offset >= lm.bufLen {
		return 0, io.EOF
	}
	if length+offset > lm.bufLen {
		return 0, io.EOF
	}
	return copy(b, lm.buf[offset:]), nil

}

// Sync synchronize the mapped buffer to the file's contents on disk.
func (lm *MMapIOSelector) Sync() error {
	return mmap.Msync(lm.buf)

}

// Close sync/unmap mapped buffer and close fd.
func (lm *MMapIOSelector) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}

	return lm.fd.Close()
}

// Delete delete mapped buffer and remove file on disk.
func (lm *MMapIOSelector) Delete() error {
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	lm.buf = nil

	if err := lm.fd.Truncate(0); err != nil {
		return err
	}
	if err := lm.fd.Close(); err != nil {
		return err
	}
	return os.Remove(lm.fd.Name())
}
