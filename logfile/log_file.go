package logfile

import (
	"bitcask/ioselector"
	"bitcask/util"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
)

type LogFile struct {
	Fid        uint32 // file id
	WriteAt    int64  // offset
	IoSelector ioselector.IOSelector
	FileLock   // 匿名结构体
}

type FileType int8

const (
	FilePrefix       = "log."
	InitialLogFileId = 0    // InitialLogFileId initial log file id: 0.
	FilePerm         = 0644 // FilePerm default permission of the newly created log file.
)

var (
	ErrInvalidDir = errors.New("logfile : directory does not exist")

	// ErrEndOfEntry end of entry in log file.
	ErrEndOfEntry = errors.New("logfile: end of entry in log file")

	// ErrInvalidCrc invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	// ErrWriteSizeNotEqual write size is not equal to entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")
)

const (
	Strs FileType = iota
)

var (
	FileTypesMap = map[string]FileType{
		"strs": Strs,
	}
	FileNamesMap = map[FileType]string{
		Strs: "log.strs.",
	}
)

// GetLogFile open an existing or create a new log file.
func GetLogFile(path string, fType FileType, fid uint32, fsize int64) (lf *LogFile, err error) {
	if !util.PathExist(path) {
		err = ErrInvalidDir
		return
	}
	lf = &LogFile{Fid: fid}
	fileName := getFileName(path, fType, fid)

	ioSelector, err := ioselector.NewFileIOSelector(fileName, fsize)
	if err != nil {
		return
	}
	lf.IoSelector = ioSelector

	return
}

// ReadLogEntry read a LogEntry from log file at offset.
// It returns a LogEntry, entry size and an error, if any.
// If offset is invalid, the err is io.EOF.
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	// read entry header
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, size := decodeHeader(headerBuf)
	// the end of entries
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}

	e := &LogEntry{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	kSize, vSize := int64(header.kSize), int64(header.vSize)
	var entrySize = size + kSize + vSize

	// read entry key and value.
	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:kSize]
		e.Value = kvBuf[kSize:]
	}
	// crc32 check.
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, entrySize, nil
}

func getFileName(path string, fType FileType, fid uint32) string {
	return path + string(os.PathSeparator) + FileNamesMap[fType] + fmt.Sprintf("%09d", fid)
}

func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)

	if err != nil {
		return
	}
	return
}

// EncodeEntry will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//         |--------------------------crc check---------------------------|
func EncodeEntry(entry *LogEntry) ([]byte, int) {
	if entry == nil {
		return nil, 0
	}
	header := make([]byte, MaxHeaderSize)

	// encode header
	header[4] = byte(entry.Type)
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(entry.Key)))

	index += binary.PutVarint(header[index:], int64(len(entry.Value)))

	index += binary.PutVarint(header[index:], entry.ExpiredAt)

	var size = index + len(entry.Key) + len(entry.Value) // len of header + len of key + len of value
	buf := make([]byte, size)

	copy(buf[:index], header[:])
	// key and value.
	copy(buf[index:], entry.Key)
	copy(buf[index+len(entry.Key):], entry.Value)

	// crc32.
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)

	return buf, size
}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) < 4 {
		return nil, 0
	}
	h := &entryHeader{
		// binary.PutVarint use small-endian pattern
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5
	ksize, n := binary.Varint(buf[index:])
	h.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	return h, int64(index + n)
}

func getEntryCrc(e *LogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}

func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}

	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)

	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}
