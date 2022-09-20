package logfile

// MaxHeaderSize max entry header size.
// crc32	typ    kSize	vSize	expiredAt
//  4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)
const MaxHeaderSize = 25

// EntryType type of Entry.
type EntryType byte

const (
	// TypeAdd represents entry type is add or update.
	TypeAdd EntryType = iota

	// TypeDelete represents entry type is delete.
	TypeDelete

	// TypeListMeta represents entry is list meta.
	TypeListMeta
)

// LogEntry is the data will be appended in log file.
type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64 // time.Unix
	Type      EntryType
}

type entryHeader struct {
	crc32     uint32 // check sum
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}
