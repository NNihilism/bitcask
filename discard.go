package bitcask

import (
	"bitcask/ioselector"
	"bitcask/logfile"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"path/filepath"
	"sort"
	"sync"
)

const (
	discardRecordSize = 12
	// 8kb, contains mostly 682 records in file.
	discardFileSize int64 = 2 << 12
	discardFileName       = "discard"
)

// ErrDiscardNoSpace no enough space for discard file.
var ErrDiscardNoSpace = errors.New("not enough space can be allocated for the discard file")

type discard struct {
	sync.Mutex
	once     *sync.Once
	file     ioselector.IOSelector
	valChan  chan *indexNode
	freeList []int64          // contains file offset that can be allocated
	location map[uint32]int64 // offset of each fid
}

func newDiscard(path, name string, bufferSize int) (*discard, error) {
	fName := filepath.Join(path, name)
	file, err := ioselector.NewMMapSelector(fName, discardFileSize)
	if err != nil {
		return nil, err
	}

	var freeList []int64
	localtion := make(map[uint32]int64)
	var offset int64

	for {
		buf := make([]byte, discardRecordSize)
		if _, err := file.Read(buf, offset); err != nil {
			if err == logfile.ErrEndOfEntry || err == io.EOF {
				break
			}
			return nil, err
		}
		fid := binary.LittleEndian.Uint32(buf[:4])
		total := binary.LittleEndian.Uint32(buf[4:])
		if fid == 0 && total == 0 {
			freeList = append(freeList, offset)
		} else {
			localtion[fid] = offset
		}
		offset += discardRecordSize
	}

	d := &discard{
		file:     file,
		valChan:  make(chan *indexNode, bufferSize),
		freeList: freeList,
		location: localtion,
		once:     new(sync.Once),
	}

	go d.listenUpdates()

	return d, nil
}

func (d *discard) listenUpdates() {
	for idxNode := range d.valChan {
		d.incrDiscard(idxNode.fid, idxNode.entrySize)
	}
	// Close the channel, and the loop will end when the buffer is empty
	if err := d.file.Close(); err != nil {
		log.Printf("close discard file err : %v", err)
	}
}

func (d *discard) incrDiscard(fid uint32, delta int) {
	if delta > 0 {
		d.incr(fid, delta)
	}
}

// format of discard file` record:
// +-------+--------------+----------------+  +-------+--------------+----------------+
// |  fid  |  total size  | discarded size |  |  fid  |  total size  | discarded size |
// +-------+--------------+----------------+  +-------+--------------+----------------+
// 0-------4--------------8---------------12  12------16------------20----------------24
func (d *discard) incr(fid uint32, delta int) {
	d.Lock()
	defer d.Unlock()

	offset, err := d.alloc(fid)
	if err != nil {
		log.Printf("discard file allocate err : %+v", err)
		return
	}

	var buf []byte
	if delta > 0 {
		offset += 8
		buf = make([]byte, 4)
		_, err = d.file.Read(buf, offset)
		if err != nil {
			log.Printf("read in incr() value in discard.go err :%v", err)
			return
		}
		v := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, v+uint32(delta))
	} else {
		buf = make([]byte, discardRecordSize)
	}

	if _, err = d.file.Write(buf, offset); err != nil {
		log.Printf("write in incr() in discard.go err :%v", err)
		return
	}
}

func (d *discard) alloc(fid uint32) (int64, error) {
	if offset, ok := d.location[fid]; ok {
		return offset, nil
	}
	if len(d.freeList) == 0 {
		return 0, ErrDiscardNoSpace
	}
	// Why allocate from the tail...
	offset := d.freeList[len(d.freeList)-1]
	d.freeList = d.freeList[:len(d.freeList)-1]
	d.location[fid] = offset
	return offset, nil
}

func (d *discard) setTotal(fid uint32, totalSize uint32) {
	d.Lock()
	defer d.Unlock()

	if _, ok := d.location[fid]; ok {
		return
	}

	offset, err := d.alloc(fid)
	if err != nil {
		log.Printf("discard file allocate err: %+v", err)
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fid)
	binary.LittleEndian.PutUint32(buf[4:], totalSize)
	_, err = d.file.Write(buf, offset)
	if err != nil {
		log.Printf("write discard file err: %+v", err)
		return
	}
}

// CCL means compaction cnadidate list.
// iterate and find the file with most discarded data,
// there are 682 records at most, no need to worry about the performance.
func (d *discard) getCCL(activeFid uint32, ratio float64) ([]uint32, error) {
	d.Lock()
	defer d.Unlock()

	var ccl []uint32
	for fid, offset := range d.location {
		if fid == activeFid {
			continue
		}
		buf := make([]byte, discardRecordSize)
		_, err := d.file.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == logfile.ErrEndOfEntry {
				break
			}
			return nil, err
		}

		total := binary.LittleEndian.Uint32(buf[4:8])
		discard := binary.LittleEndian.Uint32(buf[8:12])

		var curRatio float64
		if total != 0 && discard != 0 {
			curRatio = float64(discard) / float64(total)
		}
		if curRatio > ratio {
			ccl = append(ccl, fid)
		}
	}

	// sort in ascending order, guarantee the older file will compact firstly.
	sort.Slice(ccl, func(i, j int) bool {
		return ccl[i] < ccl[j]
	})
	return ccl, nil
}

func (d *discard) clear(fid uint32) {
	d.incr(fid, -1)

	d.Lock()
	defer d.Unlock()

	if offset, ok := d.location[fid]; ok {
		d.freeList = append(d.freeList, offset)
		delete(d.location, fid)
	}
}

func (d *discard) sync() error {
	return d.file.Sync()
}
