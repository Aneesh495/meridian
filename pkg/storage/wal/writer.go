package wal

import (
	"encoding/binary"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

// RecordHeaderSize is the size in bytes of an individual WAL record header (25 bytes).
// [0:4]   PayloadLength (uint32)
// [4:5]   OpType (uint8)
// [5:13]  Term (uint64)
// [13:21] Index (uint64)
// [21:25] Checksum CRC32-C (uint32)
const RecordHeaderSize = 25

// SyncMode determines disk durability frequency.
type SyncMode uint8

const (
	SyncEveryWrite SyncMode = iota
	SyncPeriodic
	SyncNone
)

// Record represents a single persistent WAL entry.
type Record struct {
	Type    core.OpType
	Term    core.Term
	Index   core.Index
	Payload []byte
}

// Writer appends records to the current active segment.
type Writer struct {
	mu          sync.Mutex
	dir         string
	currSegment *Segment
	maxSegSize  uint64
	syncMode    SyncMode
	lastIndex   core.Index
	lastTerm    core.Term
}

// NewWriter creates a new WAL Writer in the specified directory.
func NewWriter(dir string, maxSegSize uint64, mode SyncMode) (*Writer, error) {
	if maxSegSize == 0 {
		maxSegSize = DefaultSegmentSize
	}

	seg, err := CreateSegment(dir, 1)
	if err != nil {
		return nil, err
	}

	return &Writer{
		dir:         dir,
		currSegment: seg,
		maxSegSize:  maxSegSize,
		syncMode:    mode,
	}, nil
}

// Append writes a single record to the WAL, rolling over segments if necessary.
func (w *Writer) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	payloadLen := uint32(len(rec.Payload))
	recordTotalSize := uint64(RecordHeaderSize + payloadLen)

	// Check if segment rollover is required
	if w.currSegment.size+recordTotalSize > w.maxSegSize {
		if err := w.rollSegmentLocked(); err != nil {
			return err
		}
	}

	// Prepare record header
	var header [RecordHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], payloadLen)
	header[4] = byte(rec.Type)
	binary.BigEndian.PutUint64(header[5:13], uint64(rec.Term))
	binary.BigEndian.PutUint64(header[13:21], uint64(rec.Index))

	// Compute CRC32-C over payload
	crc := core.ChecksumCRC32(rec.Payload)
	binary.BigEndian.PutUint32(header[21:25], crc)

	// Write header and payload
	if _, err := w.currSegment.file.Write(header[:]); err != nil {
		return err
	}
	if payloadLen > 0 {
		if _, err := w.currSegment.file.Write(rec.Payload); err != nil {
			return err
		}
	}

	w.currSegment.size += recordTotalSize
	w.lastIndex = rec.Index
	w.lastTerm = rec.Term

	if w.syncMode == SyncEveryWrite {
		return w.currSegment.Sync()
	}

	return nil
}

// AppendBatch writes a slice of records atomically.
func (w *Writer) AppendBatch(records []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range records {
		payloadLen := uint32(len(rec.Payload))
		recordTotalSize := uint64(RecordHeaderSize + payloadLen)

		if w.currSegment.size+recordTotalSize > w.maxSegSize {
			if err := w.rollSegmentLocked(); err != nil {
				return err
			}
		}

		var header [RecordHeaderSize]byte
		binary.BigEndian.PutUint32(header[0:4], payloadLen)
		header[4] = byte(rec.Type)
		binary.BigEndian.PutUint64(header[5:13], uint64(rec.Term))
		binary.BigEndian.PutUint64(header[13:21], uint64(rec.Index))

		crc := core.ChecksumCRC32(rec.Payload)
		binary.BigEndian.PutUint32(header[21:25], crc)

		if _, err := w.currSegment.file.Write(header[:]); err != nil {
			return err
		}
		if payloadLen > 0 {
			if _, err := w.currSegment.file.Write(rec.Payload); err != nil {
				return err
			}
		}

		w.currSegment.size += recordTotalSize
		w.lastIndex = rec.Index
		w.lastTerm = rec.Term
	}

	if w.syncMode == SyncEveryWrite {
		return w.currSegment.Sync()
	}

	return nil
}

func (w *Writer) rollSegmentLocked() error {
	if err := w.currSegment.Sync(); err != nil {
		return err
	}
	if err := w.currSegment.Close(); err != nil {
		return err
	}

	nextID := w.currSegment.id + 1
	newSeg, err := CreateSegment(w.dir, nextID)
	if err != nil {
		return err
	}

	w.currSegment = newSeg
	return nil
}

// Sync flushes data to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currSegment.Sync()
}

// Close gracefully closes the active segment.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currSegment.Close()
}
