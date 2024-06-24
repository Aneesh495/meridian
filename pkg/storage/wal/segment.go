package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Aneesh495/meridian/pkg/core"
)

// SegmentMagic identifies valid Meridian WAL segment files: 'M', 'W', 'A', 'L'.
const SegmentMagic uint32 = 0x4D57414C

// SegmentHeaderSize is the size of the initial segment header (16 bytes).
const SegmentHeaderSize = 16

// DefaultSegmentSize is 64MB per segment file.
const DefaultSegmentSize uint64 = 64 * 1024 * 1024

// Segment represents a single sequential chunk of the Write-Ahead Log.
type Segment struct {
	id       uint64
	path     string
	file     *os.File
	size     uint64
	minTerm  core.Term
	maxTerm  core.Term
	minIndex core.Index
	maxIndex core.Index
	isClosed bool
}

// CreateSegment initializes a new WAL segment file on disk.
func CreateSegment(dir string, id uint64) (*Segment, error) {
	filename := fmt.Sprintf("%016d.wal", id)
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}

	// Write 16-byte segment header
	var header [SegmentHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], SegmentMagic)
	binary.BigEndian.PutUint64(header[4:12], id)
	binary.BigEndian.PutUint32(header[12:16], 0) // reserved

	if _, err := file.Write(header[:]); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Segment{
		id:       id,
		path:     path,
		file:     file,
		size:     SegmentHeaderSize,
		minIndex: 0,
		maxIndex: 0,
	}, nil
}

// OpenSegment opens an existing WAL segment file for reading or writing.
func OpenSegment(dir string, id uint64, readOnly bool) (*Segment, error) {
	filename := fmt.Sprintf("%016d.wal", id)
	path := filepath.Join(dir, filename)

	flags := os.O_RDWR
	if readOnly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, err
	}

	var header [SegmentHeaderSize]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		_ = file.Close()
		return nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != SegmentMagic {
		_ = file.Close()
		return nil, fmt.Errorf("invalid WAL segment magic: 0x%08X", magic)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &Segment{
		id:       id,
		path:     path,
		file:     file,
		size:     uint64(stat.Size()),
		isClosed: false,
	}, nil
}

// Sync flushes kernel buffers to stable disk storage.
func (s *Segment) Sync() error {
	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}

// Close closes the file handle.
func (s *Segment) Close() error {
	if s.isClosed {
		return nil
	}
	s.isClosed = true
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Size returns the current byte length of the segment.
func (s *Segment) Size() uint64 {
	return s.size
}

// ID returns the segment ID.
func (s *Segment) ID() uint64 {
	return s.id
}
