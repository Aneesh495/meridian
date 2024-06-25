package wal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Reader sequentially scans records from a segment file.
type Reader struct {
	segment *Segment
	offset  int64
}

// NewReader creates a Reader for a given Segment.
func NewReader(segment *Segment) *Reader {
	return &Reader{
		segment: segment,
		offset:  int64(SegmentHeaderSize),
	}
}

// Next reads the next Record from the segment. Returns io.EOF at end of valid records.
func (r *Reader) Next() (*Record, error) {
	if r.offset >= int64(r.segment.size) {
		return nil, io.EOF
	}

	var header [RecordHeaderSize]byte
	n, err := r.segment.file.ReadAt(header[:], r.offset)
	if err != nil {
		if err == io.EOF && n < RecordHeaderSize {
			return nil, io.EOF
		}
		if n == 0 {
			return nil, io.EOF
		}
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	opType := core.OpType(header[4])
	term := core.Term(binary.BigEndian.Uint64(header[5:13]))
	index := core.Index(binary.BigEndian.Uint64(header[13:21]))
	expectedCRC := binary.BigEndian.Uint32(header[21:25])

	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		payloadOffset := r.offset + int64(RecordHeaderSize)
		if _, err := r.segment.file.ReadAt(payload, payloadOffset); err != nil {
			return nil, fmt.Errorf("corrupt record payload at offset %d: %w", r.offset, err)
		}

		actualCRC := core.ChecksumCRC32(payload)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("%w: at offset %d (term=%d, index=%d)", core.ErrCRC32Mismatch, r.offset, term, index)
		}
	}

	r.offset += int64(RecordHeaderSize + payloadLen)

	return &Record{
		Type:    opType,
		Term:    term,
		Index:   index,
		Payload: payload,
	}, nil
}

// Offset returns the current scan offset.
func (r *Reader) Offset() int64 {
	return r.offset
}
