package core

import (
	"encoding/binary"
	"io"
	"math"
)

// ByteBuffer is a growable byte buffer with binary serialization primitives.
type ByteBuffer struct {
	buf []byte
	off int
}

// NewByteBuffer creates a new ByteBuffer with initial capacity.
func NewByteBuffer(capacity int) *ByteBuffer {
	return &ByteBuffer{
		buf: make([]byte, 0, capacity),
		off: 0,
	}
}

// WrapByteBuffer creates a ByteBuffer wrapping an existing byte slice for reading.
func WrapByteBuffer(b []byte) *ByteBuffer {
	return &ByteBuffer{
		buf: b,
		off: 0,
	}
}

// Bytes returns the written contents of the buffer.
func (b *ByteBuffer) Bytes() []byte {
	return b.buf
}

// Len returns the current length of the buffer.
func (b *ByteBuffer) Len() int {
	return len(b.buf)
}

// Remaining returns the number of unread bytes.
func (b *ByteBuffer) Remaining() int {
	return len(b.buf) - b.off
}

// Offset returns the current read offset.
func (b *ByteBuffer) Offset() int {
	return b.off
}

// Reset clears the buffer for reuse while keeping underlying memory.
func (b *ByteBuffer) Reset() {
	b.buf = b.buf[:0]
	b.off = 0
}

// SetOffset sets the read offset.
func (b *ByteBuffer) SetOffset(off int) error {
	if off < 0 || off > len(b.buf) {
		return ErrBufferOverflow
	}
	b.off = off
	return nil
}

// WriteByte appends a single byte.
func (b *ByteBuffer) WriteByte(val byte) {
	b.buf = append(b.buf, val)
}

// ReadByte reads a single byte.
func (b *ByteBuffer) ReadByte() (byte, error) {
	if b.off >= len(b.buf) {
		return 0, io.EOF
	}
	val := b.buf[b.off]
	b.off++
	return val, nil
}

// WriteUint16 appends a big-endian uint16.
func (b *ByteBuffer) WriteUint16(val uint16) {
	b.buf = binary.BigEndian.AppendUint16(b.buf, val)
}

// ReadUint16 reads a big-endian uint16.
func (b *ByteBuffer) ReadUint16() (uint16, error) {
	if b.Remaining() < 2 {
		return 0, io.EOF
	}
	val := binary.BigEndian.Uint16(b.buf[b.off:])
	b.off += 2
	return val, nil
}

// WriteUint32 appends a big-endian uint32.
func (b *ByteBuffer) WriteUint32(val uint32) {
	b.buf = binary.BigEndian.AppendUint32(b.buf, val)
}

// ReadUint32 reads a big-endian uint32.
func (b *ByteBuffer) ReadUint32() (uint32, error) {
	if b.Remaining() < 4 {
		return 0, io.EOF
	}
	val := binary.BigEndian.Uint32(b.buf[b.off:])
	b.off += 4
	return val, nil
}

// WriteUint64 appends a big-endian uint64.
func (b *ByteBuffer) WriteUint64(val uint64) {
	b.buf = binary.BigEndian.AppendUint64(b.buf, val)
}

// ReadUint64 reads a big-endian uint64.
func (b *ByteBuffer) ReadUint64() (uint64, error) {
	if b.Remaining() < 8 {
		return 0, io.EOF
	}
	val := binary.BigEndian.Uint64(b.buf[b.off:])
	b.off += 8
	return val, nil
}

// WriteFloat64 appends an IEEE-754 binary64 float.
func (b *ByteBuffer) WriteFloat64(val float64) {
	b.WriteUint64(math.Float64bits(val))
}

// ReadFloat64 reads an IEEE-754 binary64 float.
func (b *ByteBuffer) ReadFloat64() (float64, error) {
	bits, err := b.ReadUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(bits), nil
}

// WriteUvarint writes an unsigned variable-length integer.
func (b *ByteBuffer) WriteUvarint(val uint64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], val)
	b.buf = append(b.buf, scratch[:n]...)
}

// ReadUvarint reads an unsigned variable-length integer.
func (b *ByteBuffer) ReadUvarint() (uint64, error) {
	val, n := binary.Uvarint(b.buf[b.off:])
	if n <= 0 {
		return 0, io.EOF
	}
	b.off += n
	return val, nil
}

// WriteVarint writes a signed variable-length integer using zig-zag encoding.
func (b *ByteBuffer) WriteVarint(val int64) {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutVarint(scratch[:], val)
	b.buf = append(b.buf, scratch[:n]...)
}

// ReadVarint reads a signed variable-length integer.
func (b *ByteBuffer) ReadVarint() (int64, error) {
	val, n := binary.Varint(b.buf[b.off:])
	if n <= 0 {
		return 0, io.EOF
	}
	b.off += n
	return val, nil
}

// WriteBytes writes a length-prefixed slice of bytes.
func (b *ByteBuffer) WriteBytes(data []byte) {
	b.WriteUvarint(uint64(len(data)))
	b.buf = append(b.buf, data...)
}

// ReadBytes reads a length-prefixed slice of bytes.
func (b *ByteBuffer) ReadBytes() ([]byte, error) {
	length, err := b.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if uint64(b.Remaining()) < length {
		return nil, io.EOF
	}
	start := b.off
	b.off += int(length)
	res := make([]byte, length)
	copy(res, b.buf[start:b.off])
	return res, nil
}

// WriteRaw appends raw bytes without a length prefix.
func (b *ByteBuffer) WriteRaw(data []byte) {
	b.buf = append(b.buf, data...)
}

// ReadRaw reads n raw bytes.
func (b *ByteBuffer) ReadRaw(n int) ([]byte, error) {
	if b.Remaining() < n {
		return nil, io.EOF
	}
	start := b.off
	b.off += n
	res := make([]byte, n)
	copy(res, b.buf[start:b.off])
	return res, nil
}

// WriteString writes a length-prefixed string.
func (b *ByteBuffer) WriteString(s string) {
	b.WriteBytes([]byte(s))
}

// ReadString reads a length-prefixed string.
func (b *ByteBuffer) ReadString() (string, error) {
	bs, err := b.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
