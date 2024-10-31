package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Magic bytes identifying Meridian protocol frames: 'M', 'E', 'R', 'I'.
const Magic uint32 = 0x4D455249

// CurrentProtocolVersion supported by this engine.
const CurrentProtocolVersion uint16 = 1

// HeaderSize is the fixed size in bytes of a Frame header (26 bytes).
// Format:
// [0:4]   Magic (4B)
// [4:6]   Version (2B)
// [6:8]   Flags (2B)
// [8:16]  StreamID (8B)
// [16:18] MsgType (2B)
// [18:22] PayloadLen (4B)
// [22:26] Checksum CRC32-C (4B)
const HeaderSize = 26

// MaxPayloadSize limits payload size to 64MB to prevent buffer exhaustion attacks.
const MaxPayloadSize = 64 * 1024 * 1024

// Frame flags.
const (
	FlagResponse   uint16 = 1 << 0
	FlagCompressed uint16 = 1 << 1
	FlagEndStream  uint16 = 1 << 2
	FlagHeartbeat  uint16 = 1 << 3
)

var (
	ErrMagicMismatch = errors.New("meridian/protocol: invalid frame magic bytes")
	ErrUnsupportedVersion = errors.New("meridian/protocol: unsupported protocol version")
	ErrPayloadTooLarge = errors.New("meridian/protocol: frame payload exceeds maximum size")
)

// Frame represents a framed packet transmitted over TCP.
type Frame struct {
	Version  uint16
	Flags    uint16
	StreamID uint64
	Type     MsgType
	Payload  []byte
}

// NewFrame constructs a new protocol frame.
func NewFrame(streamID uint64, msgType MsgType, payload []byte) *Frame {
	return &Frame{
		Version:  CurrentProtocolVersion,
		Flags:    0,
		StreamID: streamID,
		Type:     msgType,
		Payload:  payload,
	}
}

// Encode serializes the frame into a byte slice including 26-byte header and CRC32-C.
func (f *Frame) Encode() []byte {
	payloadLen := len(f.Payload)
	totalLen := HeaderSize + payloadLen
	buf := make([]byte, totalLen)

	binary.BigEndian.PutUint32(buf[0:4], Magic)
	binary.BigEndian.PutUint16(buf[4:6], f.Version)
	binary.BigEndian.PutUint16(buf[6:8], f.Flags)
	binary.BigEndian.PutUint64(buf[8:16], f.StreamID)
	binary.BigEndian.PutUint16(buf[16:18], uint16(f.Type))
	binary.BigEndian.PutUint32(buf[18:22], uint32(payloadLen))

	// Compute Castagnoli CRC32 over payload
	crc := core.ChecksumCRC32(f.Payload)
	binary.BigEndian.PutUint32(buf[22:26], crc)

	if payloadLen > 0 {
		copy(buf[HeaderSize:], f.Payload)
	}

	return buf
}

// WriteTo writes the encoded frame directly to an io.Writer satisfying io.WriterTo.
func (f *Frame) WriteTo(w io.Writer) (int64, error) {
	encoded := f.Encode()
	n, err := w.Write(encoded)
	return int64(n), err
}

// ReadFrame reads a complete frame from an io.Reader, verifying magic and CRC32.
func ReadFrame(r io.Reader) (*Frame, error) {
	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != Magic {
		return nil, fmt.Errorf("%w: got 0x%08X expected 0x%08X", ErrMagicMismatch, magic, Magic)
	}

	version := binary.BigEndian.Uint16(header[4:6])
	if version != CurrentProtocolVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	flags := binary.BigEndian.Uint16(header[6:8])
	streamID := binary.BigEndian.Uint64(header[8:16])
	msgType := MsgType(binary.BigEndian.Uint16(header[16:18]))
	payloadLen := binary.BigEndian.Uint32(header[18:22])
	expectedCRC := binary.BigEndian.Uint32(header[22:26])

	if payloadLen > MaxPayloadSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, payloadLen)
	}

	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}

		actualCRC := core.ChecksumCRC32(payload)
		if actualCRC != expectedCRC {
			return nil, fmt.Errorf("%w: expected 0x%08X, calculated 0x%08X", core.ErrCRC32Mismatch, expectedCRC, actualCRC)
		}
	}

	return &Frame{
		Version:  version,
		Flags:    flags,
		StreamID: streamID,
		Type:     msgType,
		Payload:  payload,
	}, nil
}
