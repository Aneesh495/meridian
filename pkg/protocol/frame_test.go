package protocol

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
)

func TestFrameEncodeDecode(t *testing.T) {
	payload := []byte("replicated-state-machine-payload-content")
	frame := NewFrame(42, MsgAppendEntries, payload)
	frame.Flags |= FlagResponse

	encoded := frame.Encode()
	if len(encoded) != HeaderSize+len(payload) {
		t.Fatalf("expected length %d, got %d", HeaderSize+len(payload), len(encoded))
	}

	decoded, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode frame: %v", err)
	}

	if decoded.StreamID != 42 {
		t.Fatalf("expected stream ID 42, got %d", decoded.StreamID)
	}
	if decoded.Type != MsgAppendEntries {
		t.Fatalf("expected MsgAppendEntries, got %v", decoded.Type)
	}
	if decoded.Flags&FlagResponse == 0 {
		t.Fatalf("expected FlagResponse to be set")
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFrameCRCVerificationFailure(t *testing.T) {
	payload := []byte("critical-consensus-entry")
	frame := NewFrame(100, MsgTxnCommit, payload)
	encoded := frame.Encode()

	// Corrupt one byte of payload
	encoded[HeaderSize+2] ^= 0xFF

	_, err := ReadFrame(bytes.NewReader(encoded))
	if err == nil || !errors.Is(err, core.ErrCRC32Mismatch) {
		t.Fatalf("expected ErrCRC32Mismatch, got %v", err)
	}
}

func TestFrameInvalidMagic(t *testing.T) {
	payload := []byte("data")
	frame := NewFrame(1, MsgPing, payload)
	encoded := frame.Encode()

	// Corrupt magic bytes
	encoded[0] = 'B'

	_, err := ReadFrame(bytes.NewReader(encoded))
	if err == nil || !errors.Is(err, ErrMagicMismatch) {
		t.Fatalf("expected ErrMagicMismatch, got %v", err)
	}
}
