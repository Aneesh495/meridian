package core

import (
	"math"
	"testing"
)

func TestByteBufferPrimitives(t *testing.T) {
	buf := NewByteBuffer(128)

	buf.WriteByte(0x42)
	buf.WriteUint16(0x1234)
	buf.WriteUint32(0x89ABCDEF)
	buf.WriteUint64(0xFEDCBA9876543210)
	buf.WriteFloat64(math.Pi)
	buf.WriteUvarint(1048576)
	buf.WriteVarint(-424242)
	buf.WriteString("meridian-fabric")
	buf.WriteBytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	r := WrapByteBuffer(buf.Bytes())

	b, err := r.ReadByte()
	if err != nil || b != 0x42 {
		t.Fatalf("expected 0x42, got %x (err: %v)", b, err)
	}

	u16, err := r.ReadUint16()
	if err != nil || u16 != 0x1234 {
		t.Fatalf("expected 0x1234, got %x (err: %v)", u16, err)
	}

	u32, err := r.ReadUint32()
	if err != nil || u32 != 0x89ABCDEF {
		t.Fatalf("expected 0x89ABCDEF, got %x (err: %v)", u32, err)
	}

	u64, err := r.ReadUint64()
	if err != nil || u64 != 0xFEDCBA9876543210 {
		t.Fatalf("expected 0xFEDCBA9876543210, got %x (err: %v)", u64, err)
	}

	f64, err := r.ReadFloat64()
	if err != nil || math.Abs(f64-math.Pi) > 1e-9 {
		t.Fatalf("expected Pi, got %v (err: %v)", f64, err)
	}

	uv, err := r.ReadUvarint()
	if err != nil || uv != 1048576 {
		t.Fatalf("expected 1048576, got %d (err: %v)", uv, err)
	}

	v, err := r.ReadVarint()
	if err != nil || v != -424242 {
		t.Fatalf("expected -424242, got %d (err: %v)", v, err)
	}

	str, err := r.ReadString()
	if err != nil || str != "meridian-fabric" {
		t.Fatalf("expected 'meridian-fabric', got '%s' (err: %v)", str, err)
	}

	bs, err := r.ReadBytes()
	if err != nil || len(bs) != 4 || bs[0] != 0xDE || bs[3] != 0xEF {
		t.Fatalf("bytes mismatch: %v (err: %v)", bs, err)
	}

	if r.Remaining() != 0 {
		t.Fatalf("expected 0 remaining bytes, got %d", r.Remaining())
	}
}

func TestByteBufferResetAndOffset(t *testing.T) {
	buf := NewByteBuffer(32)
	buf.WriteString("hello")
	if buf.Len() == 0 {
		t.Fatalf("expected non-empty buffer")
	}

	buf.Reset()
	if buf.Len() != 0 || buf.Remaining() != 0 {
		t.Fatalf("expected empty buffer after reset")
	}

	buf.WriteString("world")
	r := WrapByteBuffer(buf.Bytes())
	if err := r.SetOffset(2); err != nil {
		t.Fatalf("failed to set offset: %v", err)
	}
	if r.Offset() != 2 {
		t.Fatalf("expected offset 2, got %d", r.Offset())
	}
	if err := r.SetOffset(999); err == nil {
		t.Fatalf("expected error setting invalid offset")
	}
}
