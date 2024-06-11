package core

import (
	"testing"
)

func TestBufferPool(t *testing.T) {
	pool := NewBufferPool()

	b1 := pool.Get(32)
	if cap(b1) < 64 {
		t.Fatalf("expected at least 64 cap, got %d", cap(b1))
	}
	pool.Put(b1)

	b2 := pool.Get(2000)
	if cap(b2) < 4096 {
		t.Fatalf("expected at least 4096 cap, got %d", cap(b2))
	}
	pool.Put(b2)

	// Oversized buffer fallback
	b3 := pool.Get(2 * 1024 * 1024)
	if len(b3) != 2*1024*1024 {
		t.Fatalf("expected 2MB len, got %d", len(b3))
	}
	pool.Put(b3)
}

func TestByteBufferPool(t *testing.T) {
	p := NewByteBufferPool(1024)
	buf := p.Get()
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer")
	}

	buf.WriteString("meridian")
	if buf.Len() == 0 {
		t.Fatalf("expected non-empty buffer")
	}

	p.Put(buf)

	buf2 := p.Get()
	if buf2.Len() != 0 {
		t.Fatalf("expected recycled buffer to be reset")
	}
}
