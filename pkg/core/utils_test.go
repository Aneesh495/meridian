package core

import (
	"testing"
)

func TestHashBytesAndString(t *testing.T) {
	s := "meridian-partition-key-42"
	h1 := HashString(s)
	h2 := HashBytes([]byte(s))
	if h1 != h2 {
		t.Fatalf("expected HashString == HashBytes, got %d vs %d", h1, h2)
	}
	if h1 == 0 {
		t.Fatalf("expected non-zero hash")
	}

	diff := HashString("meridian-partition-key-43")
	if h1 == diff {
		t.Fatalf("expected different hash values for distinct inputs")
	}
}

func TestMurmur3_64(t *testing.T) {
	data1 := []byte("transaction-intent-lock")
	data2 := []byte("transaction-intent-lock-secondary")

	m1 := Murmur3_64(data1, 0x1234)
	m2 := Murmur3_64(data2, 0x1234)
	if m1 == m2 {
		t.Fatalf("hash collision between distinct inputs")
	}

	// Stability test
	if Murmur3_64(data1, 0x1234) != m1 {
		t.Fatalf("murmur hash must be deterministic")
	}
}

func TestGenerateID(t *testing.T) {
	id1 := GenerateID(8)
	id2 := GenerateID(8)
	if len(id1) != 16 || len(id2) != 16 {
		t.Fatalf("expected 16 hex chars, got %d and %d", len(id1), len(id2))
	}
	if id1 == id2 {
		t.Fatalf("generated identical IDs")
	}
}

func TestMinMaxClamp(t *testing.T) {
	if Min(10, 20) != 10 || Min(20, 10) != 10 {
		t.Fatalf("Min calculation failed")
	}
	if Max(10, 20) != 20 || Max(20, 10) != 20 {
		t.Fatalf("Max calculation failed")
	}
	if Clamp(5, 10, 20) != 10 || Clamp(25, 10, 20) != 20 || Clamp(15, 10, 20) != 15 {
		t.Fatalf("Clamp calculation failed")
	}
}
