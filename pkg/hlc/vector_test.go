package hlc

import (
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
)

func TestVectorClockCausality(t *testing.T) {
	v1 := NewVectorClock()
	v2 := NewVectorClock()

	v1.Set(core.NodeID(1), 1)
	v2.Set(core.NodeID(1), 1)

	if !v1.Equal(v2) {
		t.Fatalf("v1 and v2 should be equal")
	}

	v1.Increment(core.NodeID(1)) // v1 is {1: 2}
	if !v1.Dominates(v2) {
		t.Fatalf("v1 {1:2} should dominate v2 {1:1}")
	}
	if v2.Dominates(v1) {
		t.Fatalf("v2 should not dominate v1")
	}

	v2.Set(core.NodeID(2), 1) // v2 is {1: 1, 2: 1}
	if !v1.ConcurrentWith(v2) {
		t.Fatalf("v1 {1:2} and v2 {1:1, 2:1} should be concurrent")
	}

	// Merge v2 into v1 => {1: 2, 2: 1}
	v1.Merge(v2)
	if !v1.Dominates(v2) {
		t.Fatalf("merged v1 {1:2, 2:1} should dominate v2 {1:1, 2:1}")
	}
}

func TestVectorClockBinaryEncoding(t *testing.T) {
	vc := NewVectorClock()
	vc.Set(core.NodeID(10), 100)
	vc.Set(core.NodeID(20), 200)
	vc.Set(core.NodeID(5), 50)

	enc := vc.Encode()
	decoded, err := DecodeVectorClock(enc)
	if err != nil {
		t.Fatalf("failed to decode vector clock: %v", err)
	}

	if !vc.Equal(decoded) {
		t.Fatalf("vector clock mismatch after decode: orig=%s, decoded=%s", vc, decoded)
	}
}
