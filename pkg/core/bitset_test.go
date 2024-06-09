package core

import (
	"testing"
)

func TestBitSetOperations(t *testing.T) {
	bs := NewBitSet(64)
	if !bs.IsEmpty() {
		t.Fatalf("newly allocated bitset should be empty")
	}

	bs.Set(0)
	bs.Set(63)
	bs.Set(64)
	bs.Set(127)
	bs.Set(500)

	if bs.Count() != 5 {
		t.Fatalf("expected count 5, got %d", bs.Count())
	}

	if !bs.Test(0) || !bs.Test(63) || !bs.Test(64) || !bs.Test(127) || !bs.Test(500) {
		t.Fatalf("tested bits must be set")
	}
	if bs.Test(1) || bs.Test(62) || bs.Test(65) || bs.Test(499) {
		t.Fatalf("tested bits must be unset")
	}

	bs.Clear(63)
	if bs.Test(63) || bs.Count() != 4 {
		t.Fatalf("bit 63 should be cleared, count should be 4")
	}

	clone := bs.Clone()
	if clone.Count() != 4 || !clone.Test(500) {
		t.Fatalf("cloned bitset should match original")
	}

	bs.ClearAll()
	if !bs.IsEmpty() || bs.Count() != 0 {
		t.Fatalf("bitset should be empty after ClearAll")
	}
}

func TestBitSetUnionAndIntersection(t *testing.T) {
	b1 := NewBitSet(64)
	b2 := NewBitSet(64)

	b1.Set(10)
	b1.Set(20)
	b1.Set(30)

	b2.Set(20)
	b2.Set(30)
	b2.Set(40)

	union := b1.Union(b2)
	if union.Count() != 4 {
		t.Fatalf("union count expected 4, got %d", union.Count())
	}
	if !union.Test(10) || !union.Test(20) || !union.Test(30) || !union.Test(40) {
		t.Fatalf("union must contain 10, 20, 30, 40")
	}

	inter := b1.Intersection(b2)
	if inter.Count() != 2 {
		t.Fatalf("intersection count expected 2, got %d", inter.Count())
	}
	if !inter.Test(20) || !inter.Test(30) {
		t.Fatalf("intersection must contain 20, 30")
	}
}

func TestBitSetNextSetBit(t *testing.T) {
	b := NewBitSet(256)
	b.Set(5)
	b.Set(70)
	b.Set(200)

	if n := b.NextSetBit(0); n != 5 {
		t.Fatalf("expected next bit 5, got %d", n)
	}
	if n := b.NextSetBit(5); n != 5 {
		t.Fatalf("expected next bit 5, got %d", n)
	}
	if n := b.NextSetBit(6); n != 70 {
		t.Fatalf("expected next bit 70, got %d", n)
	}
	if n := b.NextSetBit(71); n != 200 {
		t.Fatalf("expected next bit 200, got %d", n)
	}
	if n := b.NextSetBit(201); n != -1 {
		t.Fatalf("expected -1 after 201, got %d", n)
	}
}
