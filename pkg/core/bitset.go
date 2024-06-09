package core

import (
	"math/bits"
)

// BitSet is a dynamic 64-bit word bit array optimized for quorum tracking and membership sets.
type BitSet struct {
	words []uint64
}

// NewBitSet creates a new BitSet with initial bit capacity.
func NewBitSet(initialBits int) *BitSet {
	numWords := (initialBits + 63) / 64
	if numWords < 1 {
		numWords = 1
	}
	return &BitSet{
		words: make([]uint64, numWords),
	}
}

func (b *BitSet) ensureCapacity(wordIdx int) {
	if wordIdx >= len(b.words) {
		newWords := make([]uint64, wordIdx+1)
		copy(newWords, b.words)
		b.words = newWords
	}
}

// Set marks bit i as 1.
func (b *BitSet) Set(i int) {
	if i < 0 {
		return
	}
	wordIdx := i / 64
	bitIdx := uint(i % 64)
	b.ensureCapacity(wordIdx)
	b.words[wordIdx] |= (1 << bitIdx)
}

// Clear marks bit i as 0.
func (b *BitSet) Clear(i int) {
	if i < 0 {
		return
	}
	wordIdx := i / 64
	bitIdx := uint(i % 64)
	if wordIdx < len(b.words) {
		b.words[wordIdx] &^= (1 << bitIdx)
	}
}

// Test checks if bit i is set.
func (b *BitSet) Test(i int) bool {
	if i < 0 {
		return false
	}
	wordIdx := i / 64
	bitIdx := uint(i % 64)
	if wordIdx >= len(b.words) {
		return false
	}
	return (b.words[wordIdx] & (1 << bitIdx)) != 0
}

// Count returns the total number of set bits (population count).
func (b *BitSet) Count() int {
	cnt := 0
	for _, w := range b.words {
		cnt += bits.OnesCount64(w)
	}
	return cnt
}

// IsEmpty returns true if no bits are set.
func (b *BitSet) IsEmpty() bool {
	for _, w := range b.words {
		if w != 0 {
			return false
		}
	}
	return true
}

// ClearAll unsets all bits in the bitset.
func (b *BitSet) ClearAll() {
	for i := range b.words {
		b.words[i] = 0
	}
}

// Clone creates a deep copy of the bitset.
func (b *BitSet) Clone() *BitSet {
	newWords := make([]uint64, len(b.words))
	copy(newWords, b.words)
	return &BitSet{words: newWords}
}

// Union computes the bitwise OR of this bitset with other.
func (b *BitSet) Union(other *BitSet) *BitSet {
	maxLen := len(b.words)
	if len(other.words) > maxLen {
		maxLen = len(other.words)
	}
	res := &BitSet{words: make([]uint64, maxLen)}
	for i := 0; i < maxLen; i++ {
		var w1, w2 uint64
		if i < len(b.words) {
			w1 = b.words[i]
		}
		if i < len(other.words) {
			w2 = other.words[i]
		}
		res.words[i] = w1 | w2
	}
	return res
}

// Intersection computes the bitwise AND of this bitset with other.
func (b *BitSet) Intersection(other *BitSet) *BitSet {
	minLen := len(b.words)
	if len(other.words) < minLen {
		minLen = len(other.words)
	}
	res := &BitSet{words: make([]uint64, minLen)}
	for i := 0; i < minLen; i++ {
		res.words[i] = b.words[i] & other.words[i]
	}
	return res
}

// NextSetBit returns the index of the first bit set on or after start.
// Returns -1 if no such bit exists.
func (b *BitSet) NextSetBit(start int) int {
	if start < 0 {
		start = 0
	}
	wordIdx := start / 64
	if wordIdx >= len(b.words) {
		return -1
	}

	bitIdx := uint(start % 64)
	w := b.words[wordIdx] >> bitIdx
	if w != 0 {
		return start + bits.TrailingZeros64(w)
	}

	wordIdx++
	for wordIdx < len(b.words) {
		w = b.words[wordIdx]
		if w != 0 {
			return wordIdx*64 + bits.TrailingZeros64(w)
		}
		wordIdx++
	}

	return -1
}
