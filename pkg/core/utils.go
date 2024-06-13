package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// HashBytes computes the 64-bit FNV-1a hash of a byte slice.
func HashBytes(data []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var hash uint64 = offset64
	for _, b := range data {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

// HashString computes the 64-bit FNV-1a hash of a string.
func HashString(s string) uint64 {
	return HashBytes([]byte(s))
}

// Murmur3_64 computes a 64-bit MurmurHash3 variant for fast uniform key partitioning.
func Murmur3_64(data []byte, seed uint64) uint64 {
	const (
		m = 0xc6a4a7935bd1e995
		r = 47
	)
	h := seed ^ (uint64(len(data)) * m)

	nblocks := len(data) / 8
	for i := 0; i < nblocks; i++ {
		start := i * 8
		k := uint64(data[start]) |
			uint64(data[start+1])<<8 |
			uint64(data[start+2])<<16 |
			uint64(data[start+3])<<24 |
			uint64(data[start+4])<<32 |
			uint64(data[start+5])<<40 |
			uint64(data[start+6])<<48 |
			uint64(data[start+7])<<56

		k *= m
		k ^= k >> r
		k *= m

		h ^= k
		h *= m
	}

	tail := data[nblocks*8:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// GenerateID generates a cryptographically secure hex ID with n random bytes (2*n hex chars).
func GenerateID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fallback deterministic formatting if crypto/rand fails
		return fmt.Sprintf("%x", HashBytes(b))
	}
	return hex.EncodeToString(b)
}

// Min returns the smaller of two uint64 values.
func Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// Max returns the larger of two uint64 values.
func Max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Clamp restricts v to the range [low, high].
func Clamp(v, low, high uint64) uint64 {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}
