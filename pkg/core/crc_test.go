package core

import (
	"testing"
)

func TestCRC32Checksum(t *testing.T) {
	data := []byte("meridian-distributed-consensus-state-fabric")
	crc1 := ChecksumCRC32(data)
	if crc1 == 0 {
		t.Fatalf("expected non-zero checksum")
	}

	if !VerifyCRC32(data, crc1) {
		t.Fatalf("expected verification to pass")
	}

	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[0] ^= 0xFF

	if VerifyCRC32(corrupted, crc1) {
		t.Fatalf("expected verification to fail for corrupted data")
	}
}

func TestCRC32Masking(t *testing.T) {
	crc := uint32(0x12345678)
	masked := MaskCRC32(crc)
	unmasked := UnmaskCRC32(masked)
	if crc != unmasked {
		t.Fatalf("expected %x, got %x", crc, unmasked)
	}
}

func TestCRC32Incremental(t *testing.T) {
	part1 := []byte("hello ")
	part2 := []byte("world")
	full := []byte("hello world")

	fullCRC := ChecksumCRC32(full)

	incCRC := ChecksumCRC32(part1)
	incCRC = UpdateCRC32(incCRC, part2)

	if fullCRC != incCRC {
		t.Fatalf("incremental CRC mismatch: full=%x, incremental=%x", fullCRC, incCRC)
	}
}
