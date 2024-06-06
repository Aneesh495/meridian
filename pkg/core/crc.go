package core

import (
	"hash/crc32"
)

// CastagnoliTable is the CRC32-C polynomial table (0x82F63B78).
var CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ChecksumCRC32 calculates the 32-bit Castagnoli CRC checksum of the input bytes.
func ChecksumCRC32(b []byte) uint32 {
	return crc32.Checksum(b, CastagnoliTable)
}

// UpdateCRC32 updates an existing CRC32 checksum with additional data bytes.
func UpdateCRC32(crc uint32, b []byte) uint32 {
	return crc32.Update(crc, CastagnoliTable, b)
}

// VerifyCRC32 compares the calculated CRC32 checksum of data against expected checksum.
func VerifyCRC32(data []byte, expected uint32) bool {
	return ChecksumCRC32(data) == expected
}

// MaskCRC32 masks a CRC32 checksum for storage in logs to prevent accidental match
// with actual record headers in corrupted regions.
func MaskCRC32(c uint32) uint32 {
	return ((c >> 15) | (c << 17)) + 0xa282ead8
}

// UnmaskCRC32 inverts the masking applied by MaskCRC32.
func UnmaskCRC32(masked uint32) uint32 {
	rot := masked - 0xa282ead8
	return (rot >> 17) | (rot << 15)
}
