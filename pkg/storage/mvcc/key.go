package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

// KeyTag denotes whether an entry is a committed version or an active lock intent.
type KeyTag byte

const (
	TagVersion   KeyTag = 0x00
	TagLock      KeyTag = 0x01
	TagTombstone KeyTag = 0x02
)

var (
	ErrInvalidMVCCKey = errors.New("meridian/mvcc: invalid encoded MVCC key")
)

// EncodeVersionKey encodes a user key with a timestamp into a versioned storage key.
// The timestamp is encoded with inverted WallTime and Logical counter so that newer
// versions appear lexicographically before older versions.
func EncodeVersionKey(userKey []byte, ts hlc.Timestamp, isTombstone bool) []byte {
	buf := make([]byte, len(userKey)+1+18+1)
	copy(buf, userKey)
	buf[len(userKey)] = byte(TagVersion)

	tag := TagVersion
	if isTombstone {
		tag = TagTombstone
	}

	// Invert timestamp for descending sort
	invWall := ^uint64(ts.WallTime)
	invLogical := ^ts.Logical

	offset := len(userKey) + 1
	binary.BigEndian.PutUint64(buf[offset:offset+8], invWall)
	binary.BigEndian.PutUint16(buf[offset+8:offset+10], invLogical)
	binary.BigEndian.PutUint64(buf[offset+10:offset+18], uint64(ts.NodeID))
	buf[offset+18] = byte(tag)

	return buf
}

// EncodeLockKey encodes a lock intent key for a user key.
func EncodeLockKey(userKey []byte) []byte {
	buf := make([]byte, len(userKey)+1)
	copy(buf, userKey)
	buf[len(userKey)] = byte(TagLock)
	return buf
}

// DecodeVersionKey extracts the user key, timestamp, and tombstone flag.
func DecodeVersionKey(encoded []byte) ([]byte, hlc.Timestamp, bool, error) {
	if len(encoded) < 1+18+1 {
		return nil, hlc.ZeroTimestamp, false, ErrInvalidMVCCKey
	}

	offset := len(encoded) - 19
	if encoded[offset-1] != byte(TagVersion) {
		return nil, hlc.ZeroTimestamp, false, ErrInvalidMVCCKey
	}

	userKey := encoded[:offset-1]
	invWall := binary.BigEndian.Uint64(encoded[offset : offset+8])
	invLogical := binary.BigEndian.Uint16(encoded[offset+8 : offset+10])
	nodeID := binary.BigEndian.Uint64(encoded[offset+10 : offset+18])
	tag := KeyTag(encoded[offset+18])

	ts := hlc.Timestamp{
		WallTime: int64(^invWall),
		Logical:  ^invLogical,
		NodeID:   core.NodeID(nodeID),
	}

	isTombstone := (tag == TagTombstone)
	return userKey, ts, isTombstone, nil
}

// ExtractUserKey returns the user-visible key component from an encoded key.
func ExtractUserKey(encoded []byte) []byte {
	idx := bytes.LastIndexByte(encoded, byte(TagVersion))
	if idx == -1 {
		idx = bytes.LastIndexByte(encoded, byte(TagLock))
	}
	if idx != -1 {
		return encoded[:idx]
	}
	return encoded
}
