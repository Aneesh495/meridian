package mvcc

import (
	"bytes"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage/art"
)

// Entry represents a visible key-value snapshot record.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp hlc.Timestamp
}

// SnapshotIterator scans user keys with a given prefix, returning the latest version visible at readTS.
type SnapshotIterator struct {
	engine     *Engine
	readTS     hlc.Timestamp
	prefix     []byte
	artIter    *art.Iterator
	lastSeen   []byte
	current    *Entry
	isExhausted bool
}

// NewSnapshotIterator creates an iterator over keys with prefix as visible at readTS.
func (e *Engine) NewSnapshotIterator(prefix []byte, readTS hlc.Timestamp) *SnapshotIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return &SnapshotIterator{
		engine:  e,
		readTS:  readTS,
		prefix:  prefix,
		artIter: e.tree.NewIterator(prefix),
	}
}

// Next advances the iterator to the next distinct visible user key.
func (it *SnapshotIterator) Next() bool {
	if it.isExhausted {
		return false
	}

	for it.artIter.HasNext() {
		leaf, ok := it.artIter.Next()
		if !ok {
			break
		}

		// Skip locks
		if len(leaf.Key) > 0 && leaf.Key[len(leaf.Key)-1] == byte(TagLock) {
			continue
		}

		userKey, ts, isTombstone, err := DecodeVersionKey(leaf.Key)
		if err != nil {
			continue
		}

		if !bytes.HasPrefix(userKey, it.prefix) {
			continue
		}

		// If this is a different version of the user key we already yielded, skip
		if it.lastSeen != nil && bytes.Equal(userKey, it.lastSeen) {
			continue
		}

		// Check if visible at readTS
		if !it.readTS.Less(ts) {
			it.lastSeen = userKey

			if isTombstone {
				// Key was deleted before readTS, proceed to next user key
				continue
			}

			it.current = &Entry{
				Key:       userKey,
				Value:     leaf.Value,
				Timestamp: ts,
			}
			return true
		}
	}

	it.isExhausted = true
	it.current = nil
	return false
}

// Entry returns the current entry pointed to by the iterator.
func (it *SnapshotIterator) Entry() *Entry {
	return it.current
}
