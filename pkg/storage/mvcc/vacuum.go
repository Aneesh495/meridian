package mvcc

import (
	"bytes"

	"github.com/Aneesh495/meridian/pkg/hlc"
)

// Vacuum removes obsolete historical versions older than gcWatermark.
// Returns the number of reclaimed version records.
func (e *Engine) Vacuum(gcWatermark hlc.Timestamp) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	it := e.tree.NewIterator(nil)
	var keysToDelete [][]byte

	var currentUserKey []byte
	var keptVersionForUserKey bool

	for it.HasNext() {
		leaf, ok := it.Next()
		if !ok {
			break
		}

		userKey, ts, isTombstone, err := DecodeVersionKey(leaf.Key)
		if err != nil {
			continue
		}

		if !bytes.Equal(userKey, currentUserKey) {
			currentUserKey = userKey
			keptVersionForUserKey = false
		}

		// If version is older than gcWatermark:
		if ts.Less(gcWatermark) {
			if keptVersionForUserKey {
				// We already have a newer version visible at/before watermark, this older one is obsolete
				keysToDelete = append(keysToDelete, leaf.Key)
			} else {
				// Keep the newest version below watermark so reads right at watermark can still see it
				keptVersionForUserKey = true
				if isTombstone {
					// Obsolete tombstone can be pruned
					keysToDelete = append(keysToDelete, leaf.Key)
				}
			}
		} else {
			// Version is newer than watermark, must be kept
		}
	}

	for _, k := range keysToDelete {
		e.tree.Delete(k)
	}

	return len(keysToDelete), nil
}
