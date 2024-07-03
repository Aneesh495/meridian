package mvcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage/art"
)

var (
	ErrWriteConflict = errors.New("meridian/mvcc: write-write conflict with newer version")
)

// Lock represents an in-flight transaction lock intent.
type Lock struct {
	Primary     []byte        `json:"primary"`
	TxnID       core.TxnID    `json:"txn_id"`
	StartTS     hlc.Timestamp `json:"start_ts"`
	MinCommitTS hlc.Timestamp `json:"min_commit_ts"`
	TTL         uint64        `json:"ttl"`
	Op          core.OpType   `json:"op"`
	Value       []byte        `json:"value"`
}

// Engine coordinates multi-version read/write transactions backed by an Adaptive Radix Tree.
type Engine struct {
	mu   sync.RWMutex
	tree *art.Tree
}

// NewEngine creates a new MVCC storage engine.
func NewEngine() *Engine {
	return &Engine{
		tree: art.NewTree(),
	}
}

// Put writes a committed key-value version at commitTS.
func (e *Engine) Put(userKey []byte, value []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	vKey := EncodeVersionKey(userKey, commitTS, false)
	e.tree.Insert(vKey, value)
	return nil
}

// Delete records a committed deletion tombstone at commitTS.
func (e *Engine) Delete(userKey []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	vKey := EncodeVersionKey(userKey, commitTS, true)
	e.tree.Insert(vKey, nil)
	return nil
}

// Get reads the latest visible value of userKey at readTS.
func (e *Engine) Get(userKey []byte, readTS hlc.Timestamp) ([]byte, hlc.Timestamp, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if key is currently locked by a pending transaction
	lockKey := EncodeLockKey(userKey)
	if lockBytes, found := e.tree.Search(lockKey); found {
		var lock Lock
		if err := json.Unmarshal(lockBytes, &lock); == nil {
			// If lock was placed before or at readTS, read is blocked
			if !readTS.Less(lock.StartTS) {
				return nil, hlc.ZeroTimestamp, core.ErrKeyLocked
			}
		}
	}

	// Iterate over versions for this key
	prefix := make([]byte, len(userKey)+1)
	copy(prefix, userKey)
	prefix[len(userKey)] = byte(TagVersion)

	it := e.tree.NewIterator(prefix)
	for it.HasNext() {
		leaf, ok := it.Next()
		if !ok {
			break
		}

		uKey, ts, isTombstone, err := DecodeVersionKey(leaf.Key)
		if err != nil || !bytes.Equal(uKey, userKey) {
			continue
		}

		// Visible if version timestamp <= readTS
		if !readTS.Less(ts) {
			if isTombstone {
				return nil, ts, core.ErrNotFound
			}
			return leaf.Value, ts, nil
		}
	}

	return nil, hlc.ZeroTimestamp, core.ErrNotFound
}

// PutLock registers a write intent lock for userKey.
func (e *Engine) PutLock(userKey []byte, lock *Lock) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	lockKey := EncodeLockKey(userKey)
	// Check if already locked
	if existingBytes, found := e.tree.Search(lockKey); found {
		var existing Lock
		if err := json.Unmarshal(existingBytes, &existing); err == nil {
			if existing.TxnID != lock.TxnID {
				return core.ErrKeyLocked
			}
		}
	}

	// Verify no newer committed version exists beyond lock.StartTS
	prefix := make([]byte, len(userKey)+1)
	copy(prefix, userKey)
	prefix[len(userKey)] = byte(TagVersion)

	it := e.tree.NewIterator(prefix)
	if it.HasNext() {
		leaf, ok := it.Next()
		if ok {
			_, latestTS, _, err := DecodeVersionKey(leaf.Key)
			if err == nil && lock.StartTS.Less(latestTS) {
				return ErrWriteConflict
			}
		}
	}

	data, err := json.Marshal(lock)
	if err != nil {
		return err
	}

	e.tree.Insert(lockKey, data)
	return nil
}

// GetLock retrieves active lock information on userKey.
func (e *Engine) GetLock(userKey []byte) (*Lock, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	lockKey := EncodeLockKey(userKey)
	data, found := e.tree.Search(lockKey)
	if !found {
		return nil, false
	}

	var lock Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, false
	}
	return &lock, true
}

// DeleteLock removes a lock intent from userKey.
func (e *Engine) DeleteLock(userKey []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	lockKey := EncodeLockKey(userKey)
	e.tree.Delete(lockKey)
	return nil
}

// CommitLock converts a write intent lock into a committed version and deletes the lock.
func (e *Engine) CommitLock(userKey []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	lockKey := EncodeLockKey(userKey)
	lockBytes, found := e.tree.Search(lockKey)
	if !found {
		return core.ErrNotFound
	}

	var lock Lock
	if err := json.Unmarshal(lockBytes, &lock); err != nil {
		return err
	}

	// Remove lock
	e.tree.Delete(lockKey)

	// Write committed version or tombstone
	isTombstone := (lock.Op == core.OpDelete)
	vKey := EncodeVersionKey(userKey, commitTS, isTombstone)
	e.tree.Insert(vKey, lock.Value)

	return nil
}

// RollbackLock removes the write intent lock without creating a version.
func (e *Engine) RollbackLock(userKey []byte) error {
	return e.DeleteLock(userKey)
}
