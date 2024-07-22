package txn

import (
	"bytes"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

// LockInfo describes an active lock held on a specific key.
type LockInfo struct {
	Key         []byte
	TxnID       core.TxnID
	Primary     []byte
	StartTS     hlc.Timestamp
	MinCommitTS hlc.Timestamp
	ExpiresAt   time.Time
	Op          core.OpType
	Value       []byte
	IsPrimary   bool
}

// LockTable maintains in-memory active transaction locks with expiration tracking.
type LockTable struct {
	mu    sync.RWMutex
	locks map[string]*LockInfo
}

// NewLockTable allocates an empty lock table.
func NewLockTable() *LockTable {
	return &LockTable{
		locks: make(map[string]*LockInfo),
	}
}

// Acquire attempts to acquire a lock on key.
// Returns existing lock if already held.
func (lt *LockTable) Acquire(lock *LockInfo) (*LockInfo, bool) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	kStr := string(lock.Key)
	existing, ok := lt.locks[kStr]
	if ok {
		// If held by the same transaction, allow idempotent lock update
		if existing.TxnID == lock.TxnID {
			lt.locks[kStr] = lock
			return nil, true
		}
		// Check if existing lock has expired
		if time.Now().After(existing.ExpiresAt) {
			// Expired lock can be usurped
			lt.locks[kStr] = lock
			return nil, true
		}
		return existing, false
	}

	lt.locks[kStr] = lock
	return nil, true
}

// Release removes the lock on key held by txnID.
func (lt *LockTable) Release(key []byte, txnID core.TxnID) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	kStr := string(key)
	existing, ok := lt.locks[kStr]
	if !ok || existing.TxnID != txnID {
		return false
	}

	delete(lt.locks, kStr)
	return true
}

// Get retrieves the lock info on key if currently held.
func (lt *LockTable) Get(key []byte) (*LockInfo, bool) {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	lock, ok := lt.locks[string(key)]
	if !ok {
		return nil, false
	}
	if time.Now().After(lock.ExpiresAt) {
		return nil, false
	}
	return lock, true
}

// IsLockedByOther checks if key is locked by any transaction other than txnID.
func (lt *LockTable) IsLockedByOther(key []byte, txnID core.TxnID) (*LockInfo, bool) {
	lock, ok := lt.Get(key)
	if !ok {
		return nil, false
	}
	if lock.TxnID == txnID {
		return nil, false
	}
	return lock, true
}
