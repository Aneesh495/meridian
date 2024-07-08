package storage

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage/block"
	"github.com/Aneesh495/meridian/pkg/storage/mvcc"
	"github.com/Aneesh495/meridian/pkg/storage/wal"
)

// Options configures the storage engine parameters.
type Options struct {
	DataDir        string
	MaxSegmentSize uint64
	SyncMode       wal.SyncMode
	CacheSizeBytes int
}

// DefaultOptions provides standard defaults for storage engines.
var DefaultOptions = Options{
	DataDir:        ".data/storage",
	MaxSegmentSize: 64 * 1024 * 1024,
	SyncMode:       wal.SyncEveryWrite,
	CacheSizeBytes: 64 * 1024 * 1024,
}

// Engine provides durable multi-version transactional key-value storage.
type Engine struct {
	mu        sync.RWMutex
	opts      Options
	walWriter *wal.Writer
	mvcc      *mvcc.Engine
	cache     *block.Cache
	lastTerm  core.Term
	lastIndex core.Index
	closed    bool
}

// Open initializes or recovers a storage engine from disk.
func Open(opts Options) (*Engine, error) {
	// First run crash recovery on existing WAL segments
	recResult, err := wal.Recover(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("storage engine recovery failed: %w", err)
	}

	mvccEngine := mvcc.NewEngine()

	// Replay recovered WAL records into MVCC state
	for _, rec := range recResult.Records {
		switch rec.Type {
		case core.OpPut:
			var item struct {
				Key   []byte        `json:"k"`
				Value []byte        `json:"v"`
				TS    hlc.Timestamp `json:"t"`
			}
			if err := json.Unmarshal(rec.Payload, &item); err == nil {
				_ = mvccEngine.Put(item.Key, item.Value, item.TS)
			}
		case core.OpDelete:
			var item struct {
				Key []byte        `json:"k"`
				TS  hlc.Timestamp `json:"t"`
			}
			if err := json.Unmarshal(rec.Payload, &item); err == nil {
				_ = mvccEngine.Delete(item.Key, item.TS)
			}
		}
	}

	var nextSegID uint64 = 1
	for _, s := range recResult.Segments {
		if s.ID() >= nextSegID {
			nextSegID = s.ID() + 1
		}
		_ = s.Close()
	}

	// Open WAL writer for new appends
	writer, err := wal.NewWriterWithStartID(opts.DataDir, nextSegID, opts.MaxSegmentSize, opts.SyncMode)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL writer: %w", err)
	}

	cache := block.NewCache(opts.CacheSizeBytes)

	return &Engine{
		opts:      opts,
		walWriter: writer,
		mvcc:      mvccEngine,
		cache:     cache,
		lastTerm:  recResult.LastTerm,
		lastIndex: recResult.LastIndex,
	}, nil
}

// Put writes and commits a key-value pair at commitTS, appending to WAL.
func (e *Engine) Put(key []byte, value []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	payload, err := json.Marshal(struct {
		Key   []byte        `json:"k"`
		Value []byte        `json:"v"`
		TS    hlc.Timestamp `json:"t"`
	}{
		Key:   key,
		Value: value,
		TS:    commitTS,
	})
	if err != nil {
		return err
	}

	e.lastIndex++
	rec := wal.Record{
		Type:    core.OpPut,
		Term:    e.lastTerm,
		Index:   e.lastIndex,
		Payload: payload,
	}

	if err := e.walWriter.Append(rec); err != nil {
		return err
	}

	if err := e.mvcc.Put(key, value, commitTS); err != nil {
		return err
	}
	e.cache.Put(string(key), value)

	return nil
}

// Delete marks a key deleted with a tombstone at commitTS.
func (e *Engine) Delete(key []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	payload, err := json.Marshal(struct {
		Key []byte        `json:"k"`
		TS  hlc.Timestamp `json:"t"`
	}{
		Key: key,
		TS:  commitTS,
	})
	if err != nil {
		return err
	}

	e.lastIndex++
	rec := wal.Record{
		Type:    core.OpDelete,
		Term:    e.lastTerm,
		Index:   e.lastIndex,
		Payload: payload,
	}

	if err := e.walWriter.Append(rec); err != nil {
		return err
	}

	return e.mvcc.Delete(key, commitTS)
}

// Get reads the key value visible at readTS.
func (e *Engine) Get(key []byte, readTS hlc.Timestamp) ([]byte, hlc.Timestamp, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, hlc.ZeroTimestamp, core.ErrClosed
	}

	return e.mvcc.Get(key, readTS)
}

// PutLock registers a 2PC write intent lock.
func (e *Engine) PutLock(key []byte, lock *mvcc.Lock) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	return e.mvcc.PutLock(key, lock)
}

// CommitLock resolves a 2PC write intent to a committed version.
func (e *Engine) CommitLock(key []byte, commitTS hlc.Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	return e.mvcc.CommitLock(key, commitTS)
}

// RollbackLock removes a 2PC write intent without committing.
func (e *Engine) RollbackLock(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	return e.mvcc.RollbackLock(key)
}

// NewSnapshotIterator creates an iterator over keys with prefix visible at readTS.
func (e *Engine) NewSnapshotIterator(prefix []byte, readTS hlc.Timestamp) *mvcc.SnapshotIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.mvcc.NewSnapshotIterator(prefix, readTS)
}

// Sync flushes the WAL to disk.
func (e *Engine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return core.ErrClosed
	}

	return e.walWriter.Sync()
}

// Close gracefully closes the storage engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true
	return e.walWriter.Close()
}
