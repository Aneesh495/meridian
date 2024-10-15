package bench_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/storage/wal"
	"github.com/Aneesh495/meridian/pkg/txn"
)

type benchLookup struct {
	participants map[string]*txn.Participant
}

func (b *benchLookup) GetParticipant(key []byte) (*txn.Participant, error) {
	if bytes.HasPrefix(key, []byte("shard1:")) {
		return b.participants["s1"], nil
	}
	return b.participants["s2"], nil
}

func BenchmarkTxn_WoundWait_EvaluateConflict(b *testing.B) {
	ww := txn.NewWoundWaitEngine()
	tOld := &txn.Descriptor{
		TxnID:   "txn-older",
		StartTS: hlc.NewTimestamp(100, 0, 1),
	}
	tYoung := &txn.Descriptor{
		TxnID:   "txn-younger",
		StartTS: hlc.NewTimestamp(200, 0, 1),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ww.EvaluateConflict(tOld, tYoung)
	}
}

func BenchmarkTxn_LockTable_AcquireRelease(b *testing.B) {
	lt := txn.NewLockTable()
	lock := &txn.LockInfo{
		Key:       []byte("bench:lock:key"),
		TxnID:     "txn-bench",
		StartTS:   hlc.NewTimestamp(100, 0, 1),
		ExpiresAt: time.Now().Add(time.Minute),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = lt.Acquire(lock)
		lt.Release(lock.Key, lock.TxnID)
	}
}

func BenchmarkTxn_2PC_SingleShard_Commit(b *testing.B) {
	dir, err := os.MkdirTemp("", "meridian-bench-txn-s1-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	eng, err := storage.Open(storage.Options{
		DataDir:        dir,
		MaxSegmentSize: 16 * 1024 * 1024,
		SyncMode:       wal.SyncNone,
		CacheSizeBytes: 4 * 1024 * 1024,
	})
	if err != nil {
		b.Fatalf("failed to open storage: %v", err)
	}
	defer eng.Close()

	clock := hlc.NewClock(1, 250*time.Millisecond)
	ww := txn.NewWoundWaitEngine()
	lt := txn.NewLockTable()
	part := txn.NewParticipant(1, eng, lt, ww)

	lookup := &benchLookup{
		participants: map[string]*txn.Participant{
			"s1": part,
			"s2": part,
		},
	}
	coord := txn.NewCoordinator(clock, lookup, ww)

	val := []byte("benchmark-transaction-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := coord.Begin(txn.DefaultOptions)
		key := []byte(fmt.Sprintf("shard1:item:%06d", i%1000))
		t.Put(key, val)
		if err := t.Commit(); err != nil {
			b.Fatalf("commit failed at %d: %v", i, err)
		}
	}
}

func BenchmarkTxn_2PC_Distributed_CrossShard_Commit(b *testing.B) {
	dir1, _ := os.MkdirTemp("", "meridian-bench-txn-s1-*")
	dir2, _ := os.MkdirTemp("", "meridian-bench-txn-s2-*")
	defer os.RemoveAll(dir1)
	defer os.RemoveAll(dir2)

	eng1, _ := storage.Open(storage.Options{DataDir: dir1, MaxSegmentSize: 16 * 1024 * 1024, SyncMode: wal.SyncNone, CacheSizeBytes: 4 * 1024 * 1024})
	eng2, _ := storage.Open(storage.Options{DataDir: dir2, MaxSegmentSize: 16 * 1024 * 1024, SyncMode: wal.SyncNone, CacheSizeBytes: 4 * 1024 * 1024})
	defer eng1.Close()
	defer eng2.Close()

	clock := hlc.NewClock(1, 250*time.Millisecond)
	ww := txn.NewWoundWaitEngine()
	p1 := txn.NewParticipant(1, eng1, txn.NewLockTable(), ww)
	p2 := txn.NewParticipant(2, eng2, txn.NewLockTable(), ww)

	lookup := &benchLookup{
		participants: map[string]*txn.Participant{
			"s1": p1,
			"s2": p2,
		},
	}
	coord := txn.NewCoordinator(clock, lookup, ww)
	val := []byte("cross-shard-transfer-data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := coord.Begin(txn.DefaultOptions)
		k1 := []byte(fmt.Sprintf("shard1:account:%04d", i%500))
		k2 := []byte(fmt.Sprintf("shard2:account:%04d", i%500))
		t.Put(k1, val)
		t.Put(k2, val)
		if err := t.Commit(); err != nil {
			b.Fatalf("distributed commit failed at %d: %v", i, err)
		}
	}
}
