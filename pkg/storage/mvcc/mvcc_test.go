package mvcc

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

func TestMVCCPointInTimeReads(t *testing.T) {
	engine := NewEngine()
	userKey := []byte("account:alice")

	t1 := hlc.NewTimestamp(100, 0, 1)
	t2 := hlc.NewTimestamp(200, 0, 1)
	t3 := hlc.NewTimestamp(300, 0, 1)

	// Write at t1: $100
	_ = engine.Put(userKey, []byte("100"), t1)
	// Write at t2: $250
	_ = engine.Put(userKey, []byte("250"), t2)
	// Delete at t3
	_ = engine.Delete(userKey, t3)

	// Read before t1 -> Not Found
	t0 := hlc.NewTimestamp(50, 0, 1)
	_, _, err := engine.Get(userKey, t0)
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected ErrNotFound at t0, got %v", err)
	}

	// Read between t1 and t2 -> $100
	t1_5 := hlc.NewTimestamp(150, 0, 1)
	val, ts, err := engine.Get(userKey, t1_5)
	if err != nil || string(val) != "100" || !ts.Equal(t1) {
		t.Fatalf("expected $100 at t1_5, got '%s' (err: %v)", string(val), err)
	}

	// Read between t2 and t3 -> $250
	t2_5 := hlc.NewTimestamp(250, 0, 1)
	val, ts, err = engine.Get(userKey, t2_5)
	if err != nil || string(val) != "250" || !ts.Equal(t2) {
		t.Fatalf("expected $250 at t2_5, got '%s' (err: %v)", string(val), err)
	}

	// Read after t3 (delete) -> Not Found
	t4 := hlc.NewTimestamp(400, 0, 1)
	_, _, err = engine.Get(userKey, t4)
	if err == nil || !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after deletion at t4, got %v", err)
	}
}

func TestMVCCLockIntentCommitWorkflow(t *testing.T) {
	engine := NewEngine()
	userKey := []byte("stock:AAPL")

	startTS := hlc.NewTimestamp(100, 0, 1)
	lock := &Lock{
		Primary: userKey,
		TxnID:   "txn-trade-001",
		StartTS: startTS,
		Op:      core.OpPut,
		Value:   []byte("185.50"),
	}

	if err := engine.PutLock(userKey, lock); err != nil {
		t.Fatalf("failed to put lock: %v", err)
	}

	// Read at ts >= startTS must be blocked by the lock
	readTS := hlc.NewTimestamp(150, 0, 1)
	_, _, err := engine.Get(userKey, readTS)
	if err == nil || !errors.Is(err, core.ErrKeyLocked) {
		t.Fatalf("expected ErrKeyLocked during active lock, got %v", err)
	}

	// Commit lock at t=200
	commitTS := hlc.NewTimestamp(200, 0, 1)
	if err := engine.CommitLock(userKey, commitTS); err != nil {
		t.Fatalf("failed to commit lock: %v", err)
	}

	// Read now succeeds
	val, ts, err := engine.Get(userKey, commitTS)
	if err != nil || string(val) != "185.50" || !ts.Equal(commitTS) {
		t.Fatalf("expected committed value 185.50, got %s (err: %v)", string(val), err)
	}
}

func TestMVCCSnapshotIteratorAndVacuum(t *testing.T) {
	engine := NewEngine()

	// Insert versions for multiple keys
	k1 := []byte("users:001")
	k2 := []byte("users:002")

	_ = engine.Put(k1, []byte("Alice-v1"), hlc.NewTimestamp(10, 0, 1))
	_ = engine.Put(k1, []byte("Alice-v2"), hlc.NewTimestamp(20, 0, 1))
	_ = engine.Put(k1, []byte("Alice-v3"), hlc.NewTimestamp(30, 0, 1))

	_ = engine.Put(k2, []byte("Bob-v1"), hlc.NewTimestamp(15, 0, 1))

	// Snapshot iterator at t=25
	iter := engine.NewSnapshotIterator([]byte("users:"), hlc.NewTimestamp(25, 0, 1))
	var seen [][]byte
	for iter.Next() {
		entry := iter.Entry()
		seen = append(seen, entry.Value)
	}

	if len(seen) != 2 {
		t.Fatalf("expected 2 visible user keys, got %d", len(seen))
	}

	// Vacuum versions older than t=25
	reclaimed, err := engine.Vacuum(hlc.NewTimestamp(25, 0, 1))
	if err != nil {
		t.Fatalf("vacuum failed: %v", err)
	}
	if reclaimed < 1 {
		t.Fatalf("expected at least 1 obsolete version reclaimed, got %d", reclaimed)
	}

	// Verify Alice-v3 is still visible at t=30
	val, _, err := engine.Get(k1, hlc.NewTimestamp(30, 0, 1))
	if err != nil || !bytes.Equal(val, []byte("Alice-v3")) {
		t.Fatalf("latest version should remain intact after vacuum")
	}
}
