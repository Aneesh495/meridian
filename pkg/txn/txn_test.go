package txn

import (
	"bytes"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/storage/wal"
)

type mockStorageLookup struct {
	participants map[string]*Participant
}

func (m *mockStorageLookup) GetParticipant(key []byte) (*Participant, error) {
	if bytes.HasPrefix(key, []byte("acc:1")) {
		return m.participants["shard-1"], nil
	}
	return m.participants["shard-2"], nil
}

func TestDistributedAtomic2PCTransfer(t *testing.T) {
	dir1, _ := os.MkdirTemp("", "meridian-txn-s1-*")
	dir2, _ := os.MkdirTemp("", "meridian-txn-s2-*")
	defer os.RemoveAll(dir1)
	defer os.RemoveAll(dir2)

	eng1, err := storage.Open(storage.Options{DataDir: dir1, MaxSegmentSize: 1024 * 1024, SyncMode: wal.SyncEveryWrite, CacheSizeBytes: 1024 * 1024})
	if err != nil {
		t.Fatalf("failed to open eng1: %v", err)
	}
	defer eng1.Close()

	eng2, err := storage.Open(storage.Options{DataDir: dir2, MaxSegmentSize: 1024 * 1024, SyncMode: wal.SyncEveryWrite, CacheSizeBytes: 1024 * 1024})
	if err != nil {
		t.Fatalf("failed to open eng2: %v", err)
	}
	defer eng2.Close()

	clock := hlc.NewClock(1, 250*time.Millisecond)
	ww := NewWoundWaitEngine()
	lt1 := NewLockTable()
	lt2 := NewLockTable()

	p1 := NewParticipant(1, eng1, lt1, ww)
	p2 := NewParticipant(2, eng2, lt2, ww)

	lookup := &mockStorageLookup{
		participants: map[string]*Participant{
			"shard-1": p1,
			"shard-2": p2,
		},
	}

	coord := NewCoordinator(clock, lookup, ww)

	initTxn := coord.Begin(DefaultOptions)
	initTxn.Put([]byte("acc:100"), []byte("1000"))
	initTxn.Put([]byte("acc:200"), []byte("500"))
	if err := initTxn.Commit(); err != nil {
		t.Fatalf("init txn failed: %v", err)
	}

	transferTxn := coord.Begin(DefaultOptions)
	val1, err := transferTxn.Get([]byte("acc:100"))
	if err != nil {
		t.Fatalf("failed to get acc:100: %v", err)
	}
	bal1, _ := strconv.Atoi(string(val1))

	val2, err := transferTxn.Get([]byte("acc:200"))
	if err != nil {
		t.Fatalf("failed to get acc:200: %v", err)
	}
	bal2, _ := strconv.Atoi(string(val2))

	transferTxn.Put([]byte("acc:100"), []byte(strconv.Itoa(bal1-300)))
	transferTxn.Put([]byte("acc:200"), []byte(strconv.Itoa(bal2+300)))

	if err := transferTxn.Commit(); err != nil {
		t.Fatalf("transfer commit failed: %v", err)
	}

	verifyTxn := coord.Begin(DefaultOptions)
	res1, _ := verifyTxn.Get([]byte("acc:100"))
	res2, _ := verifyTxn.Get([]byte("acc:200"))

	if string(res1) != "700" || string(res2) != "800" {
		t.Fatalf("unexpected balances: acc:100=%s, acc:200=%s", string(res1), string(res2))
	}
}
