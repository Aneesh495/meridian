package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage/wal"
)

func TestStorageEngineEndToEndCrashRecovery(t *testing.T) {
	dir, err := os.MkdirTemp("", "meridian-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := Options{
		DataDir:        dir,
		MaxSegmentSize: 4096,
		SyncMode:       wal.SyncEveryWrite,
		CacheSizeBytes: 1024 * 1024,
	}

	// 1. Open engine and write 50 records
	engine1, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	for i := 1; i <= 50; i++ {
		k := []byte(fmt.Sprintf("entity:%04d", i))
		v := []byte(fmt.Sprintf("payload-%d", i))
		ts := hlc.NewTimestamp(int64(i*10), 0, 1)

		if err := engine1.Put(k, v, ts); err != nil {
			t.Fatalf("failed to put key %d: %v", i, err)
		}
	}
	_ = engine1.Close()

	// 2. Re-open engine from same directory (crash recovery simulation)
	engine2, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to re-open engine: %v", err)
	}
	defer engine2.Close()

	// 3. Verify all 50 keys recovered and visible at snapshot
	readTS := hlc.NewTimestamp(1000, 0, 1)
	for i := 1; i <= 50; i++ {
		k := []byte(fmt.Sprintf("entity:%04d", i))
		expectedVal := []byte(fmt.Sprintf("payload-%d", i))

		val, ts, err := engine2.Get(k, readTS)
		if err != nil {
			t.Fatalf("failed to get recovered key %d: %v", i, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("value mismatch: expected %s, got %s", expectedVal, val)
		}
		if ts.WallTime != int64(i*10) {
			t.Fatalf("timestamp mismatch: expected %d, got %d", i*10, ts.WallTime)
		}
	}
}
