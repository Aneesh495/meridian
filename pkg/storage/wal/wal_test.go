package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
)

func TestWALAppendAndReadSequential(t *testing.T) {
	dir, err := os.MkdirTemp("", "meridian-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Use small 1KB segment size to force frequent rollovers
	writer, err := NewWriter(dir, 1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	const totalRecords = 50
	for i := 1; i <= totalRecords; i++ {
		rec := Record{
			Type:    core.OpPut,
			Term:    1,
			Index:   core.Index(i),
			Payload: []byte(fmt.Sprintf("key-%04d=val-%04d", i, i)),
		}
		if err := writer.Append(rec); err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}
	_ = writer.Close()

	// Recover and verify
	result, err := Recover(dir)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer func() {
		for _, s := range result.Segments {
			_ = s.Close()
		}
	}()

	if len(result.Records) != totalRecords {
		t.Fatalf("expected %d records, got %d", totalRecords, len(result.Records))
	}

	if result.LastIndex != totalRecords {
		t.Fatalf("expected last index %d, got %d", totalRecords, result.LastIndex)
	}

	for i, rec := range result.Records {
		expectedIdx := core.Index(i + 1)
		if rec.Index != expectedIdx {
			t.Fatalf("index mismatch: expected %d, got %d", expectedIdx, rec.Index)
		}
		expectedPayload := fmt.Sprintf("key-%04d=val-%04d", i+1, i+1)
		if !bytes.Equal(rec.Payload, []byte(expectedPayload)) {
			t.Fatalf("payload mismatch at %d: %s vs %s", i, string(rec.Payload), expectedPayload)
		}
	}
}

func TestWALRecoveryTornWrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "meridian-wal-torn-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	writer, err := NewWriter(dir, 1024*1024, SyncEveryWrite)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	for i := 1; i <= 10; i++ {
		rec := Record{
			Type:    core.OpPut,
			Term:    2,
			Index:   core.Index(i),
			Payload: []byte(fmt.Sprintf("stable-record-%d", i)),
		}
		if err := writer.Append(rec); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}
	_ = writer.Close()

	// Simulate a torn partial write by appending 10 bytes of garbage to segment file
	segPath := filepath.Join(dir, "0000000000000001.wal")
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open segment: %v", err)
	}
	_, _ = f.Write([]byte("torn-garba"))
	_ = f.Close()

	// Recover should detect and repair the torn write
	result, err := Recover(dir)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer func() {
		for _, s := range result.Segments {
			_ = s.Close()
		}
	}()

	if len(result.Records) != 10 {
		t.Fatalf("expected 10 records after torn write recovery, got %d", len(result.Records))
	}
}
