package bench_test

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/storage/wal"
)

func BenchmarkWAL_Append_SyncNone(b *testing.B) {
	payloadSizes := []int{64, 256, 1024, 4096, 16384}

	for _, sz := range payloadSizes {
		b.Run(fmt.Sprintf("Size=%dB", sz), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "meridian-bench-wal-*")
			if err != nil {
				b.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(dir)

			writer, err := wal.NewWriter(dir, 64*1024*1024, wal.SyncNone)
			if err != nil {
				b.Fatalf("failed to create wal writer: %v", err)
			}
			defer writer.Close()

			payload := make([]byte, sz)
			_, _ = rand.Read(payload)

			b.ResetTimer()
			b.SetBytes(int64(sz))

			for i := 0; i < b.N; i++ {
				rec := wal.Record{
					Type:    core.OpPut,
					Term:    1,
					Index:   core.Index(i + 1),
					Payload: payload,
				}
				if err := writer.Append(rec); err != nil {
					b.Fatalf("append failed at %d: %v", i, err)
				}
			}
		})
	}
}

func BenchmarkWAL_Append_SyncEveryWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "meridian-bench-wal-sync-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	writer, err := wal.NewWriter(dir, 64*1024*1024, wal.SyncEveryWrite)
	if err != nil {
		b.Fatalf("failed to create wal writer: %v", err)
	}
	defer writer.Close()

	payload := make([]byte, 256)
	_, _ = rand.Read(payload)

	b.ResetTimer()
	b.SetBytes(256)

	for i := 0; i < b.N; i++ {
		rec := wal.Record{
			Type:    core.OpPut,
			Term:    1,
			Index:   core.Index(i + 1),
			Payload: payload,
		}
		if err := writer.Append(rec); err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}
}

func BenchmarkWAL_Recovery(b *testing.B) {
	dir, err := os.MkdirTemp("", "meridian-bench-wal-recov-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Pre-populate 5,000 records across 512KB segments
	writer, err := wal.NewWriter(dir, 512*1024, wal.SyncNone)
	if err != nil {
		b.Fatalf("failed to create writer: %v", err)
	}

	payload := []byte("benchmark-record-payload-128-bytes-long-for-recovery-benchmarking-pipeline-meridian-distributed-state-engine")
	const numRecords = 5000
	for i := 1; i <= numRecords; i++ {
		_ = writer.Append(wal.Record{
			Type:    core.OpPut,
			Term:    1,
			Index:   core.Index(i),
			Payload: payload,
		})
	}
	_ = writer.Close()

	b.ResetTimer()
	b.SetBytes(int64(len(payload) * numRecords))

	for i := 0; i < b.N; i++ {
		res, err := wal.Recover(dir)
		if err != nil {
			b.Fatalf("recovery failed: %v", err)
		}
		for _, s := range res.Segments {
			_ = s.Close()
		}
	}
}
