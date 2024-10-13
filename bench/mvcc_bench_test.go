package bench_test

import (
	"fmt"
	"testing"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage/mvcc"
)

func BenchmarkMVCC_Put(b *testing.B) {
	engine := mvcc.NewEngine()
	val := []byte("mvcc-benchmark-state-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("account:ledger:%06d", i%1000))
		ts := hlc.NewTimestamp(int64(i+1), 0, 1)
		_ = engine.Put(key, val, ts)
	}
}

func BenchmarkMVCC_Get_Latest(b *testing.B) {
	engine := mvcc.NewEngine()
	const numKeys = 1000
	const versionsPerKey = 10

	for v := 1; v <= versionsPerKey; v++ {
		for k := 0; k < numKeys; k++ {
			key := []byte(fmt.Sprintf("account:balance:%06d", k))
			val := []byte(fmt.Sprintf("balance-v%d", v))
			ts := hlc.NewTimestamp(int64(v*1000+k), 0, 1)
			_ = engine.Put(key, val, ts)
		}
	}

	readTS := hlc.NewTimestamp(int64((versionsPerKey+1)*1000), 0, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("account:balance:%06d", i%numKeys))
		_, _, err := engine.Get(key, readTS)
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkMVCC_Get_Historical(b *testing.B) {
	engine := mvcc.NewEngine()
	const numKeys = 1000
	const versionsPerKey = 10

	for v := 1; v <= versionsPerKey; v++ {
		for k := 0; k < numKeys; k++ {
			key := []byte(fmt.Sprintf("account:balance:%06d", k))
			val := []byte(fmt.Sprintf("balance-v%d", v))
			ts := hlc.NewTimestamp(int64(v*1000), uint16(k%1000), 1)
			_ = engine.Put(key, val, ts)
		}
	}

	historicalTS := hlc.NewTimestamp(3500, 0, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("account:balance:%06d", i%numKeys))
		_, _, err := engine.Get(key, historicalTS)
		if err != nil {
			b.Fatalf("historical read failed: %v", err)
		}
	}
}

func BenchmarkMVCC_Scan_Prefix(b *testing.B) {
	engine := mvcc.NewEngine()
	const count = 1000
	val := []byte("balance-snapshot-data")

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("tenant:001:user:%06d", i))
		ts := hlc.NewTimestamp(1000, uint16(i%1000), 1)
		_ = engine.Put(key, val, ts)
	}

	prefix := []byte("tenant:001:user:")
	scanTS := hlc.NewTimestamp(2000, 0, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := engine.NewSnapshotIterator(prefix, scanTS)
		scanned := 0
		for it.Next() {
			scanned++
		}
		if scanned != count {
			b.Fatalf("scan count mismatch: expected %d, got %d", count, scanned)
		}
	}
}

func BenchmarkMVCC_Vacuum_GC(b *testing.B) {
	engine := mvcc.NewEngine()
	const numKeys = 500
	const versionsPerKey = 10

	for v := 1; v <= versionsPerKey; v++ {
		for k := 0; k < numKeys; k++ {
			key := []byte(fmt.Sprintf("vacuum:key:%06d", k))
			val := []byte("temporary-state")
			ts := hlc.NewTimestamp(int64(v*100), 0, 1)
			_ = engine.Put(key, val, ts)
		}
	}

	cutoffTS := hlc.NewTimestamp(600, 0, 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Vacuum(cutoffTS)
	}
}
