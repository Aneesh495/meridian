package bench_test

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/Aneesh495/meridian/pkg/storage/art"
)

func BenchmarkART_Insert_Sequential(b *testing.B) {
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("account:seq:%08d", i))
	}
	val := []byte("payload-value-data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := art.NewTree()
		for _, k := range keys {
			tree.Insert(k, val)
		}
	}
}

func BenchmarkART_Insert_Random(b *testing.B) {
	keys := make([][]byte, 10000)
	for i := range keys {
		buf := make([]byte, 16)
		_, _ = rand.Read(buf)
		keys[i] = buf
	}
	val := []byte("payload-value-data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree := art.NewTree()
		for _, k := range keys {
			tree.Insert(k, val)
		}
	}
}

func BenchmarkART_Search_Hit(b *testing.B) {
	tree := art.NewTree()
	const count = 10000
	keys := make([][]byte, count)
	val := []byte("payload-value-data")

	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("user:session:%08d", i))
		tree.Insert(keys[i], val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := keys[i%count]
		_, found := tree.Search(target)
		if !found {
			b.Fatalf("expected key to exist")
		}
	}
}

func BenchmarkART_Search_Miss(b *testing.B) {
	tree := art.NewTree()
	const count = 10000
	val := []byte("payload-value-data")

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("user:session:%08d", i))
		tree.Insert(key, val)
	}

	missKeys := make([][]byte, count)
	for i := 0; i < count; i++ {
		missKeys[i] = []byte(fmt.Sprintf("nonexistent:session:%08d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := missKeys[i%count]
		_, found := tree.Search(target)
		if found {
			b.Fatalf("expected key to miss")
		}
	}
}

func BenchmarkART_Iterator_FullScan(b *testing.B) {
	tree := art.NewTree()
	const count = 10000
	val := []byte("payload-value-data")

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("metric:rate:%08d", i))
		tree.Insert(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := tree.NewIterator(nil)
		scanned := 0
		for it.HasNext() {
			_, _ = it.Next()
			scanned++
		}
		if scanned != count {
			b.Fatalf("iterator count mismatch: %d", scanned)
		}
	}
}

func BenchmarkGoMap_Search_Comparison(b *testing.B) {
	m := make(map[string][]byte, 10000)
	const count = 10000
	keys := make([]string, count)
	val := []byte("payload-value-data")

	for i := 0; i < count; i++ {
		keys[i] = fmt.Sprintf("user:session:%08d", i)
		m[keys[i]] = val
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := keys[i%count]
		_ = m[target]
	}
}
