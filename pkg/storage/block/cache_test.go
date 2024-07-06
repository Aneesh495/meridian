package block

import (
	"bytes"
	"fmt"
	"testing"
)

func TestBlockCompressor(t *testing.T) {
	original := bytes.Repeat([]byte("meridian-consensus-replicated-log-segment-"), 100)
	compressed, err := Compress(original)
	if err != nil {
		t.Fatalf("compression failed: %v", err)
	}

	if len(compressed) >= len(original) {
		t.Fatalf("expected compression to reduce size: %d vs %d", len(compressed), len(original))
	}

	decompressed, err := Decompress(compressed)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("decompressed data mismatch")
	}
}

func TestStripedBlockCache(t *testing.T) {
	// Small cache capacity: 16 shards * 1KB = 16KB total
	cache := NewCache(16 * 1024)

	// Insert 100 blocks
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("block-%04d", i)
		v := bytes.Repeat([]byte{byte(i)}, 128)
		cache.Put(k, v)
	}

	// Verify recently accessed block exists
	cache.Put("special-block", []byte("special-payload"))
	val, ok := cache.Get("special-block")
	if !ok || string(val) != "special-payload" {
		t.Fatalf("expected special block to exist in cache")
	}
}
