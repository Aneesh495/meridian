package block

import (
	"github.com/Aneesh495/meridian/pkg/core"
)

// NumShards is the number of lock-striped cache shards (16 shards).
const NumShards = 16

// Cache is a striped memory-bounded LRU block cache.
type Cache struct {
	shards [NumShards]*LRUShard
}

// NewCache creates a new Cache with total byte capacity divided across 16 shards.
func NewCache(totalCapacityBytes int) *Cache {
	if totalCapacityBytes < NumShards*1024 {
		totalCapacityBytes = NumShards * 1024
	}
	shardCap := totalCapacityBytes / NumShards

	c := &Cache{}
	for i := 0; i < NumShards; i++ {
		c.shards[i] = newLRUShard(shardCap)
	}
	return c
}

func (c *Cache) shardIndex(key string) int {
	h := core.HashString(key)
	return int(h % NumShards)
}

// Get looks up a block in the cache.
func (c *Cache) Get(key string) ([]byte, bool) {
	idx := c.shardIndex(key)
	return c.shards[idx].Get(key)
}

// Put inserts or updates a block in the cache.
func (c *Cache) Put(key string, val []byte) {
	idx := c.shardIndex(key)
	c.shards[idx].Put(key, val)
}
