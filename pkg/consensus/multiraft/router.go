package multiraft

import (
	"bytes"
	"sort"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Router maps user keys to the responsible RangePartition and ShardID.
type Router struct {
	mu         sync.RWMutex
	partitions []*RangePartition
}

// NewRouter creates an empty partition router.
func NewRouter() *Router {
	return &Router{}
}

// SetPartitions updates the ordered slice of range partitions.
func (r *Router) SetPartitions(parts []*RangePartition) {
	r.mu.Lock()
	defer r.mu.Unlock()

	sorted := make([]*RangePartition, len(parts))
	copy(sorted, parts)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].StartKey, sorted[j].StartKey) < 0
	})
	r.partitions = sorted
}

// Route finds the ShardID responsible for key. Returns core.ErrNotFound if unmapped.
func (r *Router) Route(key []byte) (core.ShardID, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, p := range r.partitions {
		if p.Contains(key) {
			return p.ShardID, nil
		}
	}
	return 0, core.ErrNotFound
}
