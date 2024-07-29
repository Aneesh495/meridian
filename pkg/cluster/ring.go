package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

var (
	ErrEmptyRing = errors.New("meridian/cluster: hash ring is empty")
)

// HashRing implements a consistent hash ring with virtual nodes (vnodes) for uniform load distribution.
type HashRing struct {
	mu       sync.RWMutex
	vnodes   int
	ring     []uint64
	vnodeMap map[uint64]core.NodeID
	nodes    map[core.NodeID]bool
}

// NewHashRing creates a new HashRing with the specified number of vnodes per physical node.
func NewHashRing(vnodesPerNode int) *HashRing {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 128
	}
	return &HashRing{
		vnodes:   vnodesPerNode,
		vnodeMap: make(map[uint64]core.NodeID),
		nodes:    make(map[core.NodeID]bool),
	}
}

// AddNode registers a physical node on the ring with multiple virtual nodes.
func (h *HashRing) AddNode(nodeID core.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.nodes[nodeID] {
		return
	}
	h.nodes[nodeID] = true

	for i := 0; i < h.vnodes; i++ {
		vnodeKey := fmt.Sprintf("node-%d-vnode-%d", nodeID, i)
		hash := core.Murmur3_64([]byte(vnodeKey), 0)
		h.ring = append(h.ring, hash)
		h.vnodeMap[hash] = nodeID
	}

	sort.Slice(h.ring, func(i, j int) bool { return h.ring[i] < h.ring[j] })
}

// RemoveNode unregisters a physical node and removes all its virtual nodes.
func (h *HashRing) RemoveNode(nodeID core.NodeID) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.nodes[nodeID] {
		return
	}
	delete(h.nodes, nodeID)

	var newRing []uint64
	for _, hash := range h.ring {
		if h.vnodeMap[hash] == nodeID {
			delete(h.vnodeMap, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	h.ring = newRing
}

// GetNode locates the primary node responsible for key using consistent hashing.
func (h *HashRing) GetNode(key []byte) (core.NodeID, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return 0, ErrEmptyRing
	}

	hash := core.Murmur3_64(key, 0)
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx == len(h.ring) {
		idx = 0 // wrap around
	}

	return h.vnodeMap[h.ring[idx]], nil
}

// GetNodes returns count distinct physical nodes responsible for replicas of key.
func (h *HashRing) GetNodes(key []byte, count int) ([]core.NodeID, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return nil, ErrEmptyRing
	}

	if count > len(h.nodes) {
		count = len(h.nodes)
	}

	hash := core.Murmur3_64(key, 0)
	startIdx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	res := make([]core.NodeID, 0, count)
	seen := make(map[core.NodeID]bool)

	for i := 0; i < len(h.ring) && len(res) < count; i++ {
		idx := (startIdx + i) % len(h.ring)
		nodeID := h.vnodeMap[h.ring[idx]]
		if !seen[nodeID] {
			seen[nodeID] = true
			res = append(res, nodeID)
		}
	}

	return res, nil
}
