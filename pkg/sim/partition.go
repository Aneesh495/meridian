package sim

import (
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

// PartitionMatrix controls network reachability between arbitrary pairs of cluster nodes.
// Supports symmetric partitions, asymmetric (unidirectional) partitions, and multi-component graphs.
type PartitionMatrix struct {
	mu      sync.RWMutex
	blocked map[uint64]bool // (src << 32 | dst) -> true if blocked
}

// NewPartitionMatrix creates a fully-connected partition matrix.
func NewPartitionMatrix() *PartitionMatrix {
	return &PartitionMatrix{
		blocked: make(map[uint64]bool),
	}
}

func pairKey(src, dst core.NodeID) uint64 {
	return (uint64(src) << 32) | uint64(dst)
}

// Block cuts communication from src to dst. If symmetric is true, dst to src is also blocked.
func (pm *PartitionMatrix) Block(src, dst core.NodeID, symmetric bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.blocked[pairKey(src, dst)] = true
	if symmetric {
		pm.blocked[pairKey(dst, src)] = true
	}
}

// Allow restores communication from src to dst. If symmetric is true, dst to src is also restored.
func (pm *PartitionMatrix) Allow(src, dst core.NodeID, symmetric bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	delete(pm.blocked, pairKey(src, dst))
	if symmetric {
		delete(pm.blocked, pairKey(dst, src))
	}
}

// CanCommunicate checks if packets can travel from src to dst.
func (pm *PartitionMatrix) CanCommunicate(src, dst core.NodeID) bool {
	if src == dst {
		return true // Local loopback is never blocked
	}

	pm.mu.RLock()
	defer pm.mu.RUnlock()

	return !pm.blocked[pairKey(src, dst)]
}

// SplitPartitions partitions the cluster into disjoint groups of nodes.
// Nodes in group A cannot communicate with nodes in group B.
func (pm *PartitionMatrix) SplitPartitions(groups ...[]core.NodeID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			for _, n1 := range groups[i] {
				for _, n2 := range groups[j] {
					pm.blocked[pairKey(n1, n2)] = true
					pm.blocked[pairKey(n2, n1)] = true
				}
			}
		}
	}
}

// Isolate cuts a node off from all other nodes in the cluster.
func (pm *PartitionMatrix) Isolate(target core.NodeID, allNodes []core.NodeID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, n := range allNodes {
		if n != target {
			pm.blocked[pairKey(target, n)] = true
			pm.blocked[pairKey(n, target)] = true
		}
	}
}

// Bridge creates a topology where groups can only communicate through a bridge node.
// Nodes within group1 can speak to each other and to the bridge.
// Nodes within group2 can speak to each other and to the bridge.
// Group1 and group2 cannot communicate directly with each other.
func (pm *PartitionMatrix) Bridge(group1, group2 []core.NodeID, bridge core.NodeID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, n1 := range group1 {
		for _, n2 := range group2 {
			pm.blocked[pairKey(n1, n2)] = true
			pm.blocked[pairKey(n2, n1)] = true
		}
	}
	// Ensure bridge can communicate with both
	for _, n1 := range group1 {
		delete(pm.blocked, pairKey(n1, bridge))
		delete(pm.blocked, pairKey(bridge, n1))
	}
	for _, n2 := range group2 {
		delete(pm.blocked, pairKey(n2, bridge))
		delete(pm.blocked, pairKey(bridge, n2))
	}
}

// Heal restores full pairwise connectivity across the entire cluster.
func (pm *PartitionMatrix) Heal() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.blocked = make(map[uint64]bool)
}
