package raft

import (
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Config holds all operational parameters for a Raft consensus node.
type Config struct {
	NodeID            core.NodeID
	Peers             []core.NodeID
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	LeaderLeaseTTL    time.Duration
	EnablePreVote     bool
	MaxBatchSize      int
}

// DefaultConfig provides standard recommended defaults for local and cluster environments.
func DefaultConfig(nodeID core.NodeID, peers []core.NodeID) Config {
	return Config{
		NodeID:            nodeID,
		Peers:             peers,
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		LeaderLeaseTTL:    250 * time.Millisecond,
		EnablePreVote:     true,
		MaxBatchSize:      128,
	}
}
