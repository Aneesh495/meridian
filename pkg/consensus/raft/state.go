package raft

import (
	"github.com/Aneesh495/meridian/pkg/core"
)

// HardState represents persistent consensus state that must be written to disk before replying to RPCs.
type HardState struct {
	Term   core.Term
	Vote   core.NodeID
	Commit core.Index
}

// SoftState represents volatile in-memory consensus state.
type SoftState struct {
	LeaderID core.NodeID
	Role     core.NodeRole
}

// Progress tracks log replication progress for a specific follower node.
type Progress struct {
	MatchIndex  core.Index
	NextIndex   core.Index
	LastContact int64 // timestamp in nanoseconds
	IsPaused    bool
}
