package raft

import (
	"github.com/Aneesh495/meridian/pkg/core"
)

// renewLeaderLease extends the leader lease deadline upon quorum heartbeat or commit acknowledgment.
func (r *Node) renewLeaderLease() {
	r.leaseExpires = r.Now().Add(r.cfg.LeaderLeaseTTL)
}

// HasValidLease returns true if the leader currently holds a valid lease.
func (r *Node) HasValidLease() bool {
	return r.role == core.RoleLeader && r.Now().Before(r.leaseExpires)
}

// LinearizableReadIndex returns the linearizable commit index.
// If the leader holds an active lease, it returns immediately without disk or network I/O.
func (r *Node) LinearizableReadIndex() (core.Index, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.role != core.RoleLeader {
		return 0, core.ErrNotLeader
	}

	if r.HasValidLease() {
		return r.log.commitIndex, nil
	}

	// Lease expired: broadcast heartbeat to confirm leadership
	r.broadcastAppendEntries()
	return r.log.commitIndex, nil
}
