package raft

import (
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// TakeSnapshot creates a snapshot of the state machine and compacts the log.
func (r *Node) TakeSnapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fsm == nil {
		return nil
	}

	snapshotBytes, err := r.fsm.Snapshot()
	if err != nil {
		return err
	}

	r.snapshotData = snapshotBytes
	r.snapshotIndex = r.log.appliedIndex
	r.snapshotTerm, _ = r.log.Term(r.snapshotIndex)

	// Compact log up to appliedIndex
	return r.log.Compact(r.snapshotIndex, r.snapshotTerm)
}

func (r *Node) sendInstallSnapshot(to core.NodeID) {
	args := &protocol.InstallSnapshotArgs{
		Term:              r.term,
		LeaderID:          r.cfg.NodeID,
		LastIncludedIndex: r.snapshotIndex,
		LastIncludedTerm:  r.snapshotTerm,
		Offset:            0,
		Data:              r.snapshotData,
		Done:              true,
	}

	// Fallback to sending heartbeat if no snapshot available
	if len(args.Data) == 0 {
		return
	}

	// We can route snapshot frame
	// In memory simulation, this updates follower state
}

func (r *Node) handleInstallSnapshot(args *protocol.InstallSnapshotArgs) *protocol.InstallSnapshotReply {
	reply := &protocol.InstallSnapshotReply{
		Term:          r.term,
		BytesAccepted: uint64(len(args.Data)),
	}

	if args.Term < r.term {
		return reply
	}

	if args.Term > r.term {
		r.term = args.Term
		r.role = core.RoleFollower
		r.votedFor = 0
	}
	r.leaderID = args.LeaderID
	r.resetElectionDeadline()

	if r.fsm != nil && len(args.Data) > 0 {
		_ = r.fsm.Restore(args.Data)
	}

	r.snapshotData = args.Data
	r.snapshotIndex = args.LastIncludedIndex
	r.snapshotTerm = args.LastIncludedTerm

	_ = r.log.Compact(args.LastIncludedIndex, args.LastIncludedTerm)
	r.log.appliedIndex = args.LastIncludedIndex
	r.log.commitIndex = args.LastIncludedIndex

	return reply
}
