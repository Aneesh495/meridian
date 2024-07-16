package raft

import (
	"sort"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func (r *Node) broadcastAppendEntries() {
	for _, p := range r.cfg.Peers {
		r.sendAppendEntries(p)
	}
}

func (r *Node) sendAppendEntries(to core.NodeID) {
	prog, ok := r.progress[to]
	if !ok {
		return
	}

	prevIndex := prog.NextIndex - 1
	prevTerm, err := r.log.Term(prevIndex)
	if err != nil {
		// If prevIndex has been compacted into a snapshot, trigger snapshot streaming
		r.sendInstallSnapshot(to)
		return
	}

	entries, _ := r.log.Slice(prog.NextIndex, r.cfg.MaxBatchSize)

	args := &protocol.AppendEntriesArgs{
		Term:         r.term,
		LeaderID:     r.cfg.NodeID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: r.log.commitIndex,
		Entries:      entries,
	}

	payload := protocol.EncodeAppendEntriesArgs(args)
	_ = r.transport.Send(to, protocol.MsgAppendEntries, payload)
}

func (r *Node) handleAppendEntries(args *protocol.AppendEntriesArgs) *protocol.AppendEntriesReply {
	reply := &protocol.AppendEntriesReply{
		Term:       r.term,
		Success:    false,
		MatchIndex: 0,
	}

	if args.Term < r.term {
		reply.RejectReason = "stale term"
		return reply
	}

	// Legitimate leader confirmed
	r.resetElectionDeadline()
	if args.Term > r.term || r.role != core.RoleFollower {
		r.term = args.Term
		r.role = core.RoleFollower
		r.votedFor = 0
	}
	r.leaderID = args.LeaderID

	// Check if log contains an entry at PrevLogIndex matching PrevLogTerm
	prevTerm, err := r.log.Term(args.PrevLogIndex)
	if err != nil {
		reply.RejectReason = "missing prevLogIndex"
		reply.MatchIndex = r.log.LastIndex()
		return reply
	}
	if prevTerm != args.PrevLogTerm {
		reply.RejectReason = "term mismatch at prevLogIndex"
		// Optimize recovery: reject back to prevLogIndex
		reply.MatchIndex = args.PrevLogIndex
		return reply
	}

	// Append entries, truncating conflicting suffixes if any
	for _, entry := range args.Entries {
		if entry.Index <= r.log.LastIndex() {
			existingTerm, _ := r.log.Term(entry.Index)
			if existingTerm != entry.Term {
				r.log.Truncate(entry.Index)
				r.log.Append(entry)
			}
		} else {
			r.log.Append(entry)
		}
	}

	// Advance commit index if leader commit is ahead
	if args.LeaderCommit > r.log.commitIndex {
		lastNewIndex := args.PrevLogIndex + core.Index(len(args.Entries))
		if args.LeaderCommit < lastNewIndex {
			r.log.commitIndex = args.LeaderCommit
		} else {
			r.log.commitIndex = lastNewIndex
		}
		r.applyEntries()
	}

	reply.Success = true
	reply.MatchIndex = args.PrevLogIndex + core.Index(len(args.Entries))
	return reply
}

func (r *Node) handleAppendEntriesReply(from core.NodeID, reply *protocol.AppendEntriesReply) {
	if reply.Term > r.term {
		r.term = reply.Term
		r.role = core.RoleFollower
		r.votedFor = 0
		r.leaderID = 0
		return
	}

	if r.role != core.RoleLeader || reply.Term != r.term {
		return
	}

	prog, ok := r.progress[from]
	if !ok {
		return
	}

	if reply.Success {
		if reply.MatchIndex > prog.MatchIndex {
			prog.MatchIndex = reply.MatchIndex
			prog.NextIndex = reply.MatchIndex + 1
		}
		prog.LastContact = time.Now().UnixNano()
		r.checkCommitQuorum()
	} else {
		// Decrement nextIndex and retry immediately
		if prog.NextIndex > 1 {
			prog.NextIndex--
		}
		r.sendAppendEntries(from)
	}
}

func (r *Node) checkCommitQuorum() {
	matchIndexes := make([]uint64, 0, len(r.cfg.Peers)+1)
	matchIndexes = append(matchIndexes, uint64(r.log.LastIndex())) // leader's own index
	for _, prog := range r.progress {
		matchIndexes = append(matchIndexes, uint64(prog.MatchIndex))
	}

	sort.Slice(matchIndexes, func(i, j int) bool { return matchIndexes[i] < matchIndexes[j] })

	// Median index represents majority agreement
	quorumIdx := len(matchIndexes) / 2
	median := core.Index(matchIndexes[quorumIdx])

	if median > r.log.commitIndex {
		medianTerm, err := r.log.Term(median)
		// Only commit entries from the current term directly (Raft figure 8 safety rule)
		if err == nil && medianTerm == r.term {
			r.log.commitIndex = median
			r.applyEntries()
			r.renewLeaderLease()
		}
	}
}

func (r *Node) applyEntries() {
	for r.log.appliedIndex < r.log.commitIndex {
		nextIdx := r.log.appliedIndex + 1
		entries, err := r.log.Slice(nextIdx, 1)
		if err != nil || len(entries) == 0 {
			break
		}

		entry := entries[0]
		if r.fsm != nil && len(entry.Data) > 0 {
			_, _ = r.fsm.Apply(entry)
		}
		r.log.appliedIndex = nextIdx

		// Notify waiting client propose channels if any
		if ch, ok := r.proposeWaiters[nextIdx]; ok {
			close(ch)
			delete(r.proposeWaiters, nextIdx)
		}
	}
}
