package raft

import (
	"math/rand"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// startPreVote sends pre-vote inquiries to verify if this node can win an election without bumping term.
func (r *Node) startPreVote() {
	r.role = core.RoleCandidate
	r.votes = make(map[core.NodeID]bool)
	r.votes[r.cfg.NodeID] = true

	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()

	args := &protocol.RequestVoteArgs{
		Term:         r.term + 1,
		CandidateID:  r.cfg.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		IsPreVote:    true,
	}

	payload := protocol.EncodeRequestVoteArgs(args)
	_ = r.transport.Broadcast(r.cfg.NodeID, r.cfg.Peers, protocol.MsgRequestVote, payload)
}

// startElection increments the term, transitions to Candidate, votes for self, and requests votes.
func (r *Node) startElection() {
	r.term++
	r.role = core.RoleCandidate
	r.votedFor = r.cfg.NodeID
	r.leaderID = 0
	r.votes = make(map[core.NodeID]bool)
	r.votes[r.cfg.NodeID] = true
	r.resetElectionDeadline()

	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()

	args := &protocol.RequestVoteArgs{
		Term:         r.term,
		CandidateID:  r.cfg.NodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		IsPreVote:    false,
	}

	payload := protocol.EncodeRequestVoteArgs(args)
	_ = r.transport.Broadcast(r.cfg.NodeID, r.cfg.Peers, protocol.MsgRequestVote, payload)

	r.checkElectionQuorum()
}

// handleRequestVote evaluates incoming vote requests according to Raft election safety rules.
func (r *Node) handleRequestVote(args *protocol.RequestVoteArgs) *protocol.RequestVoteReply {
	if args.Term > r.term && !args.IsPreVote {
		r.term = args.Term
		r.role = core.RoleFollower
		r.votedFor = 0
		r.leaderID = 0
	}

	reply := &protocol.RequestVoteReply{
		Term:        r.term,
		VoteGranted: false,
		LeaderID:    r.leaderID,
	}

	if args.Term < r.term {
		return reply
	}

	canVote := false
	if args.IsPreVote {
		canVote = true
	} else {
		canVote = (r.votedFor == 0 || r.votedFor == args.CandidateID)
	}

	if canVote {
		lastIdx := r.log.LastIndex()
		lastTerm := r.log.LastTerm()

		logOk := false
		if args.LastLogTerm > lastTerm {
			logOk = true
		} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx {
			logOk = true
		}

		if logOk {
			reply.VoteGranted = true
			if !args.IsPreVote {
				r.votedFor = args.CandidateID
				r.resetElectionDeadline()
			}
		}
	}

	return reply
}

// handleRequestVoteReply records a vote and transitions to leader if majority is attained.
func (r *Node) handleRequestVoteReply(from core.NodeID, reply *protocol.RequestVoteReply) {
	if reply.Term > r.term {
		r.term = reply.Term
		r.role = core.RoleFollower
		r.votedFor = 0
		r.leaderID = 0
		return
	}

	if r.role != core.RoleCandidate || reply.Term != r.term {
		return
	}

	if reply.VoteGranted {
		r.votes[from] = true
		r.checkElectionQuorum()
	}
}

func (r *Node) checkElectionQuorum() {
	quorum := (len(r.cfg.Peers)+1)/2 + 1
	granted := 0
	for _, v := range r.votes {
		if v {
			granted++
		}
	}

	if granted >= quorum {
		r.becomeLeader()
	}
}

func (r *Node) becomeLeader() {
	r.role = core.RoleLeader
	r.leaderID = r.cfg.NodeID
	r.renewLeaderLease()

	lastIdx := r.log.LastIndex()
	for _, p := range r.cfg.Peers {
		r.progress[p] = &Progress{
			MatchIndex: 0,
			NextIndex:  lastIdx + 1,
		}
	}

	noop := protocol.RaftLogEntry{
		Term:  r.term,
		Index: lastIdx + 1,
		Type:  core.OpBatch,
		Data:  nil,
	}
	r.log.Append(noop)
	r.broadcastAppendEntries()
}

func (r *Node) resetElectionDeadline() {
	jitter := time.Duration(rand.Int63n(int64(150 * time.Millisecond)))
	r.electionDeadline = time.Now().Add(r.cfg.ElectionTimeout + jitter)
}
