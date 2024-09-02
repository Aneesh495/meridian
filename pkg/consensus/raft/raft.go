package raft

import (
	"errors"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

var (
	ErrProposeTimeout = errors.New("meridian/raft: propose execution timed out")
)

// Node represents an active participant in a Raft consensus cluster.
type Node struct {
	mu               sync.RWMutex
	cfg              Config
	role             core.NodeRole
	term             core.Term
	votedFor         core.NodeID
	leaderID         core.NodeID
	log              *Log
	fsm              StateMachine
	transport        Transport
	progress         map[core.NodeID]*Progress
	votes            map[core.NodeID]bool
	electionDeadline time.Time
	leaseExpires     time.Time
	proposeWaiters   map[core.Index]chan struct{}
	snapshotData     []byte
	snapshotIndex    core.Index
	snapshotTerm     core.Term
	timeSource       func() time.Time
	stopChan         chan struct{}
	stopped          bool
}

// SetTimeSource configures an explicit physical or virtual time provider.
func (r *Node) SetTimeSource(ts func() time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeSource = ts
	r.resetElectionDeadline()
}

// Now returns current time according to configured physical or virtual time provider.
func (r *Node) Now() time.Time {
	if r.timeSource != nil {
		return r.timeSource()
	}
	return time.Now()
}

// NewNode instantiates a new Raft Node.
func NewNode(cfg Config, fsm StateMachine, transport Transport) *Node {
	n := &Node{
		cfg:            cfg,
		role:           core.RoleFollower,
		term:           0,
		votedFor:       0,
		leaderID:       0,
		log:            NewLog(),
		fsm:            fsm,
		transport:      transport,
		progress:       make(map[core.NodeID]*Progress),
		votes:          make(map[core.NodeID]bool),
		proposeWaiters: make(map[core.Index]chan struct{}),
		stopChan:       make(chan struct{}),
	}
	n.resetElectionDeadline()
	return n
}

// Start begins background timers.
func (r *Node) Start() {
	go r.run()
}

// Stop gracefully shuts down the Raft node.
func (r *Node) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stopped {
		r.stopped = true
		close(r.stopChan)
	}
}

func (r *Node) run() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.Tick()
		case <-r.stopChan:
			return
		}
	}
}

// Tick advances internal clocks and triggers elections or heartbeats.
func (r *Node) Tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.Now()

	switch r.role {
	case core.RoleFollower, core.RoleCandidate:
		if now.After(r.electionDeadline) {
			if r.cfg.EnablePreVote {
				r.startPreVote()
			} else {
				r.startElection()
			}
		}

	case core.RoleLeader:
		r.broadcastAppendEntries()
	}
}

// Step processes an incoming Raft RPC message.
func (r *Node) Step(from core.NodeID, msgType protocol.MsgType, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch msgType {
	case protocol.MsgRequestVote:
		args, err := protocol.DecodeRequestVoteArgs(payload)
		if err != nil {
			return err
		}
		reply := r.handleRequestVote(args)
		replyBytes := protocol.EncodeRequestVoteReply(reply)
		_ = r.transport.Send(r.cfg.NodeID, args.CandidateID, protocol.MsgRequestVoteResp, replyBytes)

	case protocol.MsgRequestVoteResp:
		reply, err := protocol.DecodeRequestVoteReply(payload)
		if err != nil {
			return err
		}
		r.handleRequestVoteReply(from, reply)

	case protocol.MsgAppendEntries:
		args, err := protocol.DecodeAppendEntriesArgs(payload)
		if err != nil {
			return err
		}
		reply := r.handleAppendEntries(args)
		replyBytes := protocol.EncodeAppendEntriesReply(reply)
		_ = r.transport.Send(r.cfg.NodeID, args.LeaderID, protocol.MsgAppendEntriesResp, replyBytes)

	case protocol.MsgAppendEntriesResp:
		reply, err := protocol.DecodeAppendEntriesReply(payload)
		if err != nil {
			return err
		}
		r.handleAppendEntriesReply(from, reply)
	}

	return nil
}

// Propose appends a client mutation entry to the replicated log.
func (r *Node) Propose(op core.OpType, data []byte) (core.Index, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != core.RoleLeader {
		return 0, core.ErrNotLeader
	}

	index := r.log.LastIndex() + 1
	entry := protocol.RaftLogEntry{
		Term:  r.term,
		Index: index,
		Type:  op,
		Data:  data,
	}

	r.log.Append(entry)
	waiter := make(chan struct{})
	r.proposeWaiters[index] = waiter

	r.broadcastAppendEntries()
	return index, nil
}

// ProposeWait proposes a command and blocks until it is committed and applied or timeout expires.
func (r *Node) ProposeWait(op core.OpType, data []byte, timeout time.Duration) error {
	index, err := r.Propose(op, data)
	if err != nil {
		return err
	}

	r.mu.RLock()
	waiter, exists := r.proposeWaiters[index]
	r.mu.RUnlock()

	if !exists {
		return nil
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-waiter:
		return nil
	case <-timer.C:
		return ErrProposeTimeout
	}
}

// Role returns the current consensus role.
func (r *Node) Role() core.NodeRole {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.role
}

// Term returns the current term.
func (r *Node) Term() core.Term {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.term
}

// LeaderID returns the current recognized cluster leader ID.
func (r *Node) LeaderID() core.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// LastApplied returns the highest applied log index.
func (r *Node) LastApplied() core.Index {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.log.appliedIndex
}
