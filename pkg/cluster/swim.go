package cluster

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// GossipUpdate represents a piggybacked membership state delta.
type GossipUpdate struct {
	ID          core.NodeID  `json:"id"`
	Addr        string       `json:"addr"`
	Status      MemberStatus `json:"status"`
	Incarnation uint64       `json:"inc"`
}

// SWIM manages cluster membership, failure detection, and epidemic state dissemination.
type SWIM struct {
	mu           sync.RWMutex
	self         *Member
	members      map[core.NodeID]*Member
	suspicions   map[core.NodeID]*SuspicionTimer
	gossipQueue  []GossipUpdate
	ring         *HashRing
	transport    func(to core.NodeID, msgType protocol.MsgType, payload []byte) error
	pingTimeout  time.Duration
	probePeriod  time.Duration
	stopChan     chan struct{}
	stopped      bool
}

// NewSWIM initializes a new SWIM gossip node.
func NewSWIM(self *Member, transport func(to core.NodeID, msgType protocol.MsgType, payload []byte) error) *SWIM {
	s := &SWIM{
		self:        self,
		members:     make(map[core.NodeID]*Member),
		suspicions:  make(map[core.NodeID]*SuspicionTimer),
		ring:        NewHashRing(128),
		transport:   transport,
		pingTimeout: 50 * time.Millisecond,
		probePeriod: 100 * time.Millisecond,
		stopChan:    make(chan struct{}),
	}
	s.members[self.ID] = self
	s.ring.AddNode(self.ID)
	return s
}

// AddMember adds or updates a known peer node.
func (s *SWIM) AddMember(m *Member) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.members[m.ID] = m
	s.ring.AddNode(m.ID)
}

// Members returns a snapshot of all known cluster members.
func (s *SWIM) Members() []*Member {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*Member, 0, len(s.members))
	for _, m := range s.members {
		res = append(res, m.Clone())
	}
	return res
}

// Ring returns the consistent hash ring.
func (s *SWIM) Ring() *HashRing {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ring
}

// Start begins periodic SWIM failure detector probe cycles.
func (s *SWIM) Start() {
	go s.probeLoop()
}

// Stop shuts down the gossip engine.
func (s *SWIM) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stopped {
		s.stopped = true
		close(s.stopChan)
		for _, timer := range s.suspicions {
			timer.Cancel()
		}
	}
}

func (s *SWIM) probeLoop() {
	ticker := time.NewTicker(s.probePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.probeRandomMember()
		case <-s.stopChan:
			return
		}
	}
}

func (s *SWIM) probeRandomMember() {
	s.mu.RLock()
	candidates := make([]core.NodeID, 0, len(s.members))
	for id, m := range s.members {
		if id != s.self.ID && m.Status != StatusDead {
			candidates = append(candidates, id)
		}
	}
	s.mu.RUnlock()

	if len(candidates) == 0 {
		return
	}

	targetID := candidates[rand.Intn(len(candidates))]

	// Send direct ping
	payload, _ := json.Marshal(s.collectGossipUpdates())
	_ = s.transport(targetID, protocol.MsgSWIMPing, payload)
}

func (s *SWIM) collectGossipUpdates() []GossipUpdate {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.gossipQueue) == 0 {
		return nil
	}

	updates := s.gossipQueue
	s.gossipQueue = nil
	return updates
}

// ApplyGossip applies received updates according to SWIM incarnation precedence rules.
func (s *SWIM) ApplyGossip(updates []GossipUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, u := range updates {
		// If someone suspects me, refute by bumping incarnation and asserting Alive!
		if u.ID == s.self.ID {
			if u.Status == StatusSuspect && u.Incarnation >= s.self.Incarnation {
				s.self.Incarnation = u.Incarnation + 1
				s.gossipQueue = append(s.gossipQueue, GossipUpdate{
					ID:          s.self.ID,
					Addr:        s.self.Addr,
					Status:      StatusAlive,
					Incarnation: s.self.Incarnation,
				})
			}
			continue
		}

		member, exists := s.members[u.ID]
		if !exists {
			newMember := &Member{
				ID:          u.ID,
				Addr:        u.Addr,
				Status:      u.Status,
				Incarnation: u.Incarnation,
			}
			s.members[u.ID] = newMember
			if u.Status == StatusAlive {
				s.ring.AddNode(u.ID)
			}
			continue
		}

		// Precedence rules:
		// 1. Higher incarnation always wins
		// 2. Same incarnation: Dead > Suspect > Alive
		if u.Incarnation > member.Incarnation {
			member.Incarnation = u.Incarnation
			member.Status = u.Status

			if u.Status == StatusSuspect {
				s.startSuspicionLocked(u.ID)
			} else {
				s.cancelSuspicionLocked(u.ID)
			}
			if u.Status == StatusDead {
				s.ring.RemoveNode(u.ID)
			} else if u.Status == StatusAlive {
				s.ring.AddNode(u.ID)
			}
		} else if u.Incarnation == member.Incarnation {
			if u.Status == StatusDead && member.Status != StatusDead {
				member.Status = StatusDead
				s.ring.RemoveNode(u.ID)
				s.cancelSuspicionLocked(u.ID)
			} else if u.Status == StatusSuspect && member.Status == StatusAlive {
				member.Status = StatusSuspect
				s.startSuspicionLocked(u.ID)
			}
		}
	}
}

func (s *SWIM) startSuspicionLocked(id core.NodeID) {
	if _, ok := s.suspicions[id]; ok {
		return
	}

	timer := NewSuspicionTimer(id, len(s.members), 100*time.Millisecond, 500*time.Millisecond, func(deadID core.NodeID) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if m, ok := s.members[deadID]; ok && m.Status == StatusSuspect {
			m.Status = StatusDead
			s.ring.RemoveNode(deadID)
			delete(s.suspicions, deadID)
		}
	})

	s.suspicions[id] = timer
}

func (s *SWIM) cancelSuspicionLocked(id core.NodeID) {
	if timer, ok := s.suspicions[id]; ok {
		timer.Cancel()
		delete(s.suspicions, id)
	}
}
