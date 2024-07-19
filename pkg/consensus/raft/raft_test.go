package raft

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func TestRaft3NodeElectionAndReplication(t *testing.T) {
	mesh := NewMemoryTransport()

	peers := []core.NodeID{1, 2, 3}
	nodes := make(map[core.NodeID]*Node)
	fsms := make(map[core.NodeID]*KeyValueFSM)

	for _, id := range peers {
		nodePeers := make([]core.NodeID, 0, len(peers)-1)
		for _, p := range peers {
			if p != id {
				nodePeers = append(nodePeers, p)
			}
		}

		cfg := DefaultConfig(id, nodePeers)
		cfg.ElectionTimeout = 80 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond

		fsm := NewKeyValueFSM()
		node := NewNode(cfg, fsm, mesh)

		nodes[id] = node
		fsms[id] = fsm

		currID := id
		currNode := node
		mesh.Register(currID, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
			_ = currNode.Step(from, msgType, payload)
		})
		node.Start()
		defer node.Stop()
	}

	// Trigger node 1 to start election
	nodes[1].mu.Lock()
	nodes[1].startElection()
	nodes[1].mu.Unlock()

	// Wait for election to complete
	var leaderID core.NodeID
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		for id, n := range nodes {
			if n.Role() == core.RoleLeader {
				leaderID = id
				break
			}
		}
		if leaderID != 0 {
			break
		}
	}

	if leaderID == 0 {
		t.Fatalf("failed to elect a leader")
	}

	leaderNode := nodes[leaderID]

	// Propose 10 key-value writes on the leader
	for i := 1; i <= 10; i++ {
		cmdBytes, _ := json.Marshal(struct {
			Key   string `json:"k"`
			Value []byte `json:"v"`
		}{
			Key:   "balance",
			Value: []byte{byte(i * 10)},
		})

		if _, err := leaderNode.Propose(core.OpPut, cmdBytes); err != nil {
			t.Fatalf("failed to propose command %d: %v", i, err)
		}
	}

	// Allow replication and commit
	time.Sleep(250 * time.Millisecond)

	// Verify all 3 nodes have applied entries and have identical state
	for id, fsm := range fsms {
		val, ok := fsm.Get("balance")
		if !ok || len(val) == 0 || val[0] != 100 {
			t.Fatalf("node %d failed to commit and apply value 100: ok=%v, val=%v", id, ok, val)
		}
	}
}

func TestRaftPartitionAndFailover(t *testing.T) {
	mesh := NewMemoryTransport()

	peers := []core.NodeID{1, 2, 3}
	nodes := make(map[core.NodeID]*Node)
	fsms := make(map[core.NodeID]*KeyValueFSM)

	for _, id := range peers {
		nodePeers := make([]core.NodeID, 0, len(peers)-1)
		for _, p := range peers {
			if p != id {
				nodePeers = append(nodePeers, p)
			}
		}

		cfg := DefaultConfig(id, nodePeers)
		cfg.ElectionTimeout = 60 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond

		fsm := NewKeyValueFSM()
		node := NewNode(cfg, fsm, mesh)

		nodes[id] = node
		fsms[id] = fsm

		currNode := node
		mesh.Register(id, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
			_ = currNode.Step(from, msgType, payload)
		})
	}

	// Elect Node 1
	nodes[1].mu.Lock()
	nodes[1].startElection()
	nodes[1].mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	if nodes[1].Role() != core.RoleLeader {
		t.Fatalf("node 1 should be leader")
	}

	// Partition Node 1: disconnect its handler from mesh
	mesh.mu.Lock()
	delete(mesh.handlers, 1)
	mesh.mu.Unlock()

	// Remaining nodes 2 and 3 should elect a new leader between them
	nodes[2].mu.Lock()
	nodes[2].startElection()
	nodes[2].mu.Unlock()

	time.Sleep(150 * time.Millisecond)

	var newLeaderID core.NodeID
	if nodes[2].Role() == core.RoleLeader {
		newLeaderID = 2
	} else if nodes[3].Role() == core.RoleLeader {
		newLeaderID = 3
	}

	if newLeaderID == 0 {
		t.Fatalf("nodes 2 and 3 failed to elect a new leader after node 1 partition")
	}

	// Write on new leader
	cmdBytes, _ := json.Marshal(struct {
		Key   string `json:"k"`
		Value []byte `json:"v"`
	}{
		Key:   "status",
		Value: []byte("recovered"),
	})

	if _, err := nodes[newLeaderID].Propose(core.OpPut, cmdBytes); err != nil {
		t.Fatalf("failed to propose on new leader: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	val, ok := fsms[newLeaderID].Get("status")
	if !ok || string(val) != "recovered" {
		t.Fatalf("new leader failed to commit command")
	}
}
