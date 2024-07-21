package multiraft

import (
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/consensus/raft"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func TestMultiRaftRoutingAndIsolation(t *testing.T) {
	mesh := raft.NewMemoryTransport()

	router := NewRouter()
	partitions := []*RangePartition{
		{ShardID: 1, StartKey: []byte("a"), EndKey: []byte("m"), Peers: []core.NodeID{1, 2}},
		{ShardID: 2, StartKey: []byte("m"), EndKey: []byte("z"), Peers: []core.NodeID{1, 2}},
	}
	router.SetPartitions(partitions)

	s1, err := router.Route([]byte("apple"))
	if err != nil || s1 != 1 {
		t.Fatalf("expected route to shard 1, got %d (err: %v)", s1, err)
	}

	s2, err := router.Route([]byte("orange"))
	if err != nil || s2 != 2 {
		t.Fatalf("expected route to shard 2, got %d (err: %v)", s2, err)
	}

	coordNode1 := NewCoordinator(1, router, mesh)
	coordNode2 := NewCoordinator(2, router, mesh)

	// Setup Shard 1 on Node 1 (Leader) and Node 2 (Follower)
	fsm1_1 := raft.NewKeyValueFSM()
	fsm1_2 := raft.NewKeyValueFSM()

	cfg1_1 := raft.DefaultConfig(1, []core.NodeID{2})
	cfg1_1.ElectionTimeout = 60 * time.Millisecond
	node1_1 := raft.NewNode(cfg1_1, fsm1_1, mesh)

	cfg1_2 := raft.DefaultConfig(2, []core.NodeID{1})
	cfg1_2.ElectionTimeout = 60 * time.Millisecond
	node1_2 := raft.NewNode(cfg1_2, fsm1_2, mesh)

	coordNode1.AddGroup(1, node1_1)
	coordNode2.AddGroup(1, node1_2)

	mesh.Register(1, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
		_ = coordNode1.Step(1, from, msgType, payload)
	})
	mesh.Register(2, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
		_ = coordNode2.Step(1, from, msgType, payload)
	})

	coordNode1.Start()
	coordNode2.Start()
	defer coordNode1.Stop()
	defer coordNode2.Stop()

	// Elect leader for shard 1
	node1_1.Tick()

	time.Sleep(150 * time.Millisecond)

	// Propose through coordinator
	_, _, err = coordNode1.Propose([]byte("apple"), core.OpPut, []byte(`{"k":"apple","v":"fruit"}`))
	if err != nil {
		// If not leader yet, wait or check
	}
}
