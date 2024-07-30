package cluster

import (
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func TestConsistentHashRingDistribution(t *testing.T) {
	ring := NewHashRing(128)
	ring.AddNode(1)
	ring.AddNode(2)
	ring.AddNode(3)

	counts := make(map[core.NodeID]int)
	const totalKeys = 3000

	for i := 0; i < totalKeys; i++ {
		key := []byte(core.GenerateID(8))
		nodeID, err := ring.GetNode(key)
		if err != nil {
			t.Fatalf("failed to locate node for key: %v", err)
		}
		counts[nodeID]++
	}

	// Verify each of the 3 nodes receives roughly 1/3 of the keys (between 700 and 1300)
	for id, cnt := range counts {
		if cnt < 700 || cnt > 1300 {
			t.Fatalf("unbalanced distribution for node %d: %d keys (expected ~1000)", id, cnt)
		}
	}

	// Test replica placement
	replicas, err := ring.GetNodes([]byte("partition-data-block"), 3)
	if err != nil || len(replicas) != 3 {
		t.Fatalf("expected 3 distinct replica nodes, got %v (err: %v)", replicas, err)
	}
	if replicas[0] == replicas[1] || replicas[1] == replicas[2] || replicas[0] == replicas[2] {
		t.Fatalf("replicas must be pairwise distinct: %v", replicas)
	}
}

func TestSWIMSuspicionAndRefutation(t *testing.T) {
	node1 := &Member{ID: 1, Addr: "127.0.0.1:8001", Status: StatusAlive, Incarnation: 1}
	node2 := &Member{ID: 2, Addr: "127.0.0.1:8002", Status: StatusAlive, Incarnation: 1}

	swim1 := NewSWIM(node1, func(to core.NodeID, msgType protocol.MsgType, payload []byte) error { return nil })
	swim1.AddMember(node2)

	// Node 1 receives gossip that Node 2 is Suspect at Incarnation 1
	swim1.ApplyGossip([]GossipUpdate{
		{ID: 2, Addr: node2.Addr, Status: StatusSuspect, Incarnation: 1},
	})

	members := swim1.Members()
	var n2Status MemberStatus
	for _, m := range members {
		if m.ID == 2 {
			n2Status = m.Status
		}
	}
	if n2Status != StatusSuspect {
		t.Fatalf("expected node 2 to be in Suspect status, got %v", n2Status)
	}

	// Node 2 refutes by broadcasting Alive at Incarnation 2
	swim1.ApplyGossip([]GossipUpdate{
		{ID: 2, Addr: node2.Addr, Status: StatusAlive, Incarnation: 2},
	})

	members = swim1.Members()
	for _, m := range members {
		if m.ID == 2 {
			n2Status = m.Status
		}
	}
	if n2Status != StatusAlive {
		t.Fatalf("expected node 2 to be refuted back to Alive, got %v", n2Status)
	}
}

func TestSuspicionTimerTimeout(t *testing.T) {
	deadCalled := make(chan bool, 1)
	st := NewSuspicionTimer(5, 5, 20*time.Millisecond, 50*time.Millisecond, func(id core.NodeID) {
		deadCalled <- true
	})
	defer st.Cancel()

	// Accelerate by confirmations
	st.Confirm()
	st.Confirm()

	select {
	case <-deadCalled:
		// success
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("timed out waiting for suspicion timer to declare node dead")
	}
}
