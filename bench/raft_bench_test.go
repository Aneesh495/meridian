package bench_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/consensus/multiraft"
	"github.com/Aneesh495/meridian/pkg/consensus/raft"
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func BenchmarkRaft_LogAppend(b *testing.B) {
	log := raft.NewLog()
	payload := []byte("raft-log-entry-state-payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := protocol.RaftLogEntry{
			Term:  1,
			Index: core.Index(i + 1),
			Type:  core.OpPut,
			Data:  payload,
		}
		log.Append(entry)
	}
}

func BenchmarkRaft_Propose_Throughput(b *testing.B) {
	mesh := raft.NewMemoryTransport()
	peers := []core.NodeID{1, 2, 3}
	nodes := make(map[core.NodeID]*raft.Node)

	for _, id := range peers {
		nodePeers := make([]core.NodeID, 0, len(peers)-1)
		for _, p := range peers {
			if p != id {
				nodePeers = append(nodePeers, p)
			}
		}

		cfg := raft.DefaultConfig(id, nodePeers)
		cfg.ElectionTimeout = 50 * time.Millisecond
		cfg.HeartbeatInterval = 15 * time.Millisecond

		fsm := raft.NewKeyValueFSM()
		node := raft.NewNode(cfg, fsm, mesh)

		nodes[id] = node
		currNode := node
		mesh.Register(id, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
			_ = currNode.Step(from, msgType, payload)
		})
		node.Start()
		defer node.Stop()
	}

	// Trigger node 1 to start election and become leader
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		if nodes[1].Role() == core.RoleLeader {
			break
		}
	}

	leader := nodes[1]
	payload := []byte("state-replicated-command")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = leader.Propose(core.OpPut, payload)
	}
}

func BenchmarkMultiRaft_Routing(b *testing.B) {
	router := multiraft.NewRouter()
	const numShards = 100

	var partitions []*multiraft.RangePartition
	for i := 0; i < numShards; i++ {
		start := []byte(fmt.Sprintf("key:%04d", i*100))
		end := []byte(fmt.Sprintf("key:%04d", (i+1)*100))
		partitions = append(partitions, &multiraft.RangePartition{
			ShardID:  core.ShardID(i + 1),
			StartKey: start,
			EndKey:   end,
			Peers:    []core.NodeID{1, 2, 3},
		})
	}
	router.SetPartitions(partitions)

	testKeys := make([][]byte, 1000)
	for i := range testKeys {
		testKeys[i] = []byte(fmt.Sprintf("key:%04d", (i*7)%9990))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target := testKeys[i%1000]
		_, _ = router.Route(target)
	}
}
