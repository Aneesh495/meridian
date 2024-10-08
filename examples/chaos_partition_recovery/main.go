package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/sim"
)

func main() {
	fmt.Println("=== Meridian Example: Network Partition Chaos and Recovery ===")
	fmt.Println()

	seed := uint64(0xCAFEBABEDEADBEEF)
	nodeCount := 3

	fmt.Printf("1. Initializing %d-node deterministic cluster simulation (seed: 0x%X)...\n", nodeCount, seed)
	cluster, err := sim.NewClusterSim(nodeCount, seed, sim.DefaultNetworkConfig, sim.ChaosPlan{})
	if err != nil {
		fmt.Printf("ERROR: Failed to create cluster simulator: %v\n", err)
		os.Exit(1)
	}
	defer cluster.Close()

	// Phase 1: Cluster Warmup and Leader Election
	fmt.Println("2. Warming up cluster and electing Raft leader...")
	for i := 0; i < 30; i++ {
		cluster.RunFor(40 * time.Millisecond)
		if cluster.Leader() != nil {
			break
		}
	}

	leader := cluster.Leader()
	if leader == nil {
		fmt.Println("ERROR: No leader elected during warmup phase")
		os.Exit(1)
	}
	fmt.Printf("   Leader successfully elected: Node %d\n", leader.ID)
	fmt.Println()

	// Phase 2: Normal Quorum Writes
	fmt.Println("3. Executing initial quorum writes...")
	ok1 := cluster.ProposeKV(1, []byte("asset:gold"), []byte("2500"))
	cluster.RunFor(20 * time.Millisecond)
	ok2 := cluster.ProposeKV(1, []byte("asset:silver"), []byte("30"))
	cluster.RunFor(20 * time.Millisecond)

	fmt.Printf("   Write asset:gold = 2500 (Success: %v)\n", ok1)
	fmt.Printf("   Write asset:silver = 30 (Success: %v)\n", ok2)

	val1, _ := cluster.ReadKV(2, []byte("asset:gold"))
	fmt.Printf("   Read asset:gold = %s\n", string(val1))
	fmt.Println()

	// Phase 3: Network Partition Injection
	fmt.Println("4. Injecting split-brain network partition: {Node 1} vs {Node 2, Node 3}...")
	cluster.Partitions().SplitPartitions(
		[]core.NodeID{1},
		[]core.NodeID{2, 3},
	)

	// Advance virtual time to trigger re-elections and heartbeats under partition
	for i := 0; i < 15; i++ {
		cluster.RunFor(40 * time.Millisecond)
	}

	currentLeader := cluster.Leader()
	if currentLeader != nil {
		fmt.Printf("   Active majority leader: Node %d (resides in majority partition {2, 3})\n", currentLeader.ID)
	}

	// Writes in majority partition
	okMaj := cluster.ProposeKV(3, []byte("asset:copper"), []byte("4"))
	cluster.RunFor(20 * time.Millisecond)
	fmt.Printf("   Proposed write to majority partition (Success: %v)\n", okMaj)
	fmt.Println()

	// Phase 4: Healing Partition
	fmt.Println("5. Healing network partition matrix and reconciling state...")
	cluster.Partitions().Heal()
	for i := 0; i < 10; i++ {
		cluster.RunFor(40 * time.Millisecond)
	}
	fmt.Println("   Cluster network fully re-converged.")
	fmt.Println()

	// Phase 5: Verification of Linearizability
	fmt.Println("6. Running Wing and Gong (WGL) Linearizability Verification over entire event history...")
	model := sim.KVModel{}
	res := sim.CheckLinearizability(model, cluster.History())

	fmt.Printf("   Linearizability Valid: %v\n", res.Linearizable)
	fmt.Printf("   Operations Checked:    %d\n", cluster.History().Len())

	if !res.Linearizable {
		fmt.Printf("FAILURE: Linearizability violation detected: %s\n", res.ViolationReason)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("SUCCESS: Cluster preserved safety invariants and linearizability through network partition!")
}
