package sim

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

func TestSim_DeterministicReplay(t *testing.T) {
	seed := uint64(0x4D4552494449414E)

	runSim := func() []uint64 {
		rng := NewRNG(seed)
		clock := NewClock(1000)
		events := NewEventQueue()
		matrix := NewPartitionMatrix()
		net := NewVirtualNetwork(clock, events, rng, matrix, ChaosNetworkConfig)

		var trace []uint64
		for i := 0; i < 50; i++ {
			val := rng.Uint64()
			trace = append(trace, val)
			_ = net.Send(core.NodeID(i%3+1), core.NodeID((i+1)%3+1), 0x01, []byte(fmt.Sprint(val)))
		}

		for events.Len() > 0 {
			ev := events.Pop()
			if ev != nil && ev.Execute != nil {
				_ = ev.Execute()
			}
		}
		return trace
	}

	trace1 := runSim()
	trace2 := runSim()

	if len(trace1) != len(trace2) {
		t.Fatalf("trace lengths differ: %d vs %d", len(trace1), len(trace2))
	}

	for i := range trace1 {
		if trace1[i] != trace2[i] {
			t.Fatalf("divergence at step %d: %x vs %x", i, trace1[i], trace2[i])
		}
	}
}

func TestSim_LinearizabilityChecker_Valid(t *testing.T) {
	hist := NewHistory()
	model := RegisterModel{InitialValue: []byte("0")}

	// Op 1: Write "10" from t=100 to t=200
	hist.Record(Operation{
		ID:        0,
		ClientID:  1,
		Type:      OpWrite,
		Input:     []byte("10"),
		Output:    []byte("10"),
		StartNano: 100,
		EndNano:   200,
		Ok:        true,
	})

	// Op 2: Read "10" from t=250 to t=300
	hist.Record(Operation{
		ID:        1,
		ClientID:  2,
		Type:      OpRead,
		Input:     nil,
		Output:    []byte("10"),
		StartNano: 250,
		EndNano:   300,
		Ok:        true,
	})

	res := CheckLinearizability(model, hist)
	if !res.Linearizable {
		t.Fatalf("expected trace to be linearizable, got violation: %s", res.ViolationReason)
	}
}

func TestSim_LinearizabilityChecker_StaleReadViolation(t *testing.T) {
	hist := NewHistory()
	model := RegisterModel{InitialValue: []byte("0")}

	// Op 1: Write "10" from t=100 to t=200
	hist.Record(Operation{
		ID:        0,
		ClientID:  1,
		Type:      OpWrite,
		Input:     []byte("10"),
		Output:    []byte("10"),
		StartNano: 100,
		EndNano:   200,
		Ok:        true,
	})

	// Op 2: Stale read of "0" strictly after Op 1 returned! (t=250 to t=300)
	hist.Record(Operation{
		ID:        1,
		ClientID:  2,
		Type:      OpRead,
		Input:     nil,
		Output:    []byte("0"),
		StartNano: 250,
		EndNano:   300,
		Ok:        true,
	})

	res := CheckLinearizability(model, hist)
	if res.Linearizable {
		t.Fatalf("expected stale read to violate linearizability, but checker declared it linearizable")
	}
}

func TestSim_VirtualDisk_TornWriteAndPowerCut(t *testing.T) {
	disk := NewVirtualDisk()
	defer disk.Close()

	initialData := []byte("committed-master-record")
	_, err := disk.WriteAt(initialData, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := disk.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Dirty unflushed write
	dirtyData := []byte("unflushed-in-memory-mutation")
	_, _ = disk.WriteAt(dirtyData, 0)

	// Power cut occurs before Sync
	disk.PowerCut()

	readBuf := make([]byte, len(initialData))
	_, err = disk.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// Verify un-synced dirty data was rolled back to durable state
	if !bytes.Equal(readBuf, initialData) {
		t.Fatalf("expected %s, got %s", initialData, readBuf)
	}
}

func TestSim_ClusterChaos_Linearizability(t *testing.T) {
	seed := uint64(0xCAFEBABEDEADBEEF)
	chaosPlan := ChaosPlan{
		PartitionInterval: 40 * time.Millisecond,
		PartitionDuration: 20 * time.Millisecond,
		CrashInterval:     60 * time.Millisecond,
		RebootDelay:       15 * time.Millisecond,
		DropRateSpike:     0.10,
	}

	cs, err := NewClusterSim(3, seed, ChaosNetworkConfig, chaosPlan)
	if err != nil {
		t.Fatalf("failed to create cluster sim: %v", err)
	}
	defer cs.Close()

	// Start with reliable network during initial cluster warmup to establish leader
	cs.network.SetConfig(DefaultNetworkConfig)
	for i := 0; i < 30; i++ {
		cs.RunFor(40 * time.Millisecond)
		if cs.Leader() != nil {
			break
		}
	}
	if cs.Leader() == nil {
		t.Fatalf("expected cluster leader to be elected during warmup")
	}

	// Unleash chaos: switch to lossy network and schedule dynamic partitions/crashes
	cs.network.SetConfig(ChaosNetworkConfig)
	cs.chaos.PopulateSchedule(cs.clock.NowNano(), cs.clock.NowNano()+(250*time.Millisecond).Nanoseconds())

	key := []byte("account:alice")

	// Concurrent client operations
	for i := 1; i <= 5; i++ {
		val := []byte(fmt.Sprintf("%d00", i))
		cs.ProposeKV(1, key, val)
		cs.RunFor(10 * time.Millisecond)
		cs.ReadKV(2, key)
		cs.RunFor(10 * time.Millisecond)
	}

	// Run remaining chaos events
	cs.RunFor(100 * time.Millisecond)

	// Verify linearizability against sequential key-value model
	kvModel := KVModel{}
	res := CheckLinearizability(kvModel, cs.History())

	if !res.Linearizable {
		t.Fatalf("cluster chaos produced non-linearizable history: %s (explored %d states)",
			res.ViolationReason, res.StatesExplored)
	}
}
