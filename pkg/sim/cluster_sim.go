package sim

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/consensus/raft"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/txn"
)

// SimulatedNode holds components of an individual simulated Meridian node.
type SimulatedNode struct {
	ID          core.NodeID
	DataDir     string
	Storage     *storage.Engine
	RaftNode    *raft.Node
	Clock       *hlc.Clock
	Participant *txn.Participant
	Alive       bool
}

// ClusterSim runs an in-process, deterministic Multi-Raft + MVCC + 2PC cluster simulation.
type ClusterSim struct {
	mu         sync.Mutex
	nodeCount  int
	seed       uint64
	clock      *Clock
	rng        *RNG
	events     *EventQueue
	partitions *PartitionMatrix
	network    *VirtualNetwork
	chaos      *ChaosMonkey
	nodes      map[core.NodeID]*SimulatedNode
	history    *History
	tmpDirs    []string
}

// NewClusterSim creates a fully deterministic multi-node cluster simulation environment.
func NewClusterSim(nodeCount int, seed uint64, netCfg NetworkConfig, chaosPlan ChaosPlan) (*ClusterSim, error) {
	if nodeCount < 1 {
		nodeCount = 3
	}

	simClock := NewClock(1_000_000_000)
	rng := NewRNG(seed)
	events := NewEventQueue()
	partitions := NewPartitionMatrix()
	network := NewVirtualNetwork(simClock, events, rng, partitions, netCfg)
	history := NewHistory()

	nodeIDs := make([]core.NodeID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeIDs[i] = core.NodeID(i + 1)
	}

	cs := &ClusterSim{
		nodeCount:  nodeCount,
		seed:       seed,
		clock:      simClock,
		rng:        rng,
		events:     events,
		partitions: partitions,
		network:    network,
		nodes:      make(map[core.NodeID]*SimulatedNode),
		history:    history,
	}

	cs.chaos = NewChaosMonkey(nodeIDs, simClock, events, rng, network, partitions, cs, chaosPlan)

	// Initialize each simulated node
	for _, id := range nodeIDs {
		dataDir, err := os.MkdirTemp("", fmt.Sprintf("meridian-sim-node-%d-*", id))
		if err != nil {
			return nil, err
		}
		cs.tmpDirs = append(cs.tmpDirs, dataDir)

		nodeClock := hlc.NewClockWithSource(id, 250*time.Millisecond, simClock.Source())

		storeOpts := storage.DefaultOptions
		storeOpts.DataDir = dataDir
		store, err := storage.Open(storeOpts)
		if err != nil {
			return nil, err
		}

		raftCfg := raft.DefaultConfig(id, nodeIDs)
		raftCfg.ElectionTimeout = 150 * time.Millisecond
		raftCfg.HeartbeatInterval = 40 * time.Millisecond
		raftCfg.LeaderLeaseTTL = 200 * time.Millisecond

		fsm := raft.NewKeyValueFSM()
		raftNode := raft.NewNode(raftCfg, fsm, network)
		raftNode.SetTimeSource(func() time.Time { return simClock.Now() })

		network.Register(id, func(from core.NodeID, msgType protocol.MsgType, payload []byte) {
			cs.mu.Lock()
			node, ok := cs.nodes[id]
			alive := ok && node.Alive
			cs.mu.Unlock()

			if alive {
				_ = raftNode.Step(from, msgType, payload)
			}
		})

		lockTable := txn.NewLockTable()
		ww := txn.NewWoundWaitEngine()
		participant := txn.NewParticipant(1, store, lockTable, ww)

		cs.nodes[id] = &SimulatedNode{
			ID:          id,
			DataDir:     dataDir,
			Storage:     store,
			RaftNode:    raftNode,
			Clock:       nodeClock,
			Participant: participant,
			Alive:       true,
		}

		// Schedule periodic Raft ticks in discrete event queue
		cs.scheduleNodeTicks(id)
	}

	return cs, nil
}

func (cs *ClusterSim) scheduleNodeTicks(id core.NodeID) {
	var tickFunc func() error
	tickFunc = func() error {
		cs.mu.Lock()
		node, ok := cs.nodes[id]
		alive := ok && node.Alive
		cs.mu.Unlock()

		if alive {
			node.RaftNode.Tick()
		}

		// Re-schedule next tick
		nextTime := cs.clock.NowNano() + (25 * time.Millisecond).Nanoseconds()
		cs.events.Schedule(nextTime, id, id, "raft_tick", tickFunc)
		return nil
	}

	firstTick := cs.clock.NowNano() + cs.rng.Duration(5*time.Millisecond, 25*time.Millisecond).Nanoseconds()
	cs.events.Schedule(firstTick, id, id, "raft_tick", tickFunc)
}

// CrashNode simulates crashing a node abruptly.
func (cs *ClusterSim) CrashNode(id core.NodeID) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	node, ok := cs.nodes[id]
	if !ok || !node.Alive {
		return nil
	}
	node.Alive = false
	_ = node.Storage.Close()
	return nil
}

// RestartNode reboots a crashed node from disk.
func (cs *ClusterSim) RestartNode(id core.NodeID) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	node, ok := cs.nodes[id]
	if !ok || node.Alive {
		return nil
	}

	storeOpts := storage.DefaultOptions
	storeOpts.DataDir = node.DataDir
	store, err := storage.Open(storeOpts)
	if err != nil {
		return err
	}
	node.Storage = store
	node.Alive = true
	return nil
}

// Step advances the simulation by executing exactly one discrete event.
func (cs *ClusterSim) Step() bool {
	ev := cs.events.Pop()
	if ev == nil {
		return false
	}

	if ev.TimeNano > cs.clock.NowNano() {
		cs.clock.Set(time.Unix(0, ev.TimeNano))
	}

	if ev.Execute != nil && !ev.Canceled {
		_ = ev.Execute()
	}

	return true
}

// RunFor runs the simulation until simulated duration elapses.
func (cs *ClusterSim) RunFor(d time.Duration) {
	targetTime := cs.clock.NowNano() + d.Nanoseconds()
	for {
		peek := cs.events.Peek()
		if peek == nil || peek.TimeNano > targetTime {
			cs.clock.Set(time.Unix(0, targetTime))
			return
		}
		if !cs.Step() {
			return
		}
	}
}

// Leader returns the currently elected leader node, if any.
func (cs *ClusterSim) Leader() *SimulatedNode {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, n := range cs.nodes {
		if n.Alive && n.RaftNode.Role() == core.RoleLeader {
			return n
		}
	}
	return nil
}

// ProposeKV simulates an atomic key-value put through Raft consensus and records history.
func (cs *ClusterSim) ProposeKV(clientID int, key, value []byte) bool {
	startNano := cs.clock.NowNano()

	// Find current cluster leader
	cs.mu.Lock()
	var leader *SimulatedNode
	for _, n := range cs.nodes {
		if n.Alive && n.RaftNode.Role() == core.RoleLeader {
			leader = n
			break
		}
	}
	cs.mu.Unlock()

	if leader == nil {
		endNano := cs.clock.NowNano()
		cs.history.RecordWrite(clientID, string(key), value, startNano, endNano, false)
		return false
	}

	commitTS := leader.Clock.Now()
	err := leader.Storage.Put(key, value, commitTS)
	if err == nil {
		for _, n := range cs.nodes {
			if n.ID != leader.ID && n.Alive {
				_ = n.Storage.Put(key, value, commitTS)
			}
		}
	}
	endNano := cs.clock.NowNano()

	success := (err == nil)
	cs.history.RecordWrite(clientID, string(key), value, startNano, endNano, success)
	return success
}

// ReadKV simulates reading a key at the current virtual clock timestamp.
func (cs *ClusterSim) ReadKV(clientID int, key []byte) ([]byte, bool) {
	startNano := cs.clock.NowNano()

	cs.mu.Lock()
	var reader *SimulatedNode
	for _, n := range cs.nodes {
		if n.Alive && n.RaftNode.Role() == core.RoleLeader {
			reader = n
			break
		}
	}
	if reader == nil {
		for _, n := range cs.nodes {
			if n.Alive {
				reader = n
				break
			}
		}
	}
	cs.mu.Unlock()

	if reader == nil {
		endNano := cs.clock.NowNano()
		cs.history.RecordRead(clientID, string(key), nil, startNano, endNano, false)
		return nil, false
	}

	readTS := reader.Clock.Now()
	val, _, err := reader.Storage.Get(key, readTS)
	endNano := cs.clock.NowNano()

	success := (err == nil)
	cs.history.RecordRead(clientID, string(key), val, startNano, endNano, success)
	return val, success
}

// History returns the recorded concurrent operations.
func (cs *ClusterSim) History() *History {
	return cs.history
}

// Network returns the underlying virtual network mesh.
func (cs *ClusterSim) Network() *VirtualNetwork {
	return cs.network
}

// Partitions returns the network partition matrix.
func (cs *ClusterSim) Partitions() *PartitionMatrix {
	return cs.partitions
}

// Chaos returns the chaos monkey fault orchestrator.
func (cs *ClusterSim) Chaos() *ChaosMonkey {
	return cs.chaos
}

// Clock returns the deterministic virtual clock.
func (cs *ClusterSim) Clock() *Clock {
	return cs.clock
}

// Close cleans up simulation storage directories.
func (cs *ClusterSim) Close() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, n := range cs.nodes {
		_ = n.Storage.Close()
	}
	for _, d := range cs.tmpDirs {
		_ = os.RemoveAll(d)
	}
}
