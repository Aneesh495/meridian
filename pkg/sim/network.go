package sim

import (
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// NetworkConfig tunes packet drop, latency, duplication, and corruption probabilities.
type NetworkConfig struct {
	MinLatency    time.Duration
	MaxLatency    time.Duration
	DropRate      float64 // 0.0 to 1.0
	DuplicateRate float64 // 0.0 to 1.0
	CorruptRate   float64 // 0.0 to 1.0
}

// DefaultNetworkConfig provides realistic data center network properties.
var DefaultNetworkConfig = NetworkConfig{
	MinLatency:    1 * time.Millisecond,
	MaxLatency:    5 * time.Millisecond,
	DropRate:      0.0,
	DuplicateRate: 0.0,
	CorruptRate:   0.0,
}

// ChaosNetworkConfig injects high latency and network faults.
var ChaosNetworkConfig = NetworkConfig{
	MinLatency:    5 * time.Millisecond,
	MaxLatency:    50 * time.Millisecond,
	DropRate:      0.05, // 5% packet drop
	DuplicateRate: 0.02, // 2% duplication
	CorruptRate:   0.005, // 0.5% bit flips
}

// PacketHandler is the callback invoked when a packet arrives at the destination.
type PacketHandler func(from core.NodeID, msgType protocol.MsgType, payload []byte)

// VirtualNetwork models a distributed asynchronous network mesh over simulated time.
type VirtualNetwork struct {
	mu         sync.RWMutex
	clock      *Clock
	events     *EventQueue
	rng        *RNG
	partitions *PartitionMatrix
	config     NetworkConfig
	handlers   map[core.NodeID]PacketHandler
	sentCount  uint64
	dropCount  uint64
}

// NewVirtualNetwork creates a fault-injected virtual network mesh.
func NewVirtualNetwork(clock *Clock, events *EventQueue, rng *RNG, partitions *PartitionMatrix, cfg NetworkConfig) *VirtualNetwork {
	return &VirtualNetwork{
		clock:      clock,
		events:     events,
		rng:        rng,
		partitions: partitions,
		config:     cfg,
		handlers:   make(map[core.NodeID]PacketHandler),
	}
}

// Register connects a node's message handler to the virtual network.
func (vn *VirtualNetwork) Register(nodeID core.NodeID, handler PacketHandler) {
	vn.mu.Lock()
	defer vn.mu.Unlock()
	vn.handlers[nodeID] = handler
}

// SetConfig updates network fault injection settings dynamically.
func (vn *VirtualNetwork) SetConfig(cfg NetworkConfig) {
	vn.mu.Lock()
	defer vn.mu.Unlock()
	vn.config = cfg
}

// Send routes a message frame from source to destination according to network simulation rules.
func (vn *VirtualNetwork) Send(from, to core.NodeID, msgType protocol.MsgType, payload []byte) error {
	vn.mu.Lock()
	vn.sentCount++
	cfg := vn.config
	vn.mu.Unlock()

	// 1. Check network partition matrix
	if !vn.partitions.CanCommunicate(from, to) {
		vn.mu.Lock()
		vn.dropCount++
		vn.mu.Unlock()
		return nil // Dropped by partition
	}

	// 2. Check randomized packet drop
	if cfg.DropRate > 0 && vn.rng.Bool(cfg.DropRate) {
		vn.mu.Lock()
		vn.dropCount++
		vn.mu.Unlock()
		return nil // Dropped by simulated loss
	}

	// Copy payload to model separate wire buffers
	wirePayload := make([]byte, len(payload))
	copy(wirePayload, payload)

	// 3. Check randomized bit corruption
	if cfg.CorruptRate > 0 && vn.rng.Bool(cfg.CorruptRate) && len(wirePayload) > 0 {
		idx := vn.rng.Intn(len(wirePayload))
		wirePayload[idx] ^= 0xFF // Flip bits
	}

	deliverPacket := func() error {
		vn.mu.RLock()
		handler, ok := vn.handlers[to]
		vn.mu.RUnlock()

		if ok && handler != nil {
			handler(from, msgType, wirePayload)
		}
		return nil
	}

	// 4. Compute random link latency
	latency := vn.rng.Duration(cfg.MinLatency, cfg.MaxLatency)
	currentTime := vn.clock.NowNano()
	arrivalTime := currentTime + latency.Nanoseconds()

	vn.events.Schedule(arrivalTime, from, to, "packet_deliver", deliverPacket)

	// 5. Check randomized packet duplication
	if cfg.DuplicateRate > 0 && vn.rng.Bool(cfg.DuplicateRate) {
		dupLatency := latency + vn.rng.Duration(1*time.Millisecond, 10*time.Millisecond)
		vn.events.Schedule(currentTime+dupLatency.Nanoseconds(), from, to, "packet_duplicate", deliverPacket)
	}

	return nil
}

// Broadcast sends the message to all peer nodes.
func (vn *VirtualNetwork) Broadcast(from core.NodeID, peers []core.NodeID, msgType protocol.MsgType, payload []byte) error {
	for _, peer := range peers {
		if peer != from {
			_ = vn.Send(from, peer, msgType, payload)
		}
	}
	return nil
}

// Stats returns the cumulative transmitted and dropped packet counts.
func (vn *VirtualNetwork) Stats() (sent uint64, dropped uint64) {
	vn.mu.RLock()
	defer vn.mu.RUnlock()
	return vn.sentCount, vn.dropCount
}
