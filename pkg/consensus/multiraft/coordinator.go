package multiraft

import (
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/consensus/raft"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// Coordinator manages multiple independent Raft consensus groups within a single node.
type Coordinator struct {
	mu        sync.RWMutex
	nodeID    core.NodeID
	groups    map[core.ShardID]*raft.Node
	router    *Router
	transport raft.Transport
	stopChan  chan struct{}
	stopped   bool
}

// NewCoordinator creates a new Multi-Raft coordinator.
func NewCoordinator(nodeID core.NodeID, router *Router, transport raft.Transport) *Coordinator {
	return &Coordinator{
		nodeID:    nodeID,
		groups:    make(map[core.ShardID]*raft.Node),
		router:    router,
		transport: transport,
		stopChan:  make(chan struct{}),
	}
}

// AddGroup registers an active Raft group for a shard.
func (c *Coordinator) AddGroup(shardID core.ShardID, node *raft.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groups[shardID] = node
}

// GetGroup retrieves the Raft node for a shard.
func (c *Coordinator) GetGroup(shardID core.ShardID) (*raft.Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, ok := c.groups[shardID]
	return node, ok
}

// Start launches the multiplexed tick scheduler across all hosted groups.
func (c *Coordinator) Start() {
	go c.tickLoop()
}

// Stop terminates the coordinator and stops all hosted Raft groups.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		c.stopped = true
		close(c.stopChan)
		for _, g := range c.groups {
			g.Stop()
		}
	}
}

func (c *Coordinator) tickLoop() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.RLock()
			for _, g := range c.groups {
				g.Tick()
			}
			c.mu.RUnlock()
		case <-c.stopChan:
			return
		}
	}
}

// Propose routes a mutation directly to the responsible shard's Raft group.
func (c *Coordinator) Propose(key []byte, op core.OpType, data []byte) (core.ShardID, core.Index, error) {
	shardID, err := c.router.Route(key)
	if err != nil {
		return 0, 0, err
	}

	c.mu.RLock()
	group, ok := c.groups[shardID]
	c.mu.RUnlock()

	if !ok {
		return shardID, 0, core.ErrPartitionUnreachable
	}

	idx, err := group.Propose(op, data)
	return shardID, idx, err
}

// Step routes an incoming Raft RPC to the targeted shard group.
func (c *Coordinator) Step(shardID core.ShardID, from core.NodeID, msgType protocol.MsgType, payload []byte) error {
	c.mu.RLock()
	group, ok := c.groups[shardID]
	c.mu.RUnlock()

	if !ok {
		return core.ErrPartitionUnreachable
	}

	return group.Step(from, msgType, payload)
}
