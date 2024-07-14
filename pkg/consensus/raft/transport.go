package raft

import (
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// Transport defines the communication interface for sending Raft RPC frames to peer nodes.
type Transport interface {
	Send(to core.NodeID, msgType protocol.MsgType, payload []byte) error
	Broadcast(peers []core.NodeID, msgType protocol.MsgType, payload []byte) error
}

// MemoryTransport provides zero-copy in-memory message routing between simulated Raft nodes.
type MemoryTransport struct {
	mu       sync.RWMutex
	handlers map[core.NodeID]func(from core.NodeID, msgType protocol.MsgType, payload []byte)
}

// NewMemoryTransport creates a new in-memory transport mesh.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		handlers: make(map[core.NodeID]func(from core.NodeID, msgType protocol.MsgType, payload []byte)),
	}
}

// Register connects a node's message handler to the in-memory mesh.
func (m *MemoryTransport) Register(nodeID core.NodeID, handler func(from core.NodeID, msgType protocol.MsgType, payload []byte)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[nodeID] = handler
}

// Send routes a message directly to the target node's handler.
func (m *MemoryTransport) Send(to core.NodeID, msgType protocol.MsgType, payload []byte) error {
	m.mu.RLock()
	handler, ok := m.handlers[to]
	m.mu.RUnlock()

	if !ok {
		return core.ErrPartitionUnreachable
	}

	go handler(0, msgType, payload)
	return nil
}

// Broadcast sends a message to all specified peer nodes.
func (m *MemoryTransport) Broadcast(peers []core.NodeID, msgType protocol.MsgType, payload []byte) error {
	for _, p := range peers {
		_ = m.Send(p, msgType, payload)
	}
	return nil
}
