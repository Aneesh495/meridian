package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/cluster"
	"github.com/Aneesh495/meridian/pkg/consensus/multiraft"
	"github.com/Aneesh495/meridian/pkg/consensus/raft"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/txn"
	"github.com/Aneesh495/meridian/pkg/workflow"
)

// MeridianNode is a fully integrated distributed state machine node.
type MeridianNode struct {
	mu          sync.RWMutex
	cfg         Config
	clock       *hlc.Clock
	storage     *storage.Engine
	multiraft   *multiraft.Coordinator
	lockTable   *txn.LockTable
	woundWait   *txn.WoundWaitEngine
	participant *txn.Participant
	txnCoord    *txn.Coordinator
	swim        *cluster.SWIM
	timingWheel *workflow.TimingWheel
	handler     *Handler
	server      *Server
	running     bool
}

type localLookup struct {
	participant *txn.Participant
}

func (l *localLookup) GetParticipant(key []byte) (*txn.Participant, error) {
	return l.participant, nil
}

// NewNode instantiates a full MeridianNode from configuration.
func NewNode(cfg Config) (*MeridianNode, error) {
	clock := hlc.NewClock(cfg.NodeID, 250*time.Millisecond)

	storeOpts := storage.DefaultOptions
	storeOpts.DataDir = cfg.DataDir
	storeOpts.CacheSizeBytes = cfg.CacheSizeBytes

	store, err := storage.Open(storeOpts)
	if err != nil {
		return nil, fmt.Errorf("meridian/node: failed to open storage: %w", err)
	}

	router := multiraft.NewRouter()
	transport := raft.NewMemoryTransport()
	mrCoord := multiraft.NewCoordinator(cfg.NodeID, router, transport)

	lockTable := txn.NewLockTable()
	ww := txn.NewWoundWaitEngine()
	participant := txn.NewParticipant(1, store, lockTable, ww)

	lookup := &localLookup{participant: participant}
	txnCoord := txn.NewCoordinator(clock, lookup, ww)

	swimMember := &cluster.Member{
		ID:     cfg.NodeID,
		Addr:   cfg.ListenAddr,
		Status: cluster.StatusAlive,
	}
	swim := cluster.NewSWIM(swimMember, func(to core.NodeID, msgType protocol.MsgType, payload []byte) error {
		return nil
	})

	tw := workflow.NewTimingWheel(10, 64, time.Now().UnixMilli())

	handler := NewHandler(cfg.NodeID, store, mrCoord, txnCoord, participant)
	srv := NewServer(cfg, handler)

	return &MeridianNode{
		cfg:         cfg,
		clock:       clock,
		storage:     store,
		multiraft:   mrCoord,
		lockTable:   lockTable,
		woundWait:   ww,
		participant: participant,
		txnCoord:    txnCoord,
		swim:        swim,
		timingWheel: tw,
		handler:     handler,
		server:      srv,
	}, nil
}

// Start boots all subsystems and begins accepting client connections.
func (n *MeridianNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.running {
		return nil
	}

	n.multiraft.Start()
	n.swim.Start()

	if err := n.server.Start(); err != nil {
		return err
	}

	n.running = true
	return nil
}

// Stop gracefully shuts down all node subsystems.
func (n *MeridianNode) Stop() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if !n.running {
		return nil
	}

	_ = n.server.Stop()
	n.swim.Stop()
	n.multiraft.Stop()
	_ = n.storage.Close()

	n.running = false
	return nil
}

// NodeID returns the identifier of this cluster node.
func (n *MeridianNode) NodeID() core.NodeID {
	return n.cfg.NodeID
}

// Addr returns the bound TCP address.
func (n *MeridianNode) Addr() string {
	if n.server != nil && n.server.Addr() != nil {
		return n.server.Addr().String()
	}
	return n.cfg.ListenAddr
}

// Storage returns the underlying storage engine.
func (n *MeridianNode) Storage() *storage.Engine {
	return n.storage
}

// TxnCoordinator returns the distributed 2PC coordinator.
func (n *MeridianNode) TxnCoordinator() *txn.Coordinator {
	return n.txnCoord
}

// Clock returns the Hybrid Logical Clock.
func (n *MeridianNode) Clock() *hlc.Clock {
	return n.clock
}
