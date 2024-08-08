package server

import (
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Config configures a Meridian server node instance.
type Config struct {
	NodeID            core.NodeID
	ListenAddr        string
	DataDir           string
	Peers             map[core.NodeID]string // NodeID -> Addr
	ShardCount        int
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	CacheSizeBytes    int
}

// DefaultConfig returns standard server node defaults.
func DefaultConfig(nodeID core.NodeID, listenAddr string) Config {
	return Config{
		NodeID:            nodeID,
		ListenAddr:        listenAddr,
		DataDir:           ".data/meridian",
		Peers:             make(map[core.NodeID]string),
		ShardCount:        8,
		ElectionTimeout:   250 * time.Millisecond,
		HeartbeatInterval: 80 * time.Millisecond,
		CacheSizeBytes:    32 * 1024 * 1024,
	}
}
