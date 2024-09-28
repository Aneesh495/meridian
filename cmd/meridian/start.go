package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/server"
	"github.com/Aneesh495/meridian/pkg/tui"
)

func runStart(args []string) error {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	nodeID := fs.Int("id", 1, "Unique cluster node identifier (1-based integer)")
	listenAddr := fs.String("listen", "127.0.0.1:8000", "TCP address to bind and listen for cluster/client RPCs")
	dataDir := fs.String("data", ".data/node1", "Filesystem path for durable WAL and snapshot storage")
	peersFlag := fs.String("peers", "", "Comma-separated peer mappings in id=addr format (e.g. '2=127.0.0.1:8001,3=127.0.0.1:8002')")
	shardCount := fs.Int("shards", 8, "Number of initial Multi-Raft range partitions")

	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := server.DefaultConfig(core.NodeID(*nodeID), *listenAddr)
	cfg.DataDir = *dataDir
	cfg.ShardCount = *shardCount

	if *peersFlag != "" {
		peerPairs := strings.Split(*peersFlag, ",")
		for _, pair := range peerPairs {
			parts := strings.Split(strings.TrimSpace(pair), "=")
			if len(parts) == 2 {
				pID, err := strconv.Atoi(parts[0])
				if err == nil {
					cfg.Peers[core.NodeID(pID)] = parts[1]
				}
			}
		}
	}

	fmt.Printf("%s Starting Meridian node #%d on %s...\n",
		tui.Colorize(tui.Bold+tui.FgBrightCyan, ">>"), cfg.NodeID, cfg.ListenAddr)
	fmt.Printf("   Data directory: %s | Shards: %d | Known peers: %d\n",
		cfg.DataDir, cfg.ShardCount, len(cfg.Peers))

	node, err := server.NewNode(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize node: %w", err)
	}

	if err := node.Start(); err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	}

	fmt.Printf("%s Meridian node #%d active and ready for client connections.\n",
		tui.Colorize(tui.Bold+tui.FgBrightGreen, ">>"), cfg.NodeID)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Printf("\n%s Received signal %s. Initiating graceful shutdown...\n",
		tui.Colorize(tui.Bold+tui.FgBrightYellow, ">>"), sig)

	shutdownStart := time.Now()
	if err := node.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Error during shutdown: %v\n", err)
	} else {
		fmt.Printf("%s Graceful shutdown complete in %v. Goodbye!\n",
			tui.Colorize(tui.Bold+tui.FgBrightGreen, ">>"), time.Since(shutdownStart).Round(time.Millisecond))
	}

	return nil
}
