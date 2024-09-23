package tui

import (
	"fmt"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// RaftShardStatus summarizes the state of a single Multi-Raft shard.
type RaftShardStatus struct {
	ShardID     core.ShardID
	Role        string
	LeaderID    core.NodeID
	Term        uint64
	CommitIndex uint64
	LeaseValid  bool
}

// ClusterState holds live telemetry metrics for rendering the TUI dashboard.
type ClusterState struct {
	NodeID          core.NodeID
	Addr            string
	Uptime          time.Duration
	HLCWallTime     int64
	HLCLogical      uint16
	AliveMembers    int
	SuspectMembers  int
	TotalMembers    int
	Shards          []RaftShardStatus
	ActiveTxns      int
	ActiveLocks     int
	WoundedTxns     int
	WALSegments     int
	CacheHitRatio   float64
	TPSSeries       *SparklineSeries
	LatencySeries   *SparklineSeries
}

// RenderDashboard draws the comprehensive Meridian cluster dashboard onto the screen.
func RenderDashboard(s *Screen, state *ClusterState) {
	s.Clear()
	w, h := s.Size()

	// 1. Header Banner
	headerStyle := Bold + FgBrightCyan
	s.SetString(2, 1, "MERIDIAN // DISTRIBUTED CONSENSUS & DURABLE EXECUTION FABRIC", headerStyle)

	metaStyle := FgBrightWhite
	nodeInfo := fmt.Sprintf("Node: %d @ %s | Uptime: %s | Clock: %d:%d",
		state.NodeID, state.Addr, state.Uptime.Round(time.Second), state.HLCWallTime, state.HLCLogical)
	s.SetString(2, 2, nodeInfo, metaStyle)

	// Horizontal rule
	for x := 1; x < w-1; x++ {
		s.SetCell(x, 3, '─', FgBrightBlack)
	}

	// 2. Multi-Raft Shards Box (Left Column)
	leftW := w / 2
	s.DrawBox(1, 4, leftW, 11, "Multi-Raft Shard Replicas", Bold+FgBrightBlue)

	colHdr := fmt.Sprintf("%-6s %-10s %-8s %-8s %-10s %-8s", "SHARD", "ROLE", "LEADER", "TERM", "COMMIT", "LEASE")
	s.SetString(3, 5, colHdr, Bold+FgYellow)

	for i, sh := range state.Shards {
		if 6+i >= 14 {
			break
		}
		leaseStr := "EXPIRED"
		leaseStyle := FgRed
		if sh.LeaseValid {
			leaseStr = "ACTIVE"
			leaseStyle = FgGreen
		}

		row := fmt.Sprintf("#%-5d %-10s Node %-3d %-8d %-10d %s",
			sh.ShardID, sh.Role, sh.LeaderID, sh.Term, sh.CommitIndex, Colorize(leaseStyle, leaseStr))
		s.SetString(3, 6+i, row, FgWhite)
	}

	// 3. Transactions & Concurrency Engine (Right Column)
	rightX := leftW + 1
	rightW := w - rightX - 1
	s.DrawBox(rightX, 4, rightW, 6, "2PC & Deadlock Engine", Bold+FgBrightMagenta)

	s.SetString(rightX+2, 5, fmt.Sprintf("Active Transactions: %s", Colorize(FgBrightYellow, fmt.Sprintf("%d", state.ActiveTxns))), FgWhite)
	s.SetString(rightX+2, 6, fmt.Sprintf("Lock Table Depth:    %s", Colorize(FgBrightCyan, fmt.Sprintf("%d locks", state.ActiveLocks))), FgWhite)
	s.SetString(rightX+2, 7, fmt.Sprintf("Wound-Wait Aborts:   %s", Colorize(FgBrightRed, fmt.Sprintf("%d txns", state.WoundedTxns))), FgWhite)
	s.SetString(rightX+2, 8, fmt.Sprintf("Storage WAL Segments: %d", state.WALSegments), FgWhite)

	// 4. SWIM Gossip Cluster Membership (Right Column lower)
	s.DrawBox(rightX, 10, rightW, 5, "SWIM Failure Detector", Bold+FgBrightGreen)
	s.SetString(rightX+2, 11, fmt.Sprintf("Cluster Membership:  %d nodes total", state.TotalMembers), FgWhite)
	s.SetString(rightX+2, 12, fmt.Sprintf("Healthy Replicas:    %s | Suspect: %s",
		Colorize(FgGreen, fmt.Sprintf("%d alive", state.AliveMembers)),
		Colorize(FgRed, fmt.Sprintf("%d suspect", state.SuspectMembers))), FgWhite)
	s.SetString(rightX+2, 13, fmt.Sprintf("Engine Cache Hit:    %.1f%%", state.CacheHitRatio*100), FgWhite)

	// 5. Performance Metrics & Sparklines (Bottom Box)
	bottomY := 15
	bottomH := h - bottomY - 1
	if bottomH >= 6 {
		s.DrawBox(1, bottomY, w-2, bottomH, "Throughput & p99 Commit Latency", Bold+FgBrightYellow)

		sparkW := (w - 12) / 2
		if sparkW > 5 && state.TPSSeries != nil && state.LatencySeries != nil {
			tpsSpark := state.TPSSeries.Render(sparkW)
			latSpark := state.LatencySeries.Render(sparkW)

			s.SetString(3, bottomY+1, fmt.Sprintf("TPS: %s  [Current: %.0f ops/s]", Colorize(FgBrightGreen, tpsSpark), state.TPSSeries.Last()), FgWhite)
			s.SetString(3, bottomY+2, fmt.Sprintf("p99: %s  [Current: %.2f ms]", Colorize(FgBrightCyan, latSpark), state.LatencySeries.Last()), FgWhite)
		}
	}

	// Footer instructions
	s.SetString(2, h-1, "Press [q] or [Ctrl+C] to exit monitor | [space] force election | [r] refresh", Dim+FgBrightBlack)
}
