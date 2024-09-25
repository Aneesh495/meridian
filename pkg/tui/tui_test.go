package tui

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestTUI_ANSIColorize(t *testing.T) {
	colored := Colorize(FgGreen, "hello")
	if !strings.HasPrefix(colored, FgGreen) || !strings.HasSuffix(colored, Reset) {
		t.Fatalf("unexpected colorize output: %q", colored)
	}
}

func TestTUI_Screen_DoubleBuffer(t *testing.T) {
	s := NewScreen(40, 10)
	s.SetString(2, 2, "Meridian", Bold)
	s.DrawBox(0, 0, 30, 8, "Cluster", FgCyan)

	var buf bytes.Buffer
	if err := s.Flush(&buf); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatalf("expected flush output, got empty buffer")
	}

	rendered := s.RenderToString()
	if !strings.Contains(rendered, "Meridian") {
		t.Fatalf("expected rendered screen to contain 'Meridian'")
	}
	if !strings.Contains(rendered, "Cluster") {
		t.Fatalf("expected rendered screen to contain box title 'Cluster'")
	}
}

func TestTUI_Sparkline_Render(t *testing.T) {
	series := []float64{10, 20, 30, 40, 50, 60, 70, 80}
	spark := RenderSparkline(series, 8)

	if len([]rune(spark)) != 8 {
		t.Fatalf("expected 8 sparkline runes, got %d", len([]rune(spark)))
	}

	// First character should be lowest tier ' ', last character highest '█'
	runes := []rune(spark)
	if runes[0] != ' ' || runes[7] != '█' {
		t.Fatalf("expected gradient from   to █, got %q", spark)
	}

	// Series buffer test
	sb := NewSparklineSeries(5)
	for i := 1.0; i <= 10.0; i++ {
		sb.Add(i)
	}
	if sb.Last() != 10.0 {
		t.Fatalf("expected last sample 10.0, got %f", sb.Last())
	}
}

func TestTUI_Dashboard_Layout(t *testing.T) {
	s := NewScreen(80, 24)

	tps := NewSparklineSeries(20)
	for i := 0; i < 20; i++ {
		tps.Add(float64(1000 + i*50))
	}

	lat := NewSparklineSeries(20)
	for i := 0; i < 20; i++ {
		lat.Add(float64(2.5 - float64(i)*0.05))
	}

	state := &ClusterState{
		NodeID:         1,
		Addr:           "127.0.0.1:8000",
		Uptime:         120 * time.Second,
		HLCWallTime:    time.Now().UnixNano(),
		HLCLogical:     42,
		AliveMembers:   3,
		SuspectMembers: 0,
		TotalMembers:   3,
		Shards: []RaftShardStatus{
			{ShardID: 1, Role: "LEADER", LeaderID: 1, Term: 2, CommitIndex: 150, LeaseValid: true},
			{ShardID: 2, Role: "FOLLOWER", LeaderID: 2, Term: 1, CommitIndex: 85, LeaseValid: false},
		},
		ActiveTxns:    4,
		ActiveLocks:   12,
		WoundedTxns:   1,
		WALSegments:   6,
		CacheHitRatio: 0.945,
		TPSSeries:     tps,
		LatencySeries: lat,
	}

	RenderDashboard(s, state)

	rendered := s.RenderToString()
	if !strings.Contains(rendered, "MERIDIAN") {
		t.Fatalf("expected dashboard banner")
	}
	if !strings.Contains(rendered, "LEADER") {
		t.Fatalf("expected shard status in dashboard")
	}
	if !strings.Contains(rendered, "2PC & Deadlock Engine") {
		t.Fatalf("expected 2PC engine box")
	}
}

func TestTUI_REPL_Help(t *testing.T) {
	var inBuf bytes.Buffer
	var outBuf bytes.Buffer

	inBuf.WriteString("help\nexit\n")
	repl := NewREPL(nil, &inBuf, &outBuf)

	if err := repl.Run(); err != nil {
		t.Fatalf("repl run failed: %v", err)
	}

	out := outBuf.String()
	if !strings.Contains(out, "Available Commands") {
		t.Fatalf("expected help output in REPL")
	}
	if !strings.Contains(out, "Bye!") {
		t.Fatalf("expected clean exit message")
	}
}
