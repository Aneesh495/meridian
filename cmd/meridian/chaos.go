package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/Aneesh495/meridian/pkg/sim"
	"github.com/Aneesh495/meridian/pkg/tui"
)

func runChaos(args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	nodeCount := fs.Int("nodes", 3, "Number of simulated cluster nodes")
	seed := fs.Uint64("seed", 0x4D4552494449414E, "64-bit deterministic PRNG seed for reproducible chaos")
	ops := fs.Int("ops", 20, "Number of concurrent client operations to simulate")
	dropRate := fs.Float64("droprate", 0.05, "Packet loss rate (0.0 to 1.0)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	netCfg := sim.DefaultNetworkConfig
	netCfg.DropRate = *dropRate

	chaosPlan := sim.DefaultChaosPlan

	fmt.Printf("%s Initializing Deterministic Simulation Testing (DST)...\n",
		tui.Colorize(tui.Bold+tui.FgBrightCyan, ">>"))
	fmt.Printf("   Nodes: %d | Seed: 0x%X | Operations: %d | Packet Drop: %.1f%%\n\n",
		*nodeCount, *seed, *ops, *dropRate*100)

	cs, err := sim.NewClusterSim(*nodeCount, *seed, sim.DefaultNetworkConfig, chaosPlan)
	if err != nil {
		return fmt.Errorf("failed to initialize cluster simulation: %w", err)
	}
	defer cs.Close()

	// Warmup until leader elected
	for i := 0; i < 30; i++ {
		cs.RunFor(40 * time.Millisecond)
		if cs.Leader() != nil {
			break
		}
	}

	if cs.Leader() == nil {
		return fmt.Errorf("cluster failed to elect leader during initial warmup")
	}

	fmt.Printf("%s Cluster stable. Leader elected: Node #%d\n",
		tui.Colorize(tui.Bold+tui.FgBrightGreen, ">>"), cs.Leader().ID)
	fmt.Printf("%s Injecting dynamic network partitions and packet drops...\n",
		tui.Colorize(tui.Bold+tui.FgBrightYellow, ">>"))

	cs.Network().SetConfig(netCfg)
	cs.Chaos().PopulateSchedule(cs.Clock().NowNano(), cs.Clock().NowNano()+(250*time.Millisecond).Nanoseconds())

	// Drive concurrent operations
	key := []byte("account:alice:balance")
	for i := 1; i <= *ops; i++ {
		val := []byte(fmt.Sprintf("%d00", i))
		cs.ProposeKV(1, key, val)
		cs.RunFor(10 * time.Millisecond)
		cs.ReadKV(2, key)
		cs.RunFor(10 * time.Millisecond)
	}

	fmt.Printf("%s Completed %d simulated operations across %d events.\n",
		tui.Colorize(tui.Bold+tui.FgBrightCyan, ">>"), cs.History().Len(), *ops*2)
	fmt.Printf("%s Executing Wing & Gong (WGL) linearizability verifier...\n",
		tui.Colorize(tui.Bold+tui.FgBrightCyan, ">>"))

	checkStart := time.Now()
	kvModel := sim.KVModel{}
	res := sim.CheckLinearizability(kvModel, cs.History())
	checkDuration := time.Since(checkStart)

	if !res.Linearizable {
		fmt.Printf("\n%s LINEARIZABILITY VIOLATION DETECTED!\n",
			tui.Colorize(tui.Bold+tui.FgRed, "FAIL:"))
		fmt.Printf("   Reason: %s\n", res.ViolationReason)
		fmt.Printf("   States explored: %d | Time: %v\n", res.StatesExplored, checkDuration)
		fmt.Printf("   To reproduce this exact failure, run: meridian chaos -seed=0x%X\n", *seed)
		return fmt.Errorf("linearizability violation")
	}

	fmt.Printf("\n%s STRICT LINEARIZABILITY VERIFIED (PASS)\n",
		tui.Colorize(tui.Bold+tui.FgBrightGreen, "SUCCESS:"))
	fmt.Printf("   Valid sequential linearization found among %d operations.\n", cs.History().Len())
	fmt.Printf("   Search states explored: %d | Verification time: %v\n",
		res.StatesExplored, checkDuration)
	fmt.Printf("   Deterministic seed for reproduction: 0x%X\n", *seed)

	return nil
}
