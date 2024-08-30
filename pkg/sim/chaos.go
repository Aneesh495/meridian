package sim

import (
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// ChaosPlan defines probabilities and intensities of randomized faults during simulation.
type ChaosPlan struct {
	PartitionInterval time.Duration
	PartitionDuration time.Duration
	CrashInterval     time.Duration
	RebootDelay       time.Duration
	DropRateSpike     float64
}

// DefaultChaosPlan returns standard randomized fault injection intervals.
var DefaultChaosPlan = ChaosPlan{
	PartitionInterval: 50 * time.Millisecond,
	PartitionDuration: 30 * time.Millisecond,
	CrashInterval:     80 * time.Millisecond,
	RebootDelay:       20 * time.Millisecond,
	DropRateSpike:     0.20,
}

// NodeLifecycle allows ChaosMonkey to restart or stop simulated nodes.
type NodeLifecycle interface {
	CrashNode(id core.NodeID) error
	RestartNode(id core.NodeID) error
}

// ChaosMonkey schedules randomized distributed faults into the discrete event loop.
type ChaosMonkey struct {
	nodes      []core.NodeID
	clock      *Clock
	events     *EventQueue
	rng        *RNG
	network    *VirtualNetwork
	partitions *PartitionMatrix
	lifecycle  NodeLifecycle
	plan       ChaosPlan
}

// NewChaosMonkey creates a new ChaosMonkey orchestrator.
func NewChaosMonkey(nodes []core.NodeID, clock *Clock, events *EventQueue, rng *RNG, net *VirtualNetwork, pm *PartitionMatrix, lc NodeLifecycle, plan ChaosPlan) *ChaosMonkey {
	return &ChaosMonkey{
		nodes:      nodes,
		clock:      clock,
		events:     events,
		rng:        rng,
		network:    net,
		partitions: pm,
		lifecycle:  lc,
		plan:       plan,
	}
}

// SchedulePartition injects a randomized network partition and schedules its automatic resolution.
func (cm *ChaosMonkey) SchedulePartition(atTime int64) {
	if len(cm.nodes) < 2 {
		return
	}

	exec := func() error {
		// Pick random subset to isolate
		perm := cm.rng.Perm(len(cm.nodes))
		splitPoint := 1 + cm.rng.Intn(len(cm.nodes)-1)

		g1 := make([]core.NodeID, splitPoint)
		g2 := make([]core.NodeID, len(cm.nodes)-splitPoint)

		for i := 0; i < splitPoint; i++ {
			g1[i] = cm.nodes[perm[i]]
		}
		for i := splitPoint; i < len(cm.nodes); i++ {
			g2[i-splitPoint] = cm.nodes[perm[i]]
		}

		cm.partitions.SplitPartitions(g1, g2)

		// Schedule healing after duration
		healTime := cm.clock.NowNano() + cm.plan.PartitionDuration.Nanoseconds()
		cm.events.Schedule(healTime, 0, 0, "chaos_heal_partition", func() error {
			cm.partitions.Heal()
			return nil
		})

		return nil
	}

	cm.events.Schedule(atTime, 0, 0, "chaos_inject_partition", exec)
}

// ScheduleCrash picks a random node, crashes it, and schedules its reboot.
func (cm *ChaosMonkey) ScheduleCrash(atTime int64) {
	if len(cm.nodes) == 0 || cm.lifecycle == nil {
		return
	}

	targetNode := cm.nodes[cm.rng.Intn(len(cm.nodes))]

	exec := func() error {
		_ = cm.lifecycle.CrashNode(targetNode)

		rebootTime := cm.clock.NowNano() + cm.plan.RebootDelay.Nanoseconds()
		cm.events.Schedule(rebootTime, targetNode, targetNode, "chaos_reboot_node", func() error {
			return cm.lifecycle.RestartNode(targetNode)
		})

		return nil
	}

	cm.events.Schedule(atTime, targetNode, targetNode, "chaos_crash_node", exec)
}

// PopulateSchedule schedules chaos events throughout the simulation timeframe up to endNano.
func (cm *ChaosMonkey) PopulateSchedule(startNano, endNano int64) {
	curr := startNano
	for curr < endNano {
		if cm.plan.PartitionInterval > 0 {
			curr += cm.rng.Duration(cm.plan.PartitionInterval/2, cm.plan.PartitionInterval*2).Nanoseconds()
			if curr < endNano {
				cm.SchedulePartition(curr)
			}
		}
		if cm.plan.CrashInterval > 0 {
			crashTime := curr + cm.rng.Duration(cm.plan.CrashInterval/2, cm.plan.CrashInterval*2).Nanoseconds()
			if crashTime < endNano {
				cm.ScheduleCrash(crashTime)
			}
		}
	}
}
