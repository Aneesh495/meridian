package bench_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/sim"
)

func BenchmarkSim_Xoshiro256RNG(b *testing.B) {
	rng := sim.NewRNG(0x123456789ABCDEF0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rng.Uint64()
	}
}

func BenchmarkSim_StdMathRand(b *testing.B) {
	r := rand.New(rand.NewSource(0x123456789ABCDEF0))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Uint64()
	}
}

func BenchmarkSim_EventQueue_SchedulePop(b *testing.B) {
	q := sim.NewEventQueue()
	noOp := func() error { return nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Schedule(int64(i*10), core.NodeID(1), core.NodeID(2), "event", noOp)
		_ = q.Pop()
	}
}

func BenchmarkSim_VirtualNetwork_SendDeliver(b *testing.B) {
	clock := sim.NewClock(1000)
	events := sim.NewEventQueue()
	rng := sim.NewRNG(0x4D4552494449414E)
	partitions := sim.NewPartitionMatrix()
	net := sim.NewVirtualNetwork(clock, events, rng, partitions, sim.DefaultNetworkConfig)

	net.Register(core.NodeID(2), func(from core.NodeID, msgType protocol.MsgType, payload []byte) {})
	payload := []byte("virtual-packet-payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = net.Send(core.NodeID(1), core.NodeID(2), 0x01, payload)
		ev := events.Pop()
		if ev != nil && ev.Execute != nil {
			_ = ev.Execute()
		}
	}
}

func BenchmarkSim_WGLLinearizabilityChecker(b *testing.B) {
	model := sim.KVModel{}

	// Pre-populate history of 50 concurrent operations
	hist := sim.NewHistory()
	for i := 0; i < 50; i++ {
		start := int64(i * 10)
		end := start + 15
		key := fmt.Sprintf("k%d", i%5)
		val := []byte(fmt.Sprintf("v%d", i))
		hist.RecordWrite(i%3+1, key, val, start, end, true)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := sim.CheckLinearizability(model, hist)
		if !res.Linearizable {
			b.Fatalf("expected trace to be linearizable")
		}
	}
}
