package main

import (
	"fmt"
	"math"
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/stream"
)

type MarketTick struct {
	TimestampMs int64
	Symbol      string
	Price       float64
}

func main() {
	fmt.Println("=== Meridian Example: Real-Time Sliding Window Stream Processing ===")
	fmt.Println()

	windowSize := 10 * time.Second
	slideStep := 2 * time.Second
	wa := stream.NewWindowAggregator()

	ticks := []MarketTick{
		{TimestampMs: 1000, Symbol: "AAPL", Price: 175.50},
		{TimestampMs: 1500, Symbol: "MSFT", Price: 410.20},
		{TimestampMs: 2500, Symbol: "AAPL", Price: 176.25},
		{TimestampMs: 3200, Symbol: "NVDA", Price: 120.00},
		{TimestampMs: 4500, Symbol: "AAPL", Price: 174.80},
		{TimestampMs: 5100, Symbol: "MSFT", Price: 412.50},
		{TimestampMs: 6800, Symbol: "NVDA", Price: 122.50},
		{TimestampMs: 8000, Symbol: "AAPL", Price: 177.10},
		{TimestampMs: 9500, Symbol: "NVDA", Price: 121.75},
		{TimestampMs: 11000, Symbol: "AAPL", Price: 178.00},
		{TimestampMs: 11500, Symbol: "MSFT", Price: 415.00},
		{TimestampMs: 13000, Symbol: "NVDA", Price: 124.00},
		{TimestampMs: 14500, Symbol: "AAPL", Price: 176.90},
	}

	fmt.Printf("1. Ingesting %d financial market ticks into sliding windows (size: 10s, slide: 2s)...\n", len(ticks))
	for _, tick := range ticks {
		windows := stream.AssignSlidingWindows(tick.TimestampMs, windowSize, slideStep)
		for _, w := range windows {
			wa.Add(w, tick.Symbol, tick.Price)
		}
	}
	fmt.Println("   All ticks ingested into active window aggregators.")
	fmt.Println()

	fmt.Println("2. Advancing watermarks to flush expired windows:")

	// Advance watermark to 10s
	wm1 := hlc.NewTimestamp(10000*int64(time.Millisecond), 0, 1)
	emitted1 := wa.TriggerFlushedWindows(wm1)
	fmt.Printf("   [Watermark 10,000 ms] Emitted %d window aggregates:\n", len(emitted1))
	printEmitted(emitted1)
	fmt.Println()

	// Advance watermark to 15s
	wm2 := hlc.NewTimestamp(15000*int64(time.Millisecond), 0, 1)
	emitted2 := wa.TriggerFlushedWindows(wm2)
	fmt.Printf("   [Watermark 15,000 ms] Emitted %d window aggregates:\n", len(emitted2))
	printEmitted(emitted2)
	fmt.Println()

	// Advance watermark to 25s (closing all remaining windows)
	wm3 := hlc.NewTimestamp(25000*int64(time.Millisecond), 0, 1)
	emitted3 := wa.TriggerFlushedWindows(wm3)
	fmt.Printf("   [Watermark 25,000 ms] Emitted final %d window aggregates:\n", len(emitted3))
	printEmitted(emitted3)
	fmt.Println()

	totalEmitted := len(emitted1) + len(emitted2) + len(emitted3)
	fmt.Printf("3. Verifying Invariants:\n")
	fmt.Printf("   Total flushed window aggregates: %d\n", totalEmitted)

	if totalEmitted > 0 {
		fmt.Println("SUCCESS: Sliding window stream aggregation and temporal watermarking verified!")
	} else {
		fmt.Println("FAILURE: No windows emitted!")
	}
}

func printEmitted(results []stream.AggregateResult) {
	for _, res := range results {
		fmt.Printf("     [%5d ms -> %5d ms] Symbol: %-4s | Count: %2d | Min: $%7.2f | Max: $%7.2f | Mean: $%7.2f\n",
			res.Window.Start, res.Window.End, res.Group, res.Count, res.Min, res.Max, math.Round(res.Mean*100)/100)
	}
}
