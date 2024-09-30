package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Aneesh495/meridian/pkg/server"
	"github.com/Aneesh495/meridian/pkg/tui"
)

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8000", "Target Meridian cluster node address")
	concurrency := fs.Int("concurrency", 16, "Number of concurrent client workers")
	duration := fs.Duration("duration", 5*time.Second, "Benchmark execution duration")
	valSize := fs.Int("valsize", 128, "Payload value size in bytes")
	readPct := fs.Int("readpct", 50, "Percentage of read operations (0-100)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	client, err := server.NewClient([]string{*addr}, server.DefaultClientOptions)
	if err != nil {
		return err
	}
	defer client.Close()

	// Verify connectivity first
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := client.Ping(ctx); err != nil {
		cancel()
		return fmt.Errorf("pre-flight ping to %s failed: %w", *addr, err)
	}
	cancel()

	fmt.Printf("%s Running synthetic benchmark on %s...\n",
		tui.Colorize(tui.Bold+tui.FgBrightCyan, ">>"), *addr)
	fmt.Printf("   Concurrency: %d | Duration: %v | Value Size: %dB | Reads: %d%% | Writes: %d%%\n\n",
		*concurrency, *duration, *valSize, *readPct, 100-*readPct)

	payload := make([]byte, *valSize)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}

	var opsCompleted atomic.Uint64
	var opsFailed atomic.Uint64
	stopChan := make(chan struct{})

	latenciesMu := sync.Mutex{}
	var latencies []time.Duration

	start := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localRNG := rand.New(rand.NewSource(int64(workerID * 1000)))
			localLats := make([]time.Duration, 0, 1024)

			for {
				select {
				case <-stopChan:
					latenciesMu.Lock()
					latencies = append(latencies, localLats...)
					latenciesMu.Unlock()
					return
				default:
				}

				key := []byte(fmt.Sprintf("bench:key:%d", localRNG.Intn(10000)))
				isRead := localRNG.Intn(100) < *readPct

				opStart := time.Now()
				reqCtx, reqCancel := context.WithTimeout(context.Background(), 2*time.Second)

				var opErr error
				if isRead {
					_, opErr = client.Get(reqCtx, key)
				} else {
					opErr = client.Put(reqCtx, key, payload)
				}
				reqCancel()

				lat := time.Since(opStart)
				if opErr != nil {
					opsFailed.Add(1)
				} else {
					opsCompleted.Add(1)
					localLats = append(localLats, lat)
				}
			}
		}(w)
	}

	time.Sleep(*duration)
	close(stopChan)
	wg.Wait()

	totalTime := time.Since(start)
	completed := opsCompleted.Load()
	failed := opsFailed.Load()

	tps := float64(completed) / totalTime.Seconds()
	mbps := (float64(completed) * float64(*valSize)) / (1024 * 1024 * totalTime.Seconds())

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p99, pMax time.Duration
	if len(latencies) > 0 {
		p50 = latencies[len(latencies)*50/100]
		p90 = latencies[len(latencies)*90/100]
		p99 = latencies[len(latencies)*99/100]
		pMax = latencies[len(latencies)-1]
	}

	fmt.Printf("%s Benchmark Results:\n", tui.Colorize(tui.Bold+tui.FgBrightGreen, ">>"))
	fmt.Printf("   Operations:  %s completed (%s failed)\n",
		tui.Colorize(tui.Bold+tui.FgWhite, fmt.Sprintf("%d", completed)),
		tui.Colorize(tui.FgRed, fmt.Sprintf("%d", failed)))
	fmt.Printf("   Throughput:  %s ops/sec (%.2f MB/sec)\n",
		tui.Colorize(tui.Bold+tui.FgBrightGreen, fmt.Sprintf("%.1f", tps)), mbps)
	fmt.Printf("   Latencies:   p50: %v | p90: %v | p99: %s | max: %v\n",
		p50.Round(time.Microsecond), p90.Round(time.Microsecond),
		tui.Colorize(tui.Bold+tui.FgBrightCyan, p99.Round(time.Microsecond).String()),
		pMax.Round(time.Microsecond))

	return nil
}
