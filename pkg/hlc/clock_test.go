package hlc

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

func TestHLCMonotonicity(t *testing.T) {
	fakeTime := int64(1000000000)
	clock := NewClockWithSource(core.NodeID(1), 250*time.Millisecond, func() int64 {
		return fakeTime
	})

	ts1 := clock.Now()
	ts2 := clock.Now()
	ts3 := clock.Now()

	if !ts1.Less(ts2) || !ts2.Less(ts3) {
		t.Fatalf("timestamps must be strictly monotonically increasing: %v, %v, %v", ts1, ts2, ts3)
	}

	if ts1.WallTime != fakeTime || ts2.WallTime != fakeTime || ts3.WallTime != fakeTime {
		t.Fatalf("wall times should match static physical time")
	}

	if ts1.Logical != 0 || ts2.Logical != 1 || ts3.Logical != 2 {
		t.Fatalf("logical counters should increment: %d, %d, %d", ts1.Logical, ts2.Logical, ts3.Logical)
	}

	// Advance physical time
	fakeTime += 500000
	ts4 := clock.Now()
	if !ts3.Less(ts4) {
		t.Fatalf("ts3 must be less than ts4: %v vs %v", ts3, ts4)
	}
	if ts4.WallTime != fakeTime || ts4.Logical != 0 {
		t.Fatalf("ts4 should reset logical counter upon physical clock advance: %v", ts4)
	}
}

func TestHLCUpdateCausality(t *testing.T) {
	timeNode1 := int64(1000)
	timeNode2 := int64(950)

	c1 := NewClockWithSource(core.NodeID(1), 500*time.Millisecond, func() int64 { return timeNode1 })
	c2 := NewClockWithSource(core.NodeID(2), 500*time.Millisecond, func() int64 { return timeNode2 })

	event1 := c1.Now()

	// Node 2 receives event from Node 1
	event2, err := c2.Update(event1)
	if err != nil {
		t.Fatalf("unexpected error updating c2: %v", err)
	}

	if !event1.Less(event2) {
		t.Fatalf("causality violated: event1 %v should be less than event2 %v", event1, event2)
	}

	if event2.WallTime != event1.WallTime {
		t.Fatalf("event2 should catch up to event1 wall time: %d vs %d", event2.WallTime, event1.WallTime)
	}
}

func TestHLCRejectDrift(t *testing.T) {
	localTime := int64(1000)
	clock := NewClockWithSource(core.NodeID(1), 100*time.Millisecond, func() int64 { return localTime })

	// Remote time 200ms in future exceeds 100ms max drift
	remote := NewTimestamp(localTime+200*int64(time.Millisecond), 0, core.NodeID(2))

	_, err := clock.Update(remote)
	if err == nil || !errors.Is(err, ErrClockDriftTooHigh) {
		t.Fatalf("expected ErrClockDriftTooHigh, got %v", err)
	}
}

func TestHLCConcurrentAccess(t *testing.T) {
	clock := NewClock(core.NodeID(1), 250*time.Millisecond)
	const numGoroutines = 10
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()
			var prev Timestamp
			for i := 0; i < iterations; i++ {
				curr := clock.Now()
				if !prev.IsZero() && !prev.Less(curr) {
					t.Errorf("monotonicity violation: prev=%v, curr=%v", prev, curr)
				}
				prev = curr
			}
		}()
	}

	wg.Wait()
}

func TestTimestampSerializationRoundTrip(t *testing.T) {
	orig := NewTimestamp(1710000000123456789, 42, core.NodeID(7))
	str := orig.String()

	parsed, err := ParseTimestamp(str)
	if err != nil {
		t.Fatalf("failed to parse string timestamp: %v", err)
	}
	if !orig.Equal(parsed) {
		t.Fatalf("string round-trip mismatch: orig=%v, parsed=%v", orig, parsed)
	}

	var buf [BinarySize]byte
	orig.Encode(buf[:])

	decoded, err := Decode(buf[:])
	if err != nil {
		t.Fatalf("failed to decode binary timestamp: %v", err)
	}
	if !orig.Equal(decoded) {
		t.Fatalf("binary round-trip mismatch: orig=%v, decoded=%v", orig, decoded)
	}
}
