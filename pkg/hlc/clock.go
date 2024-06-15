package hlc

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// ErrClockDriftTooHigh indicates that the remote timestamp exceeds the maximum tolerable physical offset.
var ErrClockDriftTooHigh = errors.New("meridian/hlc: remote timestamp exceeds maximum physical clock drift")

// PhysicalTimeSource abstracts the physical wall-clock provider.
type PhysicalTimeSource func() int64

// UnixNanoTimeSource returns the current local system time in nanoseconds.
func UnixNanoTimeSource() int64 {
	return time.Now().UnixNano()
}

// Clock implements a thread-safe Hybrid Logical Clock.
// Guarantees:
// 1. Strict causality: if event e1 happens-before e2, then HLC(e1) < HLC(e2).
// 2. Physical boundedness: |HLC.WallTime - PhysicalTime| <= MaxOffset.
// 3. Monotonicity: local HLC timestamps are strictly monotonically increasing.
type Clock struct {
	mu        sync.Mutex
	nodeID    core.NodeID
	latest    Timestamp
	maxOffset int64 // maximum physical clock skew in nanoseconds
	nowSource PhysicalTimeSource
}

// NewClock creates a new Hybrid Logical Clock for the given node.
// Default maxOffset is typically 250 milliseconds.
func NewClock(nodeID core.NodeID, maxOffset time.Duration) *Clock {
	return NewClockWithSource(nodeID, maxOffset, UnixNanoTimeSource)
}

// NewClockWithSource creates a Clock with a custom physical time source (used for simulation).
func NewClockWithSource(nodeID core.NodeID, maxOffset time.Duration, source PhysicalTimeSource) *Clock {
	return &Clock{
		nodeID:    nodeID,
		maxOffset: int64(maxOffset),
		nowSource: source,
		latest: Timestamp{
			WallTime: 0,
			Logical:  0,
			NodeID:   nodeID,
		},
	}
}

// Now returns a new monotonically increasing timestamp for a local event.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.nowSource()

	if pt > c.latest.WallTime {
		c.latest.WallTime = pt
		c.latest.Logical = 0
	} else {
		c.latest.Logical++
	}

	return c.latest
}

// Update advances the local clock based on a received remote timestamp.
// Used when receiving network messages to maintain causal order.
func (c *Clock) Update(remote Timestamp) (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pt := c.nowSource()

	// Verify drift limit: remote wall-clock cannot be too far in the future
	if remote.WallTime > pt && (remote.WallTime-pt) > c.maxOffset {
		return c.latest, fmt.Errorf("%w: remote=%d local_pt=%d offset=%d max=%d",
			ErrClockDriftTooHigh, remote.WallTime, pt, remote.WallTime-pt, c.maxOffset)
	}

	maxWall := pt
	if c.latest.WallTime > maxWall {
		maxWall = c.latest.WallTime
	}
	if remote.WallTime > maxWall {
		maxWall = remote.WallTime
	}

	if maxWall == c.latest.WallTime && maxWall == remote.WallTime {
		maxLogical := c.latest.Logical
		if remote.Logical > maxLogical {
			maxLogical = remote.Logical
		}
		c.latest.Logical = maxLogical + 1
	} else if maxWall == c.latest.WallTime {
		c.latest.Logical++
	} else if maxWall == remote.WallTime {
		c.latest.Logical = remote.Logical + 1
	} else {
		c.latest.Logical = 0
	}

	c.latest.WallTime = maxWall
	return c.latest, nil
}

// Latest returns the current state of the clock without advancing it.
func (c *Clock) Latest() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.latest
}

// NodeID returns the node identifier assigned to this clock.
func (c *Clock) NodeID() core.NodeID {
	return c.nodeID
}
