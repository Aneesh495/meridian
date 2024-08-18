package sim

import (
	"sync"
	"time"
)

// Clock provides a deterministic virtual clock for discrete-event simulation.
// Unlike physical clocks, virtual time only advances when explicitly stepped
// by the simulation event loop or when an event is executed.
type Clock struct {
	mu      sync.RWMutex
	current int64 // virtual time in nanoseconds
}

// NewClock initializes a virtual clock starting at virtual epoch.
func NewClock(startNano int64) *Clock {
	if startNano <= 0 {
		startNano = 1_000_000_000 // 1 second into epoch
	}
	return &Clock{
		current: startNano,
	}
}

// Now returns the current virtual time.
func (c *Clock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return time.Unix(0, c.current)
}

// NowNano returns the current virtual timestamp in nanoseconds.
func (c *Clock) NowNano() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.current
}

// Advance steps the virtual clock forward by delta nanoseconds.
func (c *Clock) Advance(delta time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current += delta.Nanoseconds()
	return time.Unix(0, c.current)
}

// Set explicitly sets the virtual clock to target timestamp.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = t.UnixNano()
}

// Source returns a physical-compatible time source function reading virtual time.
func (c *Clock) Source() func() int64 {
	return func() int64 {
		return c.NowNano()
	}
}
