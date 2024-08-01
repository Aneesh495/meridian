package workflow

import (
	"sync"
)

// TimerEntry represents an individual durable timer scheduled in the timing wheel.
type TimerEntry struct {
	ID        uint64
	ExpireAt  int64 // timestamp in milliseconds
	Callback  func()
	canceled  bool
	prev      *TimerEntry
	next      *TimerEntry
}

// Cancel marks the timer as canceled so it will not fire upon expiration.
func (te *TimerEntry) Cancel() {
	te.canceled = true
}

type bucket struct {
	head *TimerEntry
	tail *TimerEntry
}

func (b *bucket) add(e *TimerEntry) {
	e.next = nil
	e.prev = b.tail
	if b.tail != nil {
		b.tail.next = e
	} else {
		b.head = e
	}
	b.tail = e
}

// TimingWheel implements a hierarchical timing wheel with cascading levels for efficient timer management.
type TimingWheel struct {
	mu          sync.Mutex
	tickMs      int64
	wheelSize   int
	interval    int64
	currentTime int64
	buckets     []*bucket
	overflow    *TimingWheel
}

// NewTimingWheel creates a new timing wheel level.
func NewTimingWheel(tickMs int64, wheelSize int, startMs int64) *TimingWheel {
	buckets := make([]*bucket, wheelSize)
	for i := 0; i < wheelSize; i++ {
		buckets[i] = &bucket{}
	}

	return &TimingWheel{
		tickMs:      tickMs,
		wheelSize:   wheelSize,
		interval:    tickMs * int64(wheelSize),
		currentTime: startMs - (startMs % tickMs),
		buckets:     buckets,
	}
}

// Schedule inserts a new timer into the appropriate wheel level.
func (tw *TimingWheel) Schedule(e *TimerEntry) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	return tw.scheduleLocked(e)
}

func (tw *TimingWheel) scheduleLocked(e *TimerEntry) bool {
	if e.canceled {
		return false
	}

	if e.ExpireAt < tw.currentTime+tw.tickMs {
		// Already due in this current tick interval
		return false
	} else if e.ExpireAt < tw.currentTime+tw.interval {
		virtualID := e.ExpireAt / tw.tickMs
		bucketIdx := virtualID % int64(tw.wheelSize)
		tw.buckets[bucketIdx].add(e)
		return true
	} else {
		if tw.overflow == nil {
			tw.overflow = NewTimingWheel(tw.interval, tw.wheelSize, tw.currentTime)
		}
		return tw.overflow.scheduleLocked(e)
	}
}

// Advance moves the wheel forward by tickMs, executing expired timers and cascading overflow buckets.
func (tw *TimingWheel) Advance(nowMs int64) []*TimerEntry {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	var expired []*TimerEntry

	for tw.currentTime+tw.tickMs <= nowMs {
		tw.currentTime += tw.tickMs
		virtualID := tw.currentTime / tw.tickMs
		bIdx := virtualID % int64(tw.wheelSize)
		b := tw.buckets[bIdx]

		curr := b.head
		b.head = nil
		b.tail = nil

		for curr != nil {
			next := curr.next
			if !curr.canceled {
				expired = append(expired, curr)
			}
			curr = next
		}

		if tw.overflow != nil {
			cascaded := tw.overflow.Advance(tw.currentTime)
			for _, e := range cascaded {
				if e.ExpireAt <= tw.currentTime {
					expired = append(expired, e)
				} else {
					tw.scheduleLocked(e)
				}
			}
		}
	}

	return expired
}
