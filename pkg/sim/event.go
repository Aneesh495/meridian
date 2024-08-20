package sim

import (
	"container/heap"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Event represents a discrete action scheduled to occur at a specific virtual time.
type Event struct {
	ID        uint64
	TimeNano  int64
	Source    core.NodeID
	Target    core.NodeID
	Type      string
	Execute   func() error
	Canceled  bool
	index     int // internal heap index
}

// eventHeap implements heap.Interface for Events ordered by TimeNano and ID.
type eventHeap []*Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	if h[i].TimeNano == h[j].TimeNano {
		return h[i].ID < h[j].ID // Deterministic tie-breaker
	}
	return h[i].TimeNano < h[j].TimeNano
}

func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *eventHeap) Push(x any) {
	n := len(*h)
	item := x.(*Event)
	item.index = n
	*h = append(*h, item)
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// EventQueue is a thread-safe priority queue scheduling discrete events.
type EventQueue struct {
	mu     sync.Mutex
	pq     eventHeap
	nextID uint64
}

// NewEventQueue initializes an empty event scheduler.
func NewEventQueue() *EventQueue {
	eq := &EventQueue{
		pq: make(eventHeap, 0, 1024),
	}
	heap.Init(&eq.pq)
	return eq
}

// Schedule enqueues an event to execute at target virtual time.
func (eq *EventQueue) Schedule(timeNano int64, src, dst core.NodeID, eventType string, exec func() error) *Event {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	eq.nextID++
	ev := &Event{
		ID:       eq.nextID,
		TimeNano: timeNano,
		Source:   src,
		Target:   dst,
		Type:     eventType,
		Execute:  exec,
	}

	heap.Push(&eq.pq, ev)
	return ev
}

// ScheduleAfter schedules an event delta time after currentTimeNano.
func (eq *EventQueue) ScheduleAfter(currentTimeNano int64, delta time.Duration, src, dst core.NodeID, eventType string, exec func() error) *Event {
	return eq.Schedule(currentTimeNano+delta.Nanoseconds(), src, dst, eventType, exec)
}

// Pop retrieves and removes the earliest scheduled event.
func (eq *EventQueue) Pop() *Event {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	for eq.pq.Len() > 0 {
		ev := heap.Pop(&eq.pq).(*Event)
		if !ev.Canceled {
			return ev
		}
	}

	return nil
}

// Peek inspects the earliest event without removing it.
func (eq *EventQueue) Peek() *Event {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	for eq.pq.Len() > 0 {
		ev := eq.pq[0]
		if !ev.Canceled {
			return ev
		}
		heap.Pop(&eq.pq)
	}

	return nil
}

// Len returns the count of scheduled pending events.
func (eq *EventQueue) Len() int {
	eq.mu.Lock()
	defer eq.mu.Unlock()
	return eq.pq.Len()
}

// Cancel marks an event as cancelled so it will not execute.
func (eq *EventQueue) Cancel(e *Event) {
	if e == nil {
		return
	}
	eq.mu.Lock()
	defer eq.mu.Unlock()
	e.Canceled = true
}
