package workflow

import (
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

// History stores the immutable sequence of events for a workflow instance.
type History struct {
	mu         sync.RWMutex
	workflowID core.WorkflowID
	runID      core.RunID
	events     []HistoryEvent
}

// NewHistory creates a new History container.
func NewHistory(workflowID core.WorkflowID, runID core.RunID) *History {
	return &History{
		workflowID: workflowID,
		runID:      runID,
		events:     make([]HistoryEvent, 0, 32),
	}
}

// Append appends an event to the history log, assigning it the next sequential ID.
func (h *History) Append(event HistoryEvent) HistoryEvent {
	h.mu.Lock()
	defer h.mu.Unlock()

	event.ID = uint64(len(h.events) + 1)
	h.events = append(h.events, event)
	return event
}

// Events returns a copy of all recorded history events.
func (h *History) Events() []HistoryEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()

	res := make([]HistoryEvent, len(h.events))
	copy(res, h.events)
	return res
}

// Len returns the number of recorded events.
func (h *History) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.events)
}

// WorkflowID returns the associated workflow ID.
func (h *History) WorkflowID() core.WorkflowID {
	return h.workflowID
}

// RunID returns the execution run ID.
func (h *History) RunID() core.RunID {
	return h.runID
}
