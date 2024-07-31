package workflow

import (
	"fmt"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

// EventType enumerates the deterministic state changes recorded in workflow execution history.
type EventType uint16

const (
	EventWorkflowStarted EventType = iota + 1
	EventWorkflowCompleted
	EventWorkflowFailed
	EventWorkflowTimedOut

	EventActivityScheduled
	EventActivityStarted
	EventActivityCompleted
	EventActivityFailed
	EventActivityTimedOut

	EventTimerStarted
	EventTimerFired
	EventTimerCanceled

	EventSignalReceived
	EventMarkerRecorded

	EventSagaStepRegistered
	EventSagaCompensationStarted
	EventSagaCompensated
)

func (e EventType) String() string {
	switch e {
	case EventWorkflowStarted:
		return "WORKFLOW_STARTED"
	case EventWorkflowCompleted:
		return "WORKFLOW_COMPLETED"
	case EventWorkflowFailed:
		return "WORKFLOW_FAILED"
	case EventActivityScheduled:
		return "ACTIVITY_SCHEDULED"
	case EventActivityCompleted:
		return "ACTIVITY_COMPLETED"
	case EventActivityFailed:
		return "ACTIVITY_FAILED"
	case EventTimerStarted:
		return "TIMER_STARTED"
	case EventTimerFired:
		return "TIMER_FIRED"
	case EventSignalReceived:
		return "SIGNAL_RECEIVED"
	case EventSagaStepRegistered:
		return "SAGA_STEP_REGISTERED"
	case EventSagaCompensated:
		return "SAGA_COMPENSATED"
	default:
		return fmt.Sprintf("EventType(%d)", e)
	}
}

// HistoryEvent records an immutable step in a workflow execution.
type HistoryEvent struct {
	ID         uint64        `json:"id"`
	Timestamp  hlc.Timestamp `json:"ts"`
	Type       EventType     `json:"type"`
	Name       string        `json:"name,omitempty"`
	Payload    []byte        `json:"payload,omitempty"`
	Error      string        `json:"error,omitempty"`
	ScheduleID uint64        `json:"sched_id,omitempty"`
}

// ActivityTaskOptions configures timeouts and retry policy for an activity execution.
type ActivityTaskOptions struct {
	Timeout          uint64  `json:"timeout"` // in milliseconds
	MaxAttempts      int     `json:"max_attempts"`
	InitialInterval  uint64  `json:"init_interval"`
	BackoffMult      float64 `json:"backoff_mult"`
}
