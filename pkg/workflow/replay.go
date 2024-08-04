package workflow

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// WorkflowFunc defines a user-defined durable workflow function.
type WorkflowFunc func(ctx WorkflowContext, input []byte) ([]byte, error)

// ReplayEngine coordinates deterministic workflow replay against history.
type ReplayEngine struct {
	mu           sync.Mutex
	history      *History
	eventIndex   int
	activities   map[string]ActivityFunc
	pendingTasks map[uint64]*Promise
	isReplaying  bool
}

// NewReplayEngine initializes a ReplayEngine over an existing History.
func NewReplayEngine(history *History, activities map[string]ActivityFunc) *ReplayEngine {
	return &ReplayEngine{
		history:      history,
		eventIndex:   0,
		activities:   activities,
		pendingTasks: make(map[uint64]*Promise),
		isReplaying:  history.Len() > 0,
	}
}

// ScheduleActivity either retrieves completed result from history or dispatches fresh execution.
func (r *ReplayEngine) ScheduleActivity(name string, input []byte, opts ActivityTaskOptions) Future {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If replaying, check if next event in history matches this activity
	events := r.history.Events()
	for r.eventIndex < len(events) {
		ev := events[r.eventIndex]
		if ev.Type == EventActivityCompleted && ev.Name == name {
			r.eventIndex++
			p := NewPromise()
			p.Resolve(ev.Payload)
			return p
		}
		if ev.Type == EventActivityFailed && ev.Name == name {
			r.eventIndex++
			p := NewPromise()
			p.Reject(errors.New(ev.Error))
			return p
		}
		r.eventIndex++
	}

	// Live execution
	promise := NewPromise()
	actFn, ok := r.activities[name]
	if !ok {
		promise.Reject(fmt.Errorf("activity '%s' not registered", name))
		return promise
	}

	go func() {
		res, err := actFn(nil, input)
		if err != nil {
			r.history.Append(HistoryEvent{
				Type:  EventActivityFailed,
				Name:  name,
				Error: err.Error(),
			})
			promise.Reject(err)
		} else {
			r.history.Append(HistoryEvent{
				Type:    EventActivityCompleted,
				Name:    name,
				Payload: res,
			})
			promise.Resolve(res)
		}
	}()

	return promise
}

// ScheduleTimer creates a durable timer future.
func (r *ReplayEngine) ScheduleTimer(d time.Duration) Future {
	r.mu.Lock()
	defer r.mu.Unlock()

	events := r.history.Events()
	for r.eventIndex < len(events) {
		ev := events[r.eventIndex]
		if ev.Type == EventTimerFired {
			r.eventIndex++
			p := NewPromise()
			p.Resolve(true)
			return p
		}
		r.eventIndex++
	}

	promise := NewPromise()
	go func() {
		time.Sleep(d)
		r.history.Append(HistoryEvent{
			Type: EventTimerFired,
		})
		promise.Resolve(true)
	}()

	return promise
}

// Execute runs the workflow function to completion.
func (r *ReplayEngine) Execute(fn WorkflowFunc, input []byte) ([]byte, error) {
	ctx := &workflowContextImpl{
		workflowID: r.history.WorkflowID(),
		runID:      r.history.RunID(),
		virtualNow: time.Now(),
		saga:       NewSaga(),
		replayer:   r,
	}

	r.history.Append(HistoryEvent{
		Type:    EventWorkflowStarted,
		Payload: input,
	})

	res, err := fn(ctx, input)
	if err != nil {
		r.history.Append(HistoryEvent{
			Type:  EventWorkflowFailed,
			Error: err.Error(),
		})
		_ = ctx.saga.Compensate()
		return nil, err
	}

	r.history.Append(HistoryEvent{
		Type:    EventWorkflowCompleted,
		Payload: res,
	})

	return res, nil
}
