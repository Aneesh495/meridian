package workflow

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestDurableWorkflowReplayDeduplication(t *testing.T) {
	history := NewHistory("wf-transfer-001", "run-1")

	var step1Calls int32
	var step2Calls int32

	activities := map[string]ActivityFunc{
		"DebitAccount": func(ctx context.Context, in []byte) ([]byte, error) {
			atomic.AddInt32(&step1Calls, 1)
			return []byte("debited-100"), nil
		},
		"CreditAccount": func(ctx context.Context, in []byte) ([]byte, error) {
			atomic.AddInt32(&step2Calls, 1)
			return []byte("credited-100"), nil
		},
	}

	// 1. Initial run: simulates failure right before step 2 executes
	history.Append(HistoryEvent{Type: EventWorkflowStarted})
	history.Append(HistoryEvent{
		Type:    EventActivityCompleted,
		Name:    "DebitAccount",
		Payload: []byte("debited-100"),
	})

	// 2. Replay execution with same history
	replayer := NewReplayEngine(history, activities)

	transferWorkflow := func(ctx WorkflowContext, in []byte) ([]byte, error) {
		f1 := ctx.ExecuteActivity("DebitAccount", []byte("100"), ActivityTaskOptions{})
		res1, err := f1.Get()
		if err != nil {
			return nil, err
		}

		f2 := ctx.ExecuteActivity("CreditAccount", res1.([]byte), ActivityTaskOptions{})
		res2, err := f2.Get()
		if err != nil {
			return nil, err
		}

		return res2.([]byte), nil
	}

	res, err := replayer.Execute(transferWorkflow, []byte("start"))
	if err != nil {
		t.Fatalf("workflow execution failed: %v", err)
	}

	if !bytes.Equal(res, []byte("credited-100")) {
		t.Fatalf("unexpected workflow result: %s", string(res))
	}

	// DebitAccount was in history so it should NOT have been re-executed!
	if atomic.LoadInt32(&step1Calls) != 0 {
		t.Fatalf("expected step 1 calls = 0 (deduplicated by replay), got %d", step1Calls)
	}

	// CreditAccount was NOT in history, so it was executed once
	if atomic.LoadInt32(&step2Calls) != 1 {
		t.Fatalf("expected step 2 calls = 1, got %d", step2Calls)
	}
}

func TestSagaCompensationsOnFailure(t *testing.T) {
	history := NewHistory("wf-saga-001", "run-1")

	var hotelBooked bool
	var hotelCanceled bool
	var flightBooked bool

	activities := map[string]ActivityFunc{
		"BookHotel": func(ctx context.Context, in []byte) ([]byte, error) {
			hotelBooked = true
			return []byte("hotel-confirmed"), nil
		},
		"CancelHotel": func(ctx context.Context, in []byte) ([]byte, error) {
			hotelCanceled = true
			return []byte("hotel-refunded"), nil
		},
		"BookFlight": func(ctx context.Context, in []byte) ([]byte, error) {
			flightBooked = true
			return nil, errors.New("flight fully booked - card decline")
		},
	}

	replayer := NewReplayEngine(history, activities)

	travelBookingSaga := func(ctx WorkflowContext, in []byte) ([]byte, error) {
		f1 := ctx.ExecuteActivity("BookHotel", nil, ActivityTaskOptions{})
		_, err := f1.Get()
		if err != nil {
			return nil, err
		}

		ctx.Saga().AddStep("CancelHotel", func() error {
			f := ctx.ExecuteActivity("CancelHotel", nil, ActivityTaskOptions{})
			_, err := f.Get()
			return err
		})

		f2 := ctx.ExecuteActivity("BookFlight", nil, ActivityTaskOptions{})
		_, err = f2.Get()
		if err != nil {
			return nil, err
		}

		return []byte("trip-booked"), nil
	}

	_, err := replayer.Execute(travelBookingSaga, nil)
	if err == nil {
		t.Fatalf("expected workflow to fail and trigger saga compensations")
	}

	if !hotelBooked {
		t.Fatalf("expected hotel to be booked")
	}
	if !flightBooked {
		t.Fatalf("expected flight booking attempt")
	}
	if !hotelCanceled {
		t.Fatalf("expected saga rollback to cancel hotel reservation")
	}
}
