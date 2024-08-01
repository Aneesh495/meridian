package workflow

import (
	"testing"
)

func TestHierarchicalTimingWheelCascading(t *testing.T) {
	start := int64(1000)
	tw := NewTimingWheel(10, 10, start)

	var fired []uint64

	t1 := &TimerEntry{
		ID:       1,
		ExpireAt: start + 30,
		Callback: func() { fired = append(fired, 1) },
	}
	t2 := &TimerEntry{
		ID:       2,
		ExpireAt: start + 150,
		Callback: func() { fired = append(fired, 2) },
	}
	t3 := &TimerEntry{
		ID:       3,
		ExpireAt: start + 50,
		Callback: func() { fired = append(fired, 3) },
	}

	tw.Schedule(t1)
	tw.Schedule(t2)
	tw.Schedule(t3)
	t3.Cancel()

	expired := tw.Advance(start + 40)
	for _, e := range expired {
		e.Callback()
	}

	if len(fired) != 1 || fired[0] != 1 {
		t.Fatalf("expected timer 1 to fire, got: %v", fired)
	}

	expired = tw.Advance(start + 200)
	for _, e := range expired {
		e.Callback()
	}

	if len(fired) != 2 || fired[1] != 2 {
		t.Fatalf("expected timer 2 to fire after cascade, got: %v", fired)
	}
}
