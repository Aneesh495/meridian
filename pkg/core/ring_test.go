package core

import (
	"sync"
	"testing"
)

func TestRingBufferBasic(t *testing.T) {
	rb := NewRingBuffer[int](4)
	if rb.Cap() != 4 {
		t.Fatalf("expected capacity 4, got %d", rb.Cap())
	}

	for i := 0; i < 4; i++ {
		if err := rb.Push(i); err != nil {
			t.Fatalf("failed to push %d: %v", i, err)
		}
	}

	if err := rb.Push(99); err != ErrRingBufferFull {
		t.Fatalf("expected ErrRingBufferFull, got %v", err)
	}

	if rb.Len() != 4 {
		t.Fatalf("expected length 4, got %d", rb.Len())
	}

	for i := 0; i < 4; i++ {
		val, ok := rb.Pop()
		if !ok || val != i {
			t.Fatalf("expected %d, got %d (ok=%v)", i, val, ok)
		}
	}

	if _, ok := rb.Pop(); ok {
		t.Fatalf("expected empty buffer pop to fail")
	}
}

func TestRingBufferConcurrentBlocking(t *testing.T) {
	rb := NewRingBuffer[int](16)
	const total = 1000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			if err := rb.PushBlocking(i); err != nil {
				t.Errorf("push blocking failed: %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			val, ok := rb.PopBlocking()
			if !ok || val != i {
				t.Errorf("pop blocking expected %d, got %d", i, val)
				return
			}
		}
	}()

	wg.Wait()
}

func TestLockFreeSPSC(t *testing.T) {
	q := NewLockFreeSPSC[int](128)
	const total = 50000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			for !q.Offer(i) {
				// spin
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < total; i++ {
			for {
				val, ok := q.Poll()
				if ok {
					if val != i {
						t.Errorf("expected %d, got %d", i, val)
					}
					break
				}
			}
		}
	}()

	wg.Wait()
}
