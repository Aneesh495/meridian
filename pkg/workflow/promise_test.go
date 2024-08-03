package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestPromiseResolveAndGet(t *testing.T) {
	p := NewPromise()
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.Resolve("workflow-result-42")
	}()

	val, err := p.Get()
	if err != nil || val.(string) != "workflow-result-42" {
		t.Fatalf("expected 'workflow-result-42', got %v (err: %v)", val, err)
	}
}

func TestFutureAllCombinator(t *testing.T) {
	p1 := NewPromise()
	p2 := NewPromise()

	go func() {
		time.Sleep(10 * time.Millisecond)
		p1.Resolve(100)
	}()
	go func() {
		time.Sleep(20 * time.Millisecond)
		p2.Resolve(200)
	}()

	all := All(p1, p2)
	val, err := all.Get()
	if err != nil {
		t.Fatalf("All failed: %v", err)
	}

	results := val.([]any)
	if len(results) != 2 || results[0].(int) != 100 || results[1].(int) != 200 {
		t.Fatalf("unexpected All results: %v", results)
	}
}

func TestFutureAnyCombinator(t *testing.T) {
	p1 := NewPromise()
	p2 := NewPromise()

	go func() {
		time.Sleep(10 * time.Millisecond)
		p1.Reject(errors.New("branch 1 failed"))
	}()
	go func() {
		time.Sleep(30 * time.Millisecond)
		p2.Resolve("branch 2 succeeded")
	}()

	any := Any(p1, p2)
	val, err := any.Get()
	if err != nil || val.(string) != "branch 2 succeeded" {
		t.Fatalf("expected branch 2 to succeed, got: %v (err: %v)", val, err)
	}
}
