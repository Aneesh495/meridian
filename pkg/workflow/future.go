package workflow

import (
	"errors"
	"sync"
)

var (
	ErrFutureNotReady = errors.New("meridian/workflow: future result not ready")
	ErrAllFailed      = errors.New("meridian/workflow: all futures failed in Any combinator")
)

// Future represents an asynchronous computation result.
type Future interface {
	Get() (any, error)
	IsReady() bool
	Wait()
}

// Promise fulfills or rejects an associated Future.
type Promise struct {
	mu     sync.RWMutex
	ready  bool
	val    any
	err    error
	doneCh chan struct{}
	once   sync.Once
}

// NewPromise creates an unresolved Promise.
func NewPromise() *Promise {
	return &Promise{
		doneCh: make(chan struct{}),
	}
}

// Resolve sets the success value and completes the future.
func (p *Promise) Resolve(val any) {
	p.once.Do(func() {
		p.mu.Lock()
		p.val = val
		p.ready = true
		p.mu.Unlock()
		close(p.doneCh)
	})
}

// Reject sets the failure error and completes the future.
func (p *Promise) Reject(err error) {
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		p.ready = true
		p.mu.Unlock()
		close(p.doneCh)
	})
}

// Get returns the value or error once resolved.
func (p *Promise) Get() (any, error) {
	<-p.doneCh
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.val, p.err
}

// IsReady returns true if resolved or rejected.
func (p *Promise) IsReady() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ready
}

// Wait blocks until the promise resolves.
func (p *Promise) Wait() {
	<-p.doneCh
}
