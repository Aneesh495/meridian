package workflow

import (
	"errors"
	"fmt"
	"sync"
)

// CompensationStep stores an individual compensation operation to be executed during rollback.
type CompensationStep struct {
	Name         string
	CompensateFn func() error
}

// Saga orchestrates multi-step distributed operations with automated forward and reverse rollbacks.
type Saga struct {
	mu            sync.Mutex
	compensations []CompensationStep
}

// NewSaga creates an empty Saga.
func NewSaga() *Saga {
	return &Saga{
		compensations: make([]CompensationStep, 0, 8),
	}
}

// AddStep registers a compensation closure associated with a forward step.
func (s *Saga) AddStep(name string, compensateFn func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.compensations = append(s.compensations, CompensationStep{
		Name:         name,
		CompensateFn: compensateFn,
	})
}

// Compensate unwinds all registered compensation steps in reverse LIFO order.
func (s *Saga) Compensate() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	// Reverse iteration
	for i := len(s.compensations) - 1; i >= 0; i-- {
		step := s.compensations[i]
		if step.CompensateFn != nil {
			if err := step.CompensateFn(); err != nil {
				errs = append(errs, fmt.Errorf("saga compensation step '%s' failed: %w", step.Name, err))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
