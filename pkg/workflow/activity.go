package workflow

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// ActivityFunc defines the user activity implementation signature.
type ActivityFunc func(ctx context.Context, input []byte) ([]byte, error)

// ExecuteWithRetry executes an activity applying exponential backoff and jitter up to MaxAttempts.
func ExecuteWithRetry(ctx context.Context, fn ActivityFunc, input []byte, opts ActivityTaskOptions) ([]byte, error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 1
	}
	if opts.InitialInterval <= 0 {
		opts.InitialInterval = 50
	}
	if opts.BackoffMult <= 1.0 {
		opts.BackoffMult = 2.0
	}

	var lastErr error
	interval := float64(opts.InitialInterval)

	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		res, err := fn(ctx, input)
		if err == nil {
			return res, nil
		}
		lastErr = err

		if attempt == opts.MaxAttempts {
			break
		}

		// Calculate jittered exponential backoff
		jitter := 0.8 + rand.Float64()*0.4 // 80% to 120%
		sleepDuration := time.Duration(interval * jitter * float64(time.Millisecond))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepDuration):
		}

		interval *= opts.BackoffMult
		if interval > math.MaxInt64/2 {
			interval = float64(time.Minute.Milliseconds())
		}
	}

	return nil, errors.Join(errors.New("activity exhausted maximum attempts"), lastErr)
}
