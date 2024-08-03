package workflow

import (
	"sync"
)

// All combines multiple futures, resolving with a slice of results when all succeed,
// or rejecting immediately on the first failure.
func All(futures ...Future) Future {
	p := NewPromise()
	if len(futures) == 0 {
		p.Resolve([]any{})
		return p
	}

	go func() {
		results := make([]any, len(futures))
		var wg sync.WaitGroup
		var firstErr error
		var once sync.Once

		for i, f := range futures {
			wg.Add(1)
			go func(idx int, fut Future) {
				defer wg.Done()
				val, err := fut.Get()
				if err != nil {
					once.Do(func() {
						firstErr = err
					})
				} else {
					results[idx] = val
				}
			}(i, f)
		}

		wg.Wait()
		if firstErr != nil {
			p.Reject(firstErr)
		} else {
			p.Resolve(results)
		}
	}()

	return p
}

// Any resolves with the first successful future result, or rejects if all futures fail.
func Any(futures ...Future) Future {
	p := NewPromise()
	if len(futures) == 0 {
		p.Reject(ErrAllFailed)
		return p
	}

	go func() {
		successChan := make(chan any, 1)
		failCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		for _, f := range futures {
			wg.Add(1)
			go func(fut Future) {
				defer wg.Done()
				val, err := fut.Get()
				if err == nil {
					select {
					case successChan <- val:
					default:
					}
				} else {
					mu.Lock()
					failCount++
					mu.Unlock()
				}
			}(f)
		}

		go func() {
			wg.Wait()
			close(successChan)
		}()

		val, ok := <-successChan
		if ok {
			p.Resolve(val)
		} else {
			p.Reject(ErrAllFailed)
		}
	}()

	return p
}
