package core

import (
	"sync"
	"sync/atomic"
)

// RingBuffer is a thread-safe, bounded, power-of-two circular queue.
type RingBuffer[T any] struct {
	_pad0    [56]byte
	head     uint64
	_pad1    [56]byte
	tail     uint64
	_pad2    [56]byte
	mask     uint64
	capacity uint64
	slots    []T
	mu       sync.Mutex
	notEmpty sync.Cond
	notFull  sync.Cond
	closed   bool
}

// NewRingBuffer allocates a new RingBuffer. Capacity is rounded up to the next power of two.
func NewRingBuffer[T any](capacity uint64) *RingBuffer[T] {
	if capacity < 2 {
		capacity = 2
	}
	// Round up to nearest power of 2
	capP2 := uint64(1)
	for capP2 < capacity {
		capP2 <<= 1
	}

	rb := &RingBuffer[T]{
		capacity: capP2,
		mask:     capP2 - 1,
		slots:    make([]T, capP2),
	}
	rb.notEmpty.L = &rb.mu
	rb.notFull.L = &rb.mu
	return rb
}

// Push adds an item to the ring buffer. If full, it returns ErrRingBufferFull without blocking.
func (rb *RingBuffer[T]) Push(item T) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return ErrClosed
	}

	if (rb.tail - rb.head) >= rb.capacity {
		return ErrRingBufferFull
	}

	rb.slots[rb.tail&rb.mask] = item
	rb.tail++
	rb.notEmpty.Signal()
	return nil
}

// PushBlocking adds an item, blocking until space is available or ring is closed.
func (rb *RingBuffer[T]) PushBlocking(item T) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for (rb.tail-rb.head) >= rb.capacity && !rb.closed {
		rb.notFull.Wait()
	}

	if rb.closed {
		return ErrClosed
	}

	rb.slots[rb.tail&rb.mask] = item
	rb.tail++
	rb.notEmpty.Signal()
	return nil
}

// Pop extracts an item from the ring buffer. If empty, returns false.
func (rb *RingBuffer[T]) Pop() (T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var zero T
	if rb.head == rb.tail {
		return zero, false
	}

	idx := rb.head & rb.mask
	item := rb.slots[idx]
	rb.slots[idx] = zero // clear reference to prevent memory leaks
	rb.head++
	rb.notFull.Signal()
	return item, true
}

// PopBlocking extracts an item, blocking until an item is available or ring is closed.
func (rb *RingBuffer[T]) PopBlocking() (T, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	var zero T
	for rb.head == rb.tail && !rb.closed {
		rb.notEmpty.Wait()
	}

	if rb.head == rb.tail {
		return zero, false
	}

	idx := rb.head & rb.mask
	item := rb.slots[idx]
	rb.slots[idx] = zero
	rb.head++
	rb.notFull.Signal()
	return item, true
}

// Len returns the current number of elements queued.
func (rb *RingBuffer[T]) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return int(rb.tail - rb.head)
}

// Cap returns the allocated power-of-two capacity.
func (rb *RingBuffer[T]) Cap() int {
	return int(rb.capacity)
}

// Close closes the ring buffer and wakes all waiting producers and consumers.
func (rb *RingBuffer[T]) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.closed = true
	rb.notEmpty.Broadcast()
	rb.notFull.Broadcast()
}

// LockFreeSPSC is a single-producer single-consumer lock-free ring buffer.
type LockFreeSPSC[T any] struct {
	_pad0    [56]byte
	head     uint64
	_pad1    [56]byte
	tail     uint64
	_pad2    [56]byte
	mask     uint64
	capacity uint64
	slots    []T
}

// NewLockFreeSPSC creates a lock-free SPSC circular queue.
func NewLockFreeSPSC[T any](capacity uint64) *LockFreeSPSC[T] {
	if capacity < 2 {
		capacity = 2
	}
	capP2 := uint64(1)
	for capP2 < capacity {
		capP2 <<= 1
	}
	return &LockFreeSPSC[T]{
		capacity: capP2,
		mask:     capP2 - 1,
		slots:    make([]T, capP2),
	}
}

// Offer attempts to enqueue an item without blocking.
func (q *LockFreeSPSC[T]) Offer(item T) bool {
	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)
	if (tail - head) >= q.capacity {
		return false
	}
	q.slots[tail&q.mask] = item
	atomic.StoreUint64(&q.tail, tail+1)
	return true
}

// Poll attempts to dequeue an item without blocking.
func (q *LockFreeSPSC[T]) Poll() (T, bool) {
	head := atomic.LoadUint64(&q.head)
	tail := atomic.LoadUint64(&q.tail)
	var zero T
	if head == tail {
		return zero, false
	}
	idx := head & q.mask
	item := q.slots[idx]
	q.slots[idx] = zero
	atomic.StoreUint64(&q.head, head+1)
	return item, true
}
