package core

import (
	"sync"
)

// Size classes for pooled byte buffers.
var sizeClasses = []int{
	64,
	256,
	1024,      // 1KB
	4096,      // 4KB
	16384,     // 16KB
	65536,     // 64KB
	262144,    // 256KB
	1048576,   // 1MB
}

// BufferPool manages size-segregated byte slice allocations to minimize GC pressure.
type BufferPool struct {
	pools []*sync.Pool
}

// GlobalBufferPool is the singleton buffer pool across Meridian engines.
var GlobalBufferPool = NewBufferPool()

// NewBufferPool allocates a tiered buffer pool across standard size classes.
func NewBufferPool() *BufferPool {
	bp := &BufferPool{
		pools: make([]*sync.Pool, len(sizeClasses)),
	}
	for i, size := range sizeClasses {
		s := size
		bp.pools[i] = &sync.Pool{
			New: func() any {
				return make([]byte, s)
			},
		}
	}
	return bp
}

func (bp *BufferPool) classIndex(capacity int) int {
	for i, size := range sizeClasses {
		if capacity <= size {
			return i
		}
	}
	return -1
}

// Get borrows a byte slice with at least the requested capacity.
func (bp *BufferPool) Get(capacity int) []byte {
	idx := bp.classIndex(capacity)
	if idx == -1 {
		return make([]byte, capacity)
	}
	buf := bp.pools[idx].Get().([]byte)
	return buf[:capacity]
}

// Put returns a borrowed byte slice to the pool.
func (bp *BufferPool) Put(b []byte) {
	capSize := cap(b)
	idx := bp.classIndex(capSize)
	if idx == -1 {
		return
	}
	// Only recycle if it matches the class capacity exactly
	if capSize == sizeClasses[idx] {
		bp.pools[idx].Put(b[:capSize])
	}
}

// ByteBufferPool manages reusable ByteBuffer objects.
type ByteBufferPool struct {
	pool sync.Pool
}

// GlobalByteBufferPool is the singleton ByteBuffer pool.
var GlobalByteBufferPool = NewByteBufferPool(4096)

// NewByteBufferPool creates a pool of ByteBuffer objects with default capacity.
func NewByteBufferPool(defaultCap int) *ByteBufferPool {
	return &ByteBufferPool{
		pool: sync.Pool{
			New: func() any {
				return NewByteBuffer(defaultCap)
			},
		},
	}
}

// Get acquires a clean ByteBuffer from the pool.
func (p *ByteBufferPool) Get() *ByteBuffer {
	buf := p.pool.Get().(*ByteBuffer)
	buf.Reset()
	return buf
}

// Put returns a ByteBuffer to the pool.
func (p *ByteBufferPool) Put(buf *ByteBuffer) {
	if buf == nil {
		return
	}
	// Avoid hoarding oversized buffers in pool
	if buf.Len() > 4*1024*1024 {
		return
	}
	buf.Reset()
	p.pool.Put(buf)
}
