package sim

import (
	"errors"
	"io"
	"sync"
)

var (
	ErrDiskCorrupted = errors.New("meridian/sim: disk block checksum corruption")
	ErrDiskClosed    = errors.New("meridian/sim: virtual disk is closed")
)

// VirtualDisk simulates physical storage media with configurable fault injection,
// supporting page cache buffering, torn writes, power-loss wiping, and corruption.
type VirtualDisk struct {
	mu           sync.RWMutex
	durable      []byte // committed bytes survived across sync
	cache        []byte // dirty unflushed OS page cache
	dirty        bool
	closed       bool
	writeFault   bool // if true, next write will fail or tear
	tornFraction float64
}

// NewVirtualDisk creates an empty virtual disk.
func NewVirtualDisk() *VirtualDisk {
	return &VirtualDisk{
		durable: make([]byte, 0, 4096),
		cache:   make([]byte, 0, 4096),
	}
}

// WriteAt writes data at offset into the disk cache.
func (vd *VirtualDisk) WriteAt(p []byte, off int64) (int, error) {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	if vd.closed {
		return 0, ErrDiskClosed
	}

	dataToWrite := p
	if vd.writeFault && vd.tornFraction > 0 && vd.tornFraction < 1.0 {
		// Simulate torn write: write only a fraction of the payload
		partialLen := int(float64(len(p)) * vd.tornFraction)
		if partialLen < 1 {
			partialLen = 1
		}
		dataToWrite = p[:partialLen]
		vd.writeFault = false
	}

	requiredCap := int(off) + len(dataToWrite)
	if requiredCap > len(vd.cache) {
		newCache := make([]byte, requiredCap)
		copy(newCache, vd.cache)
		vd.cache = newCache
	}

	copy(vd.cache[off:], dataToWrite)
	vd.dirty = true

	return len(dataToWrite), nil
}

// ReadAt reads data at offset from current cache state.
func (vd *VirtualDisk) ReadAt(p []byte, off int64) (int, error) {
	vd.mu.RLock()
	defer vd.mu.RUnlock()

	if vd.closed {
		return 0, ErrDiskClosed
	}

	if off >= int64(len(vd.cache)) {
		return 0, io.EOF
	}

	n := copy(p, vd.cache[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Sync flushes dirty cache to durable media.
func (vd *VirtualDisk) Sync() error {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	if vd.closed {
		return ErrDiskClosed
	}

	vd.durable = make([]byte, len(vd.cache))
	copy(vd.durable, vd.cache)
	vd.dirty = false
	return nil
}

// PowerCut simulates instantaneous power loss.
// Any data written after the last Sync() is reverted to the durable state!
func (vd *VirtualDisk) PowerCut() {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	// Discard dirty page cache
	vd.cache = make([]byte, len(vd.durable))
	copy(vd.cache, vd.durable)
	vd.dirty = false
}

// InjectTornWrite arms the disk to perform a partial write on the next WriteAt call.
func (vd *VirtualDisk) InjectTornWrite(fraction float64) {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	vd.writeFault = true
	vd.tornFraction = fraction
}

// CorruptSector flips bits at offset for specified length.
func (vd *VirtualDisk) CorruptSector(off int64, length int) {
	vd.mu.Lock()
	defer vd.mu.Unlock()

	for i := int(off); i < int(off)+length && i < len(vd.cache); i++ {
		vd.cache[i] ^= 0xAA
	}
	for i := int(off); i < int(off)+length && i < len(vd.durable); i++ {
		vd.durable[i] ^= 0xAA
	}
}

// Size returns total cached bytes.
func (vd *VirtualDisk) Size() int64 {
	vd.mu.RLock()
	defer vd.mu.RUnlock()
	return int64(len(vd.cache))
}

// Close closes the virtual disk.
func (vd *VirtualDisk) Close() error {
	vd.mu.Lock()
	defer vd.mu.Unlock()
	vd.closed = true
	return nil
}
