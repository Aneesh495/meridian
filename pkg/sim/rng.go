package sim

import (
	"math/bits"
	"sync"
	"time"
)

// RNG implements the Xoshiro256++ 64-bit pseudo-random generator.
// It provides high-speed, statistically robust pseudo-random numbers
// allowing deterministic reproduction of distributed simulation runs.
type RNG struct {
	mu    sync.Mutex
	seed  uint64
	s     [4]uint64
}

// NewRNG initializes an RNG using a 64-bit seed expanded via SplitMix64.
func NewRNG(seed uint64) *RNG {
	if seed == 0 {
		seed = 0x853c49e6748fea9b
	}

	rng := &RNG{seed: seed}
	sm := seed
	for i := 0; i < 4; i++ {
		sm += 0x9e3779b97f4a7c15
		z := sm
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		rng.s[i] = z ^ (z >> 31)
	}

	return rng
}

// Seed returns the initial 64-bit seed.
func (r *RNG) Seed() uint64 {
	return r.seed
}

// Uint64 generates the next pseudo-random 64-bit integer.
func (r *RNG) Uint64() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := bits.RotateLeft64(r.s[0]+r.s[3], 23) + r.s[0]
	t := r.s[1] << 17

	r.s[2] ^= r.s[0]
	r.s[3] ^= r.s[1]
	r.s[1] ^= r.s[2]
	r.s[0] ^= r.s[3]

	r.s[2] ^= t
	r.s[3] = bits.RotateLeft64(r.s[3], 45)

	return result
}

// Intn returns a uniform integer in [0, n).
func (r *RNG) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

// Float64 returns a uniform float in [0.0, 1.0).
func (r *RNG) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

// Bool returns a random boolean with probability p of being true.
func (r *RNG) Bool(p float64) bool {
	if p <= 0.0 {
		return false
	}
	if p >= 1.0 {
		return true
	}
	return r.Float64() < p
}

// Duration returns a duration uniformly distributed in [min, max].
func (r *RNG) Duration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	delta := max - min
	return min + time.Duration(r.Uint64()%uint64(delta+1))
}

// Perm returns a pseudo-random permutation of the integers [0, n).
func (r *RNG) Perm(n int) []int {
	p := make([]int, n)
	for i := 0; i < n; i++ {
		p[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		p[i], p[j] = p[j], p[i]
	}
	return p
}

// State returns the current internal 256-bit generator state.
func (r *RNG) State() [4]uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.s
}

// Restore resets the internal state to a saved snapshot.
func (r *RNG) Restore(s [4]uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s = s
}
