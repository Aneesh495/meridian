package cluster

import (
	"math"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

// SuspicionTimer manages the dynamic deadline for a suspected node before declaring it dead.
// More confirmations exponentially shorten the suspicion window.
type SuspicionTimer struct {
	mu            sync.Mutex
	nodeID        core.NodeID
	minTimeout    time.Duration
	maxTimeout    time.Duration
	clusterSize   int
	confirmations int
	timer         *time.Timer
	onDead        func(id core.NodeID)
}

// NewSuspicionTimer starts a new dynamic suspicion timer for nodeID.
func NewSuspicionTimer(id core.NodeID, clusterSize int, minTimeout, maxTimeout time.Duration, onDead func(core.NodeID)) *SuspicionTimer {
	st := &SuspicionTimer{
		nodeID:        id,
		minTimeout:    minTimeout,
		maxTimeout:    maxTimeout,
		clusterSize:   clusterSize,
		confirmations: 1,
		onDead:        onDead,
	}

	st.timer = time.AfterFunc(maxTimeout, func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		if st.onDead != nil {
			st.onDead(st.nodeID)
		}
	})

	return st
}

// Confirm registers an additional independent suspicion confirmation, reducing remaining time.
func (st *SuspicionTimer) Confirm() {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.confirmations++
	if st.clusterSize <= 2 {
		return
	}

	// Exponential decay formula: T = max(T_min, T_max - (T_max - T_min) * (log(k) / log(N)))
	ratio := math.Log(float64(st.confirmations)) / math.Log(float64(st.clusterSize))
	if ratio > 1.0 {
		ratio = 1.0
	}

	newDuration := time.Duration(float64(st.maxTimeout) - float64(st.maxTimeout-st.minTimeout)*ratio)
	if newDuration < st.minTimeout {
		newDuration = st.minTimeout
	}

	st.timer.Reset(newDuration)
}

// Cancel stops the suspicion timer (e.g. if the node refutes suspicion).
func (st *SuspicionTimer) Cancel() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.timer != nil {
		st.timer.Stop()
	}
}
