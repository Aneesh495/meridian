package stream

import (
	"fmt"

	"github.com/Aneesh495/meridian/pkg/hlc"
)

// Watermark signifies event-time progression across stream operators.
// Guarantees that no future events with event-time <= Timestamp will arrive on the stream.
type Watermark struct {
	Timestamp hlc.Timestamp
}

func (w *Watermark) String() string {
	return fmt.Sprintf("WATERMARK(%s)", w.Timestamp)
}

// WatermarkTracker tracks monotonically advancing event-time watermarks.
type WatermarkTracker struct {
	current hlc.Timestamp
}

// NewWatermarkTracker creates a new WatermarkTracker.
func NewWatermarkTracker() *WatermarkTracker {
	return &WatermarkTracker{
		current: hlc.ZeroTimestamp,
	}
}

// Observe updates the tracker with event timestamps.
func (wt *WatermarkTracker) Observe(ts hlc.Timestamp) {
	if wt.current.Less(ts) {
		wt.current = ts
	}
}

// Current returns the highest observed watermark timestamp.
func (wt *WatermarkTracker) Current() hlc.Timestamp {
	return wt.current
}
