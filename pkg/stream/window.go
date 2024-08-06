package stream

import (
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
)

// WindowType designates the temporal grouping strategy.
type WindowType uint8

const (
	Tumbling WindowType = iota + 1
	Sliding
	Session
)

// TimeWindow defines an explicit half-open time interval [Start, End).
type TimeWindow struct {
	Start int64 // timestamp in milliseconds
	End   int64
}

// AssignTumblingWindows computes the tumbling window for an event timestamp.
func AssignTumblingWindows(eventTimeMs int64, windowSize time.Duration) TimeWindow {
	sizeMs := windowSize.Milliseconds()
	start := eventTimeMs - (eventTimeMs % sizeMs)
	return TimeWindow{
		Start: start,
		End:   start + sizeMs,
	}
}

// AssignSlidingWindows computes the overlapping sliding windows for an event timestamp.
func AssignSlidingWindows(eventTimeMs int64, windowSize, slideStep time.Duration) []TimeWindow {
	sizeMs := windowSize.Milliseconds()
	slideMs := slideStep.Milliseconds()

	var windows []TimeWindow
	lastStart := eventTimeMs - (eventTimeMs % slideMs)
	for start := lastStart; start > eventTimeMs-sizeMs; start -= slideMs {
		if start >= 0 {
			windows = append(windows, TimeWindow{
				Start: start,
				End:   start + sizeMs,
			})
		}
	}
	return windows
}

// AggregateResult contains accumulated statistical metrics over a window.
type AggregateResult struct {
	Window TimeWindow
	Group  string
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
	Mean   float64
}

// WindowAggregator incrementally aggregates numeric events partitioned by group.
type WindowAggregator struct {
	accum map[TimeWindow]map[string]*AggregateResult
}

// NewWindowAggregator creates a clean WindowAggregator.
func NewWindowAggregator() *WindowAggregator {
	return &WindowAggregator{
		accum: make(map[TimeWindow]map[string]*AggregateResult),
	}
}

// Add accumulates an individual numeric event.
func (wa *WindowAggregator) Add(w TimeWindow, group string, val float64) {
	groupMap, ok := wa.accum[w]
	if !ok {
		groupMap = make(map[string]*AggregateResult)
		wa.accum[w] = groupMap
	}

	res, ok := groupMap[group]
	if !ok {
		res = &AggregateResult{
			Window: w,
			Group:  group,
			Count:  1,
			Sum:    val,
			Min:    val,
			Max:    val,
			Mean:   val,
		}
		groupMap[group] = res
		return
	}

	res.Count++
	res.Sum += val
	if val < res.Min {
		res.Min = val
	}
	if val > res.Max {
		res.Max = val
	}
	res.Mean = res.Sum / float64(res.Count)
}

// TriggerFlushedWindows yields and removes all completed windows whose End <= watermark.
func (wa *WindowAggregator) TriggerFlushedWindows(watermark hlc.Timestamp) []AggregateResult {
	watermarkMs := watermark.WallTime / int64(time.Millisecond)

	var emitted []AggregateResult
	var completedWindows []TimeWindow

	for w, groupMap := range wa.accum {
		if w.End <= watermarkMs {
			completedWindows = append(completedWindows, w)
			for _, res := range groupMap {
				emitted = append(emitted, *res)
			}
		}
	}

	for _, w := range completedWindows {
		delete(wa.accum, w)
	}

	return emitted
}
