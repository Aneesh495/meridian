package tui

import (
	"math"
	"strings"
	"sync"
)

var sparkRunes = []rune{' ', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

// RenderSparkline converts a slice of floating point numbers into a Unicode sparkline string.
func RenderSparkline(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}

	// Truncate or use recent values to fit width
	data := values
	if len(data) > width {
		data = data[len(data)-width:]
	}

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64

	for _, v := range data {
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}

	rangeVal := maxVal - minVal
	if rangeVal <= 0 {
		rangeVal = 1.0 // Prevent division by zero
	}

	var sb strings.Builder
	// Pad leading spaces if data is narrower than width
	if len(data) < width {
		sb.WriteString(strings.Repeat(" ", width-len(data)))
	}

	numLevels := float64(len(sparkRunes) - 1)
	for _, v := range data {
		norm := (v - minVal) / rangeVal
		if norm < 0 {
			norm = 0
		}
		if norm > 1.0 {
			norm = 1.0
		}
		idx := int(math.Round(norm * numLevels))
		if idx >= len(sparkRunes) {
			idx = len(sparkRunes) - 1
		}
		sb.WriteRune(sparkRunes[idx])
	}

	return sb.String()
}

// SparklineSeries manages a circular buffer of metric datapoints for live rendering.
type SparklineSeries struct {
	mu       sync.RWMutex
	capacity int
	data     []float64
}

// NewSparklineSeries creates a new metric history buffer.
func NewSparklineSeries(capacity int) *SparklineSeries {
	if capacity <= 0 {
		capacity = 60
	}
	return &SparklineSeries{
		capacity: capacity,
		data:     make([]float64, 0, capacity),
	}
}

// Add appends a new metric sample.
func (s *SparklineSeries) Add(val float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.data) >= s.capacity {
		s.data = append(s.data[1:], val)
	} else {
		s.data = append(s.data, val)
	}
}

// Render produces the Unicode sparkline for current series.
func (s *SparklineSeries) Render(width int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return RenderSparkline(s.data, width)
}

// Last returns the most recent sample, or 0.
func (s *SparklineSeries) Last() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.data) == 0 {
		return 0
	}
	return s.data[len(s.data)-1]
}
