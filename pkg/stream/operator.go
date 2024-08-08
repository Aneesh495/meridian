package stream

import (
	"errors"
	"io"
)

var (
	ErrEndOfStream = errors.New("meridian/stream: end of stream reached")
)

// Operator defines the pull-based vectorized execution interface.
type Operator interface {
	Next() (*Chunk, *Watermark, error)
	Close() error
}

// SliceSource is an in-memory operator that streams static chunks.
type SliceSource struct {
	chunks []*Chunk
	idx    int
}

// NewSliceSource creates an operator yielding chunks from a slice.
func NewSliceSource(chunks []*Chunk) *SliceSource {
	return &SliceSource{
		chunks: chunks,
		idx:    0,
	}
}

func (s *SliceSource) Next() (*Chunk, *Watermark, error) {
	if s.idx >= len(s.chunks) {
		return nil, nil, io.EOF
	}
	c := s.chunks[s.idx]
	s.idx++
	return c, nil, nil
}

func (s *SliceSource) Close() error {
	return nil
}

// FilterOperator selects chunk rows matching a predicate.
type FilterOperator struct {
	child     Operator
	predicate func(c *Chunk, row int) bool
}

// NewFilterOperator creates an operator that filters chunk rows.
func NewFilterOperator(child Operator, pred func(*Chunk, int) bool) *FilterOperator {
	return &FilterOperator{
		child:     child,
		predicate: pred,
	}
}

func (f *FilterOperator) Next() (*Chunk, *Watermark, error) {
	for {
		chunk, wm, err := f.child.Next()
		if err != nil {
			return nil, wm, err
		}
		if chunk == nil {
			return nil, wm, nil
		}

		// Filter rows
		var matchingRows []int
		for row := 0; row < chunk.NumRows(); row++ {
			if f.predicate(chunk, row) {
				matchingRows = append(matchingRows, row)
			}
		}

		if len(matchingRows) == 0 {
			continue // pull next chunk
		}

		// Build filtered vectors
		filteredCols := make([]Vector, chunk.NumCols())
		for colIdx := 0; colIdx < chunk.NumCols(); colIdx++ {
			col := chunk.Column(colIdx)
			switch v := col.(type) {
			case *Int64Vector:
				newV := NewInt64Vector(len(matchingRows))
				for _, r := range matchingRows {
					val, ok := v.Get(r)
					if ok {
						newV.Append(val)
					} else {
						newV.AppendNull()
					}
				}
				filteredCols[colIdx] = newV
			case *Float64Vector:
				newV := NewFloat64Vector(len(matchingRows))
				for _, r := range matchingRows {
					val, ok := v.Get(r)
					if ok {
						newV.Append(val)
					} else {
						newV.AppendNull()
					}
				}
				filteredCols[colIdx] = newV
			case *StringVector:
				newV := NewStringVector(len(matchingRows))
				for _, r := range matchingRows {
					val, ok := v.Get(r)
					if ok {
						newV.Append(val)
					} else {
						newV.AppendNull()
					}
				}
				filteredCols[colIdx] = newV
			}
		}

		filteredChunk, err := NewChunk(chunk.Schema(), filteredCols)
		if err != nil {
			return nil, nil, err
		}

		return filteredChunk, wm, nil
	}
}

func (f *FilterOperator) Close() error {
	return f.child.Close()
}
