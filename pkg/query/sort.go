package query

import (
	"bytes"
	"sort"

	"github.com/Aneesh495/meridian/pkg/stream"
)

// SortDirection defines ascending or descending order.
type SortDirection uint8

const (
	SortAsc SortDirection = iota
	SortDesc
)

// SortKey specifies an ordering column, its direction, and null handling.
type SortKey struct {
	Expr       Expr
	Direction  SortDirection
	NullsFirst bool
}

// SortOperator collects all input batches and emits them sorted by the specified sort keys.
type SortOperator struct {
	input        Operator
	keys         []SortKey
	outputChunks []*stream.Chunk
	chunkIdx     int
}

// NewSortOperator creates a multi-column sort operator.
func NewSortOperator(input Operator, keys []SortKey) *SortOperator {
	return &SortOperator{
		input: input,
		keys:  keys,
	}
}

type rowLocation struct {
	chunkIdx int
	rowIdx   int
}

func (s *SortOperator) Init() error {
	s.outputChunks = nil
	s.chunkIdx = 0

	if err := s.input.Init(); err != nil {
		return err
	}

	var allChunks []*stream.Chunk
	var rowLocations []rowLocation

	for {
		chunk, err := s.input.Next()
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}
		cIdx := len(allChunks)
		allChunks = append(allChunks, chunk)
		for r := 0; r < chunk.NumRows(); r++ {
			rowLocations = append(rowLocations, rowLocation{chunkIdx: cIdx, rowIdx: r})
		}
	}

	if len(rowLocations) == 0 {
		return nil
	}

	// Pre-evaluate sort key vectors per chunk for maximum sort throughput
	keyVectors := make([][]stream.Vector, len(allChunks))
	for cIdx, chunk := range allChunks {
		keyVectors[cIdx] = make([]stream.Vector, len(s.keys))
		for kIdx, key := range s.keys {
			v, err := key.Expr.Evaluate(chunk)
			if err != nil {
				return err
			}
			keyVectors[cIdx][kIdx] = v
		}
	}

	// Sort row locations
	sort.Slice(rowLocations, func(i, j int) bool {
		loc1 := rowLocations[i]
		loc2 := rowLocations[j]

		for kIdx, key := range s.keys {
			v1 := keyVectors[loc1.chunkIdx][kIdx]
			v2 := keyVectors[loc2.chunkIdx][kIdx]

			null1 := v1.IsNull(loc1.rowIdx)
			null2 := v2.IsNull(loc2.rowIdx)

			if null1 && null2 {
				continue
			}
			if null1 {
				return key.NullsFirst
			}
			if null2 {
				return !key.NullsFirst
			}

			cmp := compareValues(v1, loc1.rowIdx, v2, loc2.rowIdx)
			if cmp == 0 {
				continue
			}

			if key.Direction == SortAsc {
				return cmp < 0
			}
			return cmp > 0
		}

		return false
	})

	// Materialize sorted output into standard-sized chunks
	batchSize := 1024
	schema := s.input.Schema()

	for offset := 0; offset < len(rowLocations); offset += batchSize {
		end := offset + batchSize
		if end > len(rowLocations) {
			end = len(rowLocations)
		}
		chunkSlice := rowLocations[offset:end]

		chunk, err := s.materializeSlice(allChunks, chunkSlice, schema)
		if err != nil {
			return err
		}
		s.outputChunks = append(s.outputChunks, chunk)
	}

	return nil
}

func compareValues(v1 stream.Vector, r1 int, v2 stream.Vector, r2 int) int {
	val1 := extractVectorValue(v1, r1)
	val2 := extractVectorValue(v2, r2)

	switch x := val1.(type) {
	case int64:
		y := val2.(int64)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0
	case float64:
		y := val2.(float64)
		if x < y {
			return -1
		} else if x > y {
			return 1
		}
		return 0
	case string:
		y := val2.(string)
		return bytes.Compare([]byte(x), []byte(y))
	case bool:
		y := val2.(bool)
		if !x && y {
			return -1
		} else if x && !y {
			return 1
		}
		return 0
	}
	return 0
}

func (s *SortOperator) materializeSlice(allChunks []*stream.Chunk, slice []rowLocation, schema *Schema) (*stream.Chunk, error) {
	numRows := len(slice)
	vectors := make([]stream.Vector, schema.FieldCount())

	for colIdx, f := range schema.Fields {
		switch f.Type {
		case TypeInt64:
			vectors[colIdx] = stream.NewInt64Vector(numRows)
		case TypeFloat64:
			vectors[colIdx] = stream.NewFloat64Vector(numRows)
		case TypeString:
			vectors[colIdx] = stream.NewStringVector(numRows)
		case TypeBool:
			vectors[colIdx] = stream.NewBoolVector(numRows)
		}
	}

	for outR, loc := range slice {
		for colIdx := 0; colIdx < schema.FieldCount(); colIdx++ {
			val := extractVectorValue(allChunks[loc.chunkIdx].Column(colIdx), loc.rowIdx)
			if val == nil {
				vectors[colIdx].SetNull(outR)
			} else {
				appendValueToVector(vectors[colIdx], val)
			}
		}
	}

	var streamFields []stream.Field
	for _, f := range schema.Fields {
		streamFields = append(streamFields, stream.Field{
			Name: f.Name,
			Type: stream.DataType(f.Type + 1),
		})
	}

	return stream.NewChunk(stream.Schema{Fields: streamFields}, vectors)
}

func (s *SortOperator) Next() (*stream.Chunk, error) {
	if s.chunkIdx >= len(s.outputChunks) {
		return nil, nil
	}
	c := s.outputChunks[s.chunkIdx]
	s.chunkIdx++
	return c, nil
}

func (s *SortOperator) Close() error {
	return s.input.Close()
}

func (s *SortOperator) Schema() *Schema {
	return s.input.Schema()
}
