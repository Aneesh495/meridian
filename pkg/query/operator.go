package query

import (
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/stream"
)

// Operator defines the pull-based iterator interface for vectorized query execution.
type Operator interface {
	Init() error
	Next() (*stream.Chunk, error)
	Close() error
	Schema() *Schema
}

// MemoryScanOperator emits chunks from a pre-loaded in-memory record batch sequence.
type MemoryScanOperator struct {
	schema *Schema
	chunks []*stream.Chunk
	idx    int
}

// NewMemoryScanOperator creates an in-memory table scan operator.
func NewMemoryScanOperator(schema *Schema, chunks []*stream.Chunk) *MemoryScanOperator {
	return &MemoryScanOperator{
		schema: schema,
		chunks: chunks,
	}
}

func (m *MemoryScanOperator) Init() error {
	m.idx = 0
	return nil
}

func (m *MemoryScanOperator) Next() (*stream.Chunk, error) {
	if m.idx >= len(m.chunks) {
		return nil, nil // EOF
	}
	c := m.chunks[m.idx]
	m.idx++
	return c, nil
}

func (m *MemoryScanOperator) Close() error {
	return nil
}

func (m *MemoryScanOperator) Schema() *Schema {
	return m.schema
}

// FilterOperator evaluates a boolean selection predicate on input chunks.
type FilterOperator struct {
	input     Operator
	predicate Expr
}

// NewFilterOperator creates a filtering execution operator.
func NewFilterOperator(input Operator, predicate Expr) *FilterOperator {
	return &FilterOperator{
		input:     input,
		predicate: predicate,
	}
}

func (f *FilterOperator) Init() error {
	return f.input.Init()
}

func (f *FilterOperator) Next() (*stream.Chunk, error) {
	for {
		chunk, err := f.input.Next()
		if err != nil || chunk == nil {
			return nil, err
		}

		if chunk.NumRows() == 0 {
			continue
		}

		mask, err := EvaluatePredicate(f.predicate, chunk)
		if err != nil {
			return nil, err
		}

		// Count matched rows
		matchedCount := mask.Count()
		if matchedCount == 0 {
			continue
		}

		// Filter each column vector according to the selection mask
		filteredCols := make([]stream.Vector, chunk.NumCols())
		for colIdx := 0; colIdx < chunk.NumCols(); colIdx++ {
			col := chunk.Column(colIdx)
			filteredCols[colIdx] = filterVector(col, mask, matchedCount)
		}

		var streamFields []stream.Field
		for _, f := range f.input.Schema().Fields {
			streamFields = append(streamFields, stream.Field{
				Name: f.Name,
				Type: stream.DataType(f.Type + 1),
			})
		}

		filteredChunk, err := stream.NewChunk(stream.Schema{Fields: streamFields}, filteredCols)
		if err != nil {
			return nil, err
		}
		return filteredChunk, nil
	}
}

func filterVector(v stream.Vector, mask *core.BitSet, count int) stream.Vector {
	switch vec := v.(type) {
	case *stream.Int64Vector:
		out := stream.NewInt64Vector(count)
		for i := 0; i < vec.Len(); i++ {
			if mask.Test(i) {
				if vec.IsNull(i) {
					out.AppendNull()
				} else {
					val, _ := vec.Get(i)
					out.Append(val)
				}
			}
		}
		return out

	case *stream.Float64Vector:
		out := stream.NewFloat64Vector(count)
		for i := 0; i < vec.Len(); i++ {
			if mask.Test(i) {
				if vec.IsNull(i) {
					out.AppendNull()
				} else {
					val, _ := vec.Get(i)
					out.Append(val)
				}
			}
		}
		return out

	case *stream.StringVector:
		out := stream.NewStringVector(count)
		for i := 0; i < vec.Len(); i++ {
			if mask.Test(i) {
				if vec.IsNull(i) {
					out.AppendNull()
				} else {
					val, _ := vec.Get(i)
					out.Append(val)
				}
			}
		}
		return out

	case *stream.BoolVector:
		out := stream.NewBoolVector(count)
		for i := 0; i < vec.Len(); i++ {
			if mask.Test(i) {
				if vec.IsNull(i) {
					out.AppendNull()
				} else {
					val, _ := vec.Get(i)
					out.Append(val)
				}
			}
		}
		return out
	}
	return v
}

func (f *FilterOperator) Close() error {
	return f.input.Close()
}

func (f *FilterOperator) Schema() *Schema {
	return f.input.Schema()
}

// ProjectOperator evaluates expressions to produce output columns.
type ProjectOperator struct {
	input       Operator
	exprs       []Expr
	outputSchema *Schema
}

// NewProjectOperator creates a projection operator.
func NewProjectOperator(input Operator, exprs []Expr, outputSchema *Schema) *ProjectOperator {
	return &ProjectOperator{
		input:        input,
		exprs:        exprs,
		outputSchema: outputSchema,
	}
}

func (p *ProjectOperator) Init() error {
	return p.input.Init()
}

func (p *ProjectOperator) Next() (*stream.Chunk, error) {
	chunk, err := p.input.Next()
	if err != nil || chunk == nil {
		return nil, err
	}

	cols := make([]stream.Vector, len(p.exprs))
	for i, expr := range p.exprs {
		v, err := expr.Evaluate(chunk)
		if err != nil {
			return nil, err
		}
		cols[i] = v
	}

	var streamFields []stream.Field
	for _, f := range p.outputSchema.Fields {
		streamFields = append(streamFields, stream.Field{
			Name: f.Name,
			Type: stream.DataType(f.Type + 1),
		})
	}

	return stream.NewChunk(stream.Schema{Fields: streamFields}, cols)
}

func (p *ProjectOperator) Close() error {
	return p.input.Close()
}

func (p *ProjectOperator) Schema() *Schema {
	return p.outputSchema
}

// LimitOperator truncates rows to limit and offset.
type LimitOperator struct {
	input   Operator
	limit   int
	offset  int
	emitted int
	skipped int
}

// NewLimitOperator creates a limit operator.
func NewLimitOperator(input Operator, limit, offset int) *LimitOperator {
	return &LimitOperator{
		input:  input,
		limit:  limit,
		offset: offset,
	}
}

func (l *LimitOperator) Init() error {
	l.emitted = 0
	l.skipped = 0
	return l.input.Init()
}

func (l *LimitOperator) Next() (*stream.Chunk, error) {
	if l.limit > 0 && l.emitted >= l.limit {
		return nil, nil // Done
	}

	for {
		chunk, err := l.input.Next()
		if err != nil || chunk == nil {
			return nil, err
		}

		n := chunk.NumRows()
		if n == 0 {
			continue
		}

		// Handle offset
		if l.skipped < l.offset {
			needed := l.offset - l.skipped
			if n <= needed {
				l.skipped += n
				continue
			}
			// Slicing chunk
			startRow := needed
			l.skipped = l.offset

			takeRows := n - startRow
			if l.limit > 0 && l.emitted+takeRows > l.limit {
				takeRows = l.limit - l.emitted
			}

			l.emitted += takeRows
			return sliceChunk(chunk, startRow, startRow+takeRows)
		}

		takeRows := n
		if l.limit > 0 && l.emitted+takeRows > l.limit {
			takeRows = l.limit - l.emitted
		}

		if takeRows < n {
			l.emitted += takeRows
			return sliceChunk(chunk, 0, takeRows)
		}

		l.emitted += takeRows
		return chunk, nil
	}
}

func sliceChunk(c *stream.Chunk, start, end int) (*stream.Chunk, error) {
	count := end - start
	cols := make([]stream.Vector, c.NumCols())
	for i := 0; i < c.NumCols(); i++ {
		cols[i] = sliceVector(c.Column(i), start, end, count)
	}
	return stream.NewChunk(c.Schema(), cols)
}

func sliceVector(v stream.Vector, start, end, count int) stream.Vector {
	switch vec := v.(type) {
	case *stream.Int64Vector:
		out := stream.NewInt64Vector(count)
		for i := start; i < end; i++ {
			if vec.IsNull(i) {
				out.AppendNull()
			} else {
				val, _ := vec.Get(i)
				out.Append(val)
			}
		}
		return out
	case *stream.StringVector:
		out := stream.NewStringVector(count)
		for i := start; i < end; i++ {
			if vec.IsNull(i) {
				out.AppendNull()
			} else {
				val, _ := vec.Get(i)
				out.Append(val)
			}
		}
		return out
	case *stream.Float64Vector:
		out := stream.NewFloat64Vector(count)
		for i := start; i < end; i++ {
			if vec.IsNull(i) {
				out.AppendNull()
			} else {
				val, _ := vec.Get(i)
				out.Append(val)
			}
		}
		return out
	case *stream.BoolVector:
		out := stream.NewBoolVector(count)
		for i := start; i < end; i++ {
			if vec.IsNull(i) {
				out.AppendNull()
			} else {
				val, _ := vec.Get(i)
				out.Append(val)
			}
		}
		return out
	}
	return v
}

func (l *LimitOperator) Close() error {
	return l.input.Close()
}

func (l *LimitOperator) Schema() *Schema {
	return l.input.Schema()
}
