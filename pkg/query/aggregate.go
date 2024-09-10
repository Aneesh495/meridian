package query

import (
	"fmt"

	"github.com/Aneesh495/meridian/pkg/stream"
)

// AggFunc defines the aggregation function type.
type AggFunc uint8

const (
	AggCount AggFunc = iota
	AggSum
	AggMin
	AggMax
	AggAvg
)

// AggExpr defines an aggregate function and its target expression.
type AggExpr struct {
	Func   AggFunc
	Expr   Expr
	Alias  string
	Output DataType
}

type groupAccumulator struct {
	count int64
	sumI  int64
	sumF  float64
	minI  int64
	maxI  int64
	minF  float64
	maxF  float64
	hasVal bool
}

// HashAggregateOperator performs vectorized hash group-by aggregation over record batches.
type HashAggregateOperator struct {
	input        Operator
	groupExprs   []Expr
	groupNames   []string
	aggExprs     []AggExpr
	outputSchema *Schema

	// Internal state
	groups       map[string][]any // serialized key -> group key values
	accumulators map[string][]*groupAccumulator // serialized key -> per-agg state
	outputChunks []*stream.Chunk
	chunkIdx     int
}

// NewHashAggregateOperator creates a new HashAggregateOperator.
func NewHashAggregateOperator(input Operator, groupExprs []Expr, groupNames []string, aggExprs []AggExpr, outputSchema *Schema) *HashAggregateOperator {
	return &HashAggregateOperator{
		input:        input,
		groupExprs:   groupExprs,
		groupNames:   groupNames,
		aggExprs:     aggExprs,
		outputSchema: outputSchema,
		groups:       make(map[string][]any),
		accumulators: make(map[string][]*groupAccumulator),
	}
}

func (h *HashAggregateOperator) Init() error {
	h.groups = make(map[string][]any)
	h.accumulators = make(map[string][]*groupAccumulator)
	h.outputChunks = nil
	h.chunkIdx = 0

	if err := h.input.Init(); err != nil {
		return err
	}

	// Consume all input chunks and accumulate
	for {
		chunk, err := h.input.Next()
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}

		if err := h.accumulateChunk(chunk); err != nil {
			return err
		}
	}

	// Materialize output chunks from accumulated groups
	return h.buildOutputChunks()
}

func (h *HashAggregateOperator) accumulateChunk(chunk *stream.Chunk) error {
	n := chunk.NumRows()
	if n == 0 {
		return nil
	}

	// Evaluate group-by columns
	groupCols := make([]stream.Vector, len(h.groupExprs))
	for i, expr := range h.groupExprs {
		v, err := expr.Evaluate(chunk)
		if err != nil {
			return err
		}
		groupCols[i] = v
	}

	// Evaluate agg target columns
	aggCols := make([]stream.Vector, len(h.aggExprs))
	for i, agg := range h.aggExprs {
		if agg.Expr != nil {
			v, err := agg.Expr.Evaluate(chunk)
			if err != nil {
				return err
			}
			aggCols[i] = v
		}
	}

	// Process row by row in batch
	for r := 0; r < n; r++ {
		// Build composite key
		keyVals := make([]any, len(groupCols))
		compositeKey := ""
		for cIdx, col := range groupCols {
			val := extractVectorValue(col, r)
			keyVals[cIdx] = val
			compositeKey += fmt.Sprintf("%v\x00", val)
		}

		accs, exists := h.accumulators[compositeKey]
		if !exists {
			accs = make([]*groupAccumulator, len(h.aggExprs))
			for i := range accs {
				accs[i] = &groupAccumulator{}
			}
			h.accumulators[compositeKey] = accs
			h.groups[compositeKey] = keyVals
		}

		// Update accumulators
		for aIdx, agg := range h.aggExprs {
			acc := accs[aIdx]
			col := aggCols[aIdx]

			switch agg.Func {
			case AggCount:
				if col == nil || !col.IsNull(r) {
					acc.count++
				}

			case AggSum, AggAvg:
				if col != nil && !col.IsNull(r) {
					acc.count++
					if intCol, ok := col.(*stream.Int64Vector); ok {
						v, _ := intCol.Get(r)
						acc.sumI += v
					} else if floatCol, ok := col.(*stream.Float64Vector); ok {
						v, _ := floatCol.Get(r)
						acc.sumF += v
					}
				}

			case AggMin:
				if col != nil && !col.IsNull(r) {
					if intCol, ok := col.(*stream.Int64Vector); ok {
						v, _ := intCol.Get(r)
						if !acc.hasVal || v < acc.minI {
							acc.minI = v
							acc.hasVal = true
						}
					}
				}

			case AggMax:
				if col != nil && !col.IsNull(r) {
					if intCol, ok := col.(*stream.Int64Vector); ok {
						v, _ := intCol.Get(r)
						if !acc.hasVal || v > acc.maxI {
							acc.maxI = v
							acc.hasVal = true
						}
					}
				}
			}
		}
	}

	return nil
}

func extractVectorValue(v stream.Vector, i int) any {
	if v == nil || v.IsNull(i) {
		return nil
	}
	switch vec := v.(type) {
	case *stream.Int64Vector:
		val, _ := vec.Get(i)
		return val
	case *stream.Float64Vector:
		val, _ := vec.Get(i)
		return val
	case *stream.StringVector:
		val, _ := vec.Get(i)
		return val
	case *stream.BoolVector:
		val, _ := vec.Get(i)
		return val
	}
	return nil
}

func (h *HashAggregateOperator) buildOutputChunks() error {
	totalGroups := len(h.groups)
	if totalGroups == 0 && len(h.groupExprs) == 0 {
		// Scalar aggregate with zero input rows: emit 1 row with defaults (e.g. COUNT(*)=0)
		totalGroups = 1
		compositeKey := "\x00"
		accs := make([]*groupAccumulator, len(h.aggExprs))
		for i := range accs {
			accs[i] = &groupAccumulator{}
		}
		h.accumulators[compositeKey] = accs
		h.groups[compositeKey] = make([]any, 0)
	}

	if totalGroups == 0 {
		return nil
	}

	// Prepare column vectors matching output schema
	vectors := make([]stream.Vector, h.outputSchema.FieldCount())
	for colIdx, field := range h.outputSchema.Fields {
		switch field.Type {
		case TypeInt64:
			vectors[colIdx] = stream.NewInt64Vector(totalGroups)
		case TypeFloat64:
			vectors[colIdx] = stream.NewFloat64Vector(totalGroups)
		case TypeString:
			vectors[colIdx] = stream.NewStringVector(totalGroups)
		case TypeBool:
			vectors[colIdx] = stream.NewBoolVector(totalGroups)
		}
	}

	// Populate group keys and computed aggregate values
	for key, accs := range h.accumulators {
		keyVals := h.groups[key]

		// Fill group-by columns
		for gIdx, val := range keyVals {
			appendValueToVector(vectors[gIdx], val)
		}

		// Fill aggregate columns
		for aIdx, agg := range h.aggExprs {
			targetColIdx := len(keyVals) + aIdx
			acc := accs[aIdx]

			switch agg.Func {
			case AggCount:
				vectors[targetColIdx].(*stream.Int64Vector).Append(acc.count)

			case AggSum:
				if agg.Output == TypeInt64 {
					vectors[targetColIdx].(*stream.Int64Vector).Append(acc.sumI)
				} else {
					vectors[targetColIdx].(*stream.Float64Vector).Append(acc.sumF)
				}

			case AggAvg:
				var avg float64
				if acc.count > 0 {
					if acc.sumI != 0 {
						avg = float64(acc.sumI) / float64(acc.count)
					} else {
						avg = acc.sumF / float64(acc.count)
					}
				}
				vectors[targetColIdx].(*stream.Float64Vector).Append(avg)

			case AggMin:
				vectors[targetColIdx].(*stream.Int64Vector).Append(acc.minI)

			case AggMax:
				vectors[targetColIdx].(*stream.Int64Vector).Append(acc.maxI)
			}
		}
	}

	var streamFields []stream.Field
	for _, f := range h.outputSchema.Fields {
		streamFields = append(streamFields, stream.Field{
			Name: f.Name,
			Type: stream.DataType(f.Type + 1),
		})
	}

	outChunk, err := stream.NewChunk(stream.Schema{Fields: streamFields}, vectors)
	if err != nil {
		return err
	}

	h.outputChunks = []*stream.Chunk{outChunk}
	return nil
}

func appendValueToVector(v stream.Vector, val any) {
	if val == nil {
		v.SetNull(v.Len())
		return
	}
	switch vec := v.(type) {
	case *stream.Int64Vector:
		if i, ok := val.(int64); ok {
			vec.Append(i)
		} else if i, ok := val.(int); ok {
			vec.Append(int64(i))
		}
	case *stream.Float64Vector:
		if f, ok := val.(float64); ok {
			vec.Append(f)
		}
	case *stream.StringVector:
		if s, ok := val.(string); ok {
			vec.Append(s)
		}
	case *stream.BoolVector:
		if b, ok := val.(bool); ok {
			vec.Append(b)
		}
	}
}

func (h *HashAggregateOperator) Next() (*stream.Chunk, error) {
	if h.chunkIdx >= len(h.outputChunks) {
		return nil, nil
	}
	c := h.outputChunks[h.chunkIdx]
	h.chunkIdx++
	return c, nil
}

func (h *HashAggregateOperator) Close() error {
	return h.input.Close()
}

func (h *HashAggregateOperator) Schema() *Schema {
	return h.outputSchema
}
