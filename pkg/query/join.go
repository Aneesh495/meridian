package query

import (
	"fmt"

	"github.com/Aneesh495/meridian/pkg/stream"
)

// JoinType defines relational join semantics.
type JoinType uint8

const (
	InnerJoin JoinType = iota
	LeftJoin
)

// HashJoinOperator executes a vectorized hash equi-join between two operator streams.
type HashJoinOperator struct {
	buildSide    Operator
	probeSide    Operator
	buildKey     Expr
	probeKey     Expr
	joinType     JoinType
	outputSchema *Schema

	// Build-side in-memory hash table: key string -> []buildRowRef
	buildTable   map[string][]buildRowRef
	buildChunks  []*stream.Chunk
}

type buildRowRef struct {
	chunkIdx int
	rowIdx   int
}

// NewHashJoinOperator creates a new HashJoinOperator.
func NewHashJoinOperator(buildSide, probeSide Operator, buildKey, probeKey Expr, joinType JoinType, outputSchema *Schema) *HashJoinOperator {
	return &HashJoinOperator{
		buildSide:    buildSide,
		probeSide:    probeSide,
		buildKey:     buildKey,
		probeKey:     probeKey,
		joinType:     joinType,
		outputSchema: outputSchema,
		buildTable:   make(map[string][]buildRowRef),
	}
}

func (h *HashJoinOperator) Init() error {
	h.buildTable = make(map[string][]buildRowRef)
	h.buildChunks = nil

	if err := h.buildSide.Init(); err != nil {
		return err
	}
	if err := h.probeSide.Init(); err != nil {
		return err
	}

	// 1. Build phase: consume entire buildSide and index by buildKey
	for {
		chunk, err := h.buildSide.Next()
		if err != nil {
			return err
		}
		if chunk == nil {
			break
		}

		cIdx := len(h.buildChunks)
		h.buildChunks = append(h.buildChunks, chunk)

		keyVec, err := h.buildKey.Evaluate(chunk)
		if err != nil {
			return err
		}

		for r := 0; r < chunk.NumRows(); r++ {
			if keyVec.IsNull(r) {
				continue
			}
			val := extractVectorValue(keyVec, r)
			keyStr := fmt.Sprintf("%v", val)
			h.buildTable[keyStr] = append(h.buildTable[keyStr], buildRowRef{
				chunkIdx: cIdx,
				rowIdx:   r,
			})
		}
	}

	return nil
}

// Next processes the next probe-side batch and outputs matched joined batches.
func (h *HashJoinOperator) Next() (*stream.Chunk, error) {
	for {
		probeChunk, err := h.probeSide.Next()
		if err != nil || probeChunk == nil {
			return nil, err
		}

		if probeChunk.NumRows() == 0 {
			continue
		}

		probeKeyVec, err := h.probeKey.Evaluate(probeChunk)
		if err != nil {
			return nil, err
		}

		// Calculate matches
		var matchedBuildRefs []buildRowRef
		var matchedProbeRows []int

		for pr := 0; pr < probeChunk.NumRows(); pr++ {
			if probeKeyVec.IsNull(pr) {
				if h.joinType == LeftJoin {
					matchedBuildRefs = append(matchedBuildRefs, buildRowRef{chunkIdx: -1, rowIdx: -1})
					matchedProbeRows = append(matchedProbeRows, pr)
				}
				continue
			}

			val := extractVectorValue(probeKeyVec, pr)
			keyStr := fmt.Sprintf("%v", val)

			refs, found := h.buildTable[keyStr]
			if found && len(refs) > 0 {
				for _, ref := range refs {
					matchedBuildRefs = append(matchedBuildRefs, ref)
					matchedProbeRows = append(matchedProbeRows, pr)
				}
			} else if h.joinType == LeftJoin {
				matchedBuildRefs = append(matchedBuildRefs, buildRowRef{chunkIdx: -1, rowIdx: -1})
				matchedProbeRows = append(matchedProbeRows, pr)
			}
		}

		if len(matchedBuildRefs) == 0 {
			continue
		}

		// Assemble joined chunk
		return h.assembleJoinedChunk(probeChunk, matchedBuildRefs, matchedProbeRows)
	}
}

func (h *HashJoinOperator) assembleJoinedChunk(probeChunk *stream.Chunk, buildRefs []buildRowRef, probeRows []int) (*stream.Chunk, error) {
	totalRows := len(buildRefs)
	outputCols := make([]stream.Vector, h.outputSchema.FieldCount())

	for i, f := range h.outputSchema.Fields {
		switch f.Type {
		case TypeInt64:
			outputCols[i] = stream.NewInt64Vector(totalRows)
		case TypeFloat64:
			outputCols[i] = stream.NewFloat64Vector(totalRows)
		case TypeString:
			outputCols[i] = stream.NewStringVector(totalRows)
		case TypeBool:
			outputCols[i] = stream.NewBoolVector(totalRows)
		}
	}

	buildColsCount := h.buildSide.Schema().FieldCount()
	probeColsCount := h.probeSide.Schema().FieldCount()

	for outR := 0; outR < totalRows; outR++ {
		bRef := buildRefs[outR]
		pRow := probeRows[outR]

		// Copy build side columns (first buildColsCount columns)
		for c := 0; c < buildColsCount && c < len(outputCols); c++ {
			if bRef.chunkIdx == -1 {
				outputCols[c].SetNull(outR)
			} else {
				val := extractVectorValue(h.buildChunks[bRef.chunkIdx].Column(c), bRef.rowIdx)
				appendValueToVector(outputCols[c], val)
			}
		}

		// Copy probe side columns
		for c := 0; c < probeColsCount && (buildColsCount+c) < len(outputCols); c++ {
			val := extractVectorValue(probeChunk.Column(c), pRow)
			appendValueToVector(outputCols[buildColsCount+c], val)
		}
	}

	var streamFields []stream.Field
	for _, f := range h.outputSchema.Fields {
		streamFields = append(streamFields, stream.Field{
			Name: f.Name,
			Type: stream.DataType(f.Type + 1),
		})
	}

	return stream.NewChunk(stream.Schema{Fields: streamFields}, outputCols)
}

func (h *HashJoinOperator) Close() error {
	_ = h.buildSide.Close()
	return h.probeSide.Close()
}

func (h *HashJoinOperator) Schema() *Schema {
	return h.outputSchema
}
