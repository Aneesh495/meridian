package stream

import (
	"errors"
	"fmt"
)

var (
	ErrLengthMismatch = errors.New("meridian/stream: column vector length mismatch in chunk")
)

// Field describes an individual typed column within a schema.
type Field struct {
	Name string
	Type DataType
}

// Schema defines the column names and data types of a stream chunk.
type Schema struct {
	Fields []Field
}

// Chunk represents a fixed-size columnar record batch passed between vectorized query operators.
type Chunk struct {
	schema  Schema
	columns []Vector
	numRows int
}

// NewChunk constructs a new columnar Chunk from vectors matching schema.
func NewChunk(schema Schema, columns []Vector) (*Chunk, error) {
	if len(columns) == 0 {
		return &Chunk{schema: schema}, nil
	}

	numRows := columns[0].Len()
	for i, col := range columns {
		if col.Len() != numRows {
			return nil, fmt.Errorf("%w: col %d has %d rows, expected %d", ErrLengthMismatch, i, col.Len(), numRows)
		}
	}

	return &Chunk{
		schema:  schema,
		columns: columns,
		numRows: numRows,
	}, nil
}

// NumRows returns the row count of the chunk.
func (c *Chunk) NumRows() int {
	return c.numRows
}

// NumCols returns the column count.
func (c *Chunk) NumCols() int {
	return len(c.columns)
}

// Column returns the vector at column index i.
func (c *Chunk) Column(i int) Vector {
	return c.columns[i]
}

// Schema returns the chunk's schema.
func (c *Chunk) Schema() Schema {
	return c.schema
}
