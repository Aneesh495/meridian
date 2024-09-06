package query

import (
	"fmt"
)

// DataType identifies the columnar type in relation schemas.
type DataType uint8

const (
	TypeInt64 DataType = iota
	TypeFloat64
	TypeString
	TypeBool
	TypeTimestamp
)

func (t DataType) String() string {
	switch t {
	case TypeInt64:
		return "INT64"
	case TypeFloat64:
		return "FLOAT64"
	case TypeString:
		return "STRING"
	case TypeBool:
		return "BOOL"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return fmt.Sprintf("Type(%d)", t)
	}
}

// Field defines a named and typed column in a relation.
type Field struct {
	Name     string
	Type     DataType
	Nullable bool
}

// Schema represents the ordered sequence of fields defining a relation.
type Schema struct {
	Fields []Field
}

// NewSchema creates a Schema from a list of fields.
func NewSchema(fields ...Field) *Schema {
	return &Schema{Fields: fields}
}

// FieldIndex returns the index of a column by name, or -1 if not found.
func (s *Schema) FieldIndex(name string) int {
	for i, f := range s.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// FieldCount returns the number of fields in the schema.
func (s *Schema) FieldCount() int {
	return len(s.Fields)
}

// Row represents a single interpreted row of values.
type Row struct {
	Values []any
}
