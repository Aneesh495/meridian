package stream

import (
	"github.com/Aneesh495/meridian/pkg/core"
)

// DataType indicates the physical data type stored in a column vector.
type DataType uint8

const (
	TypeInt64 DataType = iota + 1
	TypeFloat64
	TypeString
	TypeBool
	TypeBytes
)

// Vector represents an in-memory columnar vector with null bitmap tracking.
type Vector interface {
	Type() DataType
	Len() int
	IsNull(i int) bool
	SetNull(i int)
}

// Int64Vector holds a columnar array of 64-bit integers.
type Int64Vector struct {
	values []int64
	nulls  *core.BitSet
}

// NewInt64Vector creates a new Int64Vector.
func NewInt64Vector(capacity int) *Int64Vector {
	return &Int64Vector{
		values: make([]int64, 0, capacity),
		nulls:  core.NewBitSet(capacity),
	}
}

func (v *Int64Vector) Type() DataType { return TypeInt64 }
func (v *Int64Vector) Len() int        { return len(v.values) }
func (v *Int64Vector) IsNull(i int) bool { return v.nulls.Test(i) }
func (v *Int64Vector) SetNull(i int)     { v.nulls.Set(i) }

func (v *Int64Vector) Append(val int64) {
	v.values = append(v.values, val)
}

func (v *Int64Vector) AppendNull() {
	idx := len(v.values)
	v.values = append(v.values, 0)
	v.nulls.Set(idx)
}

func (v *Int64Vector) Get(i int) (int64, bool) {
	if i < 0 || i >= len(v.values) || v.nulls.Test(i) {
		return 0, false
	}
	return v.values[i], true
}

func (v *Int64Vector) Values() []int64 {
	return v.values
}

// Float64Vector holds a columnar array of IEEE-754 binary64 floats.
type Float64Vector struct {
	values []float64
	nulls  *core.BitSet
}

// NewFloat64Vector creates a new Float64Vector.
func NewFloat64Vector(capacity int) *Float64Vector {
	return &Float64Vector{
		values: make([]float64, 0, capacity),
		nulls:  core.NewBitSet(capacity),
	}
}

func (v *Float64Vector) Type() DataType { return TypeFloat64 }
func (v *Float64Vector) Len() int        { return len(v.values) }
func (v *Float64Vector) IsNull(i int) bool { return v.nulls.Test(i) }
func (v *Float64Vector) SetNull(i int)     { v.nulls.Set(i) }

func (v *Float64Vector) Append(val float64) {
	v.values = append(v.values, val)
}

func (v *Float64Vector) AppendNull() {
	idx := len(v.values)
	v.values = append(v.values, 0)
	v.nulls.Set(idx)
}

func (v *Float64Vector) Get(i int) (float64, bool) {
	if i < 0 || i >= len(v.values) || v.nulls.Test(i) {
		return 0, false
	}
	return v.values[i], true
}

func (v *Float64Vector) Values() []float64 {
	return v.values
}

// StringVector holds a columnar array of strings.
type StringVector struct {
	values []string
	nulls  *core.BitSet
}

// NewStringVector creates a new StringVector.
func NewStringVector(capacity int) *StringVector {
	return &StringVector{
		values: make([]string, 0, capacity),
		nulls:  core.NewBitSet(capacity),
	}
}

func (v *StringVector) Type() DataType { return TypeString }
func (v *StringVector) Len() int        { return len(v.values) }
func (v *StringVector) IsNull(i int) bool { return v.nulls.Test(i) }
func (v *StringVector) SetNull(i int)     { v.nulls.Set(i) }

func (v *StringVector) Append(val string) {
	v.values = append(v.values, val)
}

func (v *StringVector) AppendNull() {
	idx := len(v.values)
	v.values = append(v.values, "")
	v.nulls.Set(idx)
}

func (v *StringVector) Get(i int) (string, bool) {
	if i < 0 || i >= len(v.values) || v.nulls.Test(i) {
		return "", false
	}
	return v.values[i], true
}
