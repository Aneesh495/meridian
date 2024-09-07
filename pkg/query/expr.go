package query

import (
	"errors"
	"fmt"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/stream"
)

var (
	ErrTypeMismatch     = errors.New("meridian/query: expression type mismatch")
	ErrDivisionByZero   = errors.New("meridian/query: division by zero")
	ErrInvalidOperator  = errors.New("meridian/query: invalid expression operator")
)

// Op identifies arithmetic or comparison operators.
type Op string

const (
	OpAdd Op = "+"
	OpSub Op = "-"
	OpMul Op = "*"
	OpDiv Op = "/"
	OpEq  Op = "="
	OpNeq Op = "!="
	OpLt  Op = "<"
	OpLte Op = "<="
	OpGt  Op = ">"
	OpGte Op = ">="
	OpAnd Op = "AND"
	OpOr  Op = "OR"
	OpNot Op = "NOT"
)

// Expr evaluates an expression tree across a columnar Chunk.
type Expr interface {
	Evaluate(chunk *stream.Chunk) (stream.Vector, error)
	String() string
}

// LiteralExpr represents a scalar constant value.
type LiteralExpr struct {
	Value any
	Type  DataType
}

func (l *LiteralExpr) String() string {
	return fmt.Sprintf("%v", l.Value)
}

func (l *LiteralExpr) Evaluate(chunk *stream.Chunk) (stream.Vector, error) {
	n := chunk.NumRows()
	switch l.Type {
	case TypeInt64:
		val := l.Value.(int64)
		vec := stream.NewInt64Vector(n)
		for i := 0; i < n; i++ {
			vec.Append(val)
		}
		return vec, nil
	case TypeFloat64:
		val := l.Value.(float64)
		vec := stream.NewFloat64Vector(n)
		for i := 0; i < n; i++ {
			vec.Append(val)
		}
		return vec, nil
	case TypeString:
		val := l.Value.(string)
		vec := stream.NewStringVector(n)
		for i := 0; i < n; i++ {
			vec.Append(val)
		}
		return vec, nil
	case TypeBool:
		val := l.Value.(bool)
		vec := stream.NewBoolVector(n)
		for i := 0; i < n; i++ {
			vec.Append(val)
		}
		return vec, nil
	}
	return nil, ErrTypeMismatch
}

// ColumnExpr references a named column in the input Chunk schema.
type ColumnExpr struct {
	ColIndex int
	Name     string
}

func (c *ColumnExpr) String() string {
	return c.Name
}

func (c *ColumnExpr) Evaluate(chunk *stream.Chunk) (stream.Vector, error) {
	if c.ColIndex < 0 || c.ColIndex >= chunk.NumCols() {
		return nil, fmt.Errorf("meridian/query: invalid column index %d", c.ColIndex)
	}
	return chunk.Column(c.ColIndex), nil
}

// BinaryExpr evaluates an operation between two sub-expressions.
type BinaryExpr struct {
	Left  Expr
	Op    Op
	Right Expr
}

func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op, b.Right)
}

func (b *BinaryExpr) Evaluate(chunk *stream.Chunk) (stream.Vector, error) {
	leftVec, err := b.Left.Evaluate(chunk)
	if err != nil {
		return nil, err
	}
	rightVec, err := b.Right.Evaluate(chunk)
	if err != nil {
		return nil, err
	}

	n := chunk.NumRows()

	switch b.Op {
	case OpAdd, OpSub, OpMul, OpDiv:
		// Arithmetic on int64 or float64
		lInt, okL := leftVec.(*stream.Int64Vector)
		rInt, okR := rightVec.(*stream.Int64Vector)
		if okL && okR {
			res := stream.NewInt64Vector(n)
			for i := 0; i < n; i++ {
				if lInt.IsNull(i) || rInt.IsNull(i) {
					res.AppendNull()
					continue
				}
				v1, _ := lInt.Get(i)
				v2, _ := rInt.Get(i)
				switch b.Op {
				case OpAdd:
					res.Append(v1 + v2)
				case OpSub:
					res.Append(v1 - v2)
				case OpMul:
					res.Append(v1 * v2)
				case OpDiv:
					if v2 == 0 {
						return nil, ErrDivisionByZero
					}
					res.Append(v1 / v2)
				}
			}
			return res, nil
		}

	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		// Comparisons produce BoolVector
		res := stream.NewBoolVector(n)

		// Compare Int64
		lInt, okLInt := leftVec.(*stream.Int64Vector)
		rInt, okRInt := rightVec.(*stream.Int64Vector)
		if okLInt && okRInt {
			for i := 0; i < n; i++ {
				if lInt.IsNull(i) || rInt.IsNull(i) {
					res.AppendNull()
					continue
				}
				v1, _ := lInt.Get(i)
				v2, _ := rInt.Get(i)
				var match bool
				switch b.Op {
				case OpEq:
					match = (v1 == v2)
				case OpNeq:
					match = (v1 != v2)
				case OpLt:
					match = (v1 < v2)
				case OpLte:
					match = (v1 <= v2)
				case OpGt:
					match = (v1 > v2)
				case OpGte:
					match = (v1 >= v2)
				}
				res.Append(match)
			}
			return res, nil
		}

		// Compare String
		lStr, okLStr := leftVec.(*stream.StringVector)
		rStr, okRStr := rightVec.(*stream.StringVector)
		if okLStr && okRStr {
			for i := 0; i < n; i++ {
				if lStr.IsNull(i) || rStr.IsNull(i) {
					res.AppendNull()
					continue
				}
				v1, _ := lStr.Get(i)
				v2, _ := rStr.Get(i)
				var match bool
				switch b.Op {
				case OpEq:
					match = (v1 == v2)
				case OpNeq:
					match = (v1 != v2)
				case OpLt:
					match = (v1 < v2)
				case OpLte:
					match = (v1 <= v2)
				case OpGt:
					match = (v1 > v2)
				case OpGte:
					match = (v1 >= v2)
				}
				res.Append(match)
			}
			return res, nil
		}

	case OpAnd, OpOr:
		lBool, okL := leftVec.(*stream.BoolVector)
		rBool, okR := rightVec.(*stream.BoolVector)
		if okL && okR {
			res := stream.NewBoolVector(n)
			for i := 0; i < n; i++ {
				v1, _ := lBool.Get(i)
				v2, _ := rBool.Get(i)
				if b.Op == OpAnd {
					res.Append(v1 && v2)
				} else {
					res.Append(v1 || v2)
				}
			}
			return res, nil
		}
	}

	return nil, ErrTypeMismatch
}

// EvaluatePredicate evaluates a boolean expression directly to a bitset selection mask.
func EvaluatePredicate(expr Expr, chunk *stream.Chunk) (*core.BitSet, error) {
	vec, err := expr.Evaluate(chunk)
	if err != nil {
		return nil, err
	}

	boolVec, ok := vec.(*stream.BoolVector)
	if !ok {
		return nil, fmt.Errorf("meridian/query: predicate expression must return BOOL, got %v", vec.Type())
	}

	n := chunk.NumRows()
	bs := core.NewBitSet(n)
	for i := 0; i < n; i++ {
		if !boolVec.IsNull(i) {
			val, _ := boolVec.Get(i)
			if val {
				bs.Set(i)
			}
		}
	}

	return bs, nil
}
