package query

import (
	"fmt"
	"strings"
)

// TableProvider creates a scan operator for a named table.
type TableProvider struct {
	Schema  *Schema
	Factory func() Operator
}

// QueryEngine coordinates table registration, SQL parsing, optimization, and physical pipeline execution.
type QueryEngine struct {
	tables map[string]TableProvider
}

// NewQueryEngine creates a new query engine instance.
func NewQueryEngine() *QueryEngine {
	return &QueryEngine{
		tables: make(map[string]TableProvider),
	}
}

// RegisterTable registers a schema and scan operator factory for a table name.
func (qe *QueryEngine) RegisterTable(name string, schema *Schema, factory func() Operator) {
	qe.tables[strings.ToUpper(name)] = TableProvider{
		Schema:  schema,
		Factory: factory,
	}
}

// Execute parses and constructs a physical operator pipeline for a SQL query.
func (qe *QueryEngine) Execute(sql string) (Operator, error) {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("meridian/query: parse error: %w", err)
	}

	table, ok := qe.tables[strings.ToUpper(stmt.From)]
	if !ok {
		return nil, fmt.Errorf("meridian/query: table not found: %s", stmt.From)
	}

	var op Operator = table.Factory()

	// 1. Resolve column references in WHERE clause
	if stmt.Where != nil {
		resolvedWhere, err := resolveExpr(stmt.Where, table.Schema)
		if err != nil {
			return nil, err
		}
		op = NewFilterOperator(op, resolvedWhere)
	}

	// 2. Resolve projections and aliases
	var projExprs []Expr
	var outFields []Field

	for _, item := range stmt.Items {
		resolved, err := resolveExpr(item.Expr, table.Schema)
		if err != nil {
			return nil, err
		}
		projExprs = append(projExprs, resolved)

		// Infer output field type
		fType := TypeString
		if col, ok := resolved.(*ColumnExpr); ok && col.ColIndex >= 0 {
			fType = table.Schema.Fields[col.ColIndex].Type
		} else if lit, ok := resolved.(*LiteralExpr); ok {
			fType = lit.Type
		}

		outFields = append(outFields, Field{
			Name: item.Alias,
			Type: fType,
		})
	}

	outSchema := NewSchema(outFields...)
	op = NewProjectOperator(op, projExprs, outSchema)

	// 3. ORDER BY
	if len(stmt.OrderBy) > 0 {
		var sortKeys []SortKey
		for _, o := range stmt.OrderBy {
			colIdx := outSchema.FieldIndex(o.Column)
			if colIdx < 0 {
				return nil, fmt.Errorf("meridian/query: column %s not found in output", o.Column)
			}
			sortKeys = append(sortKeys, SortKey{
				Expr:      &ColumnExpr{ColIndex: colIdx, Name: o.Column},
				Direction: o.Direction,
			})
		}
		op = NewSortOperator(op, sortKeys)
	}

	// 4. LIMIT & OFFSET
	if stmt.Limit > 0 || stmt.Offset > 0 {
		op = NewLimitOperator(op, stmt.Limit, stmt.Offset)
	}

	if err := op.Init(); err != nil {
		return nil, err
	}

	return op, nil
}

// ExecuteRows executes SQL and materializes result rows into memory.
func (qe *QueryEngine) ExecuteRows(sql string) ([]Row, error) {
	op, err := qe.Execute(sql)
	if err != nil {
		return nil, err
	}
	defer op.Close()

	var rows []Row

	for {
		chunk, err := op.Next()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}

		for r := 0; r < chunk.NumRows(); r++ {
			vals := make([]any, chunk.NumCols())
			for c := 0; c < chunk.NumCols(); c++ {
				vals[c] = extractVectorValue(chunk.Column(c), r)
			}
			rows = append(rows, Row{Values: vals})
		}
	}

	return rows, nil
}

func resolveExpr(expr Expr, schema *Schema) (Expr, error) {
	if col, ok := expr.(*ColumnExpr); ok {
		idx := schema.FieldIndex(col.Name)
		if idx < 0 {
			return nil, fmt.Errorf("column '%s' not found in schema", col.Name)
		}
		return &ColumnExpr{ColIndex: idx, Name: col.Name}, nil
	}

	if bin, ok := expr.(*BinaryExpr); ok {
		left, err := resolveExpr(bin.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := resolveExpr(bin.Right, schema)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Left:  left,
			Op:    bin.Op,
			Right: right,
		}, nil
	}

	return expr, nil
}
