package bench_test

import (
	"testing"

	"github.com/Aneesh495/meridian/pkg/query"
	"github.com/Aneesh495/meridian/pkg/stream"
)

func createBenchmarkChunk(numRows int) (*stream.Chunk, *query.Schema) {
	schema := query.NewSchema(
		query.Field{Name: "id", Type: query.TypeInt64},
		query.Field{Name: "category", Type: query.TypeString},
		query.Field{Name: "amount", Type: query.TypeInt64},
	)

	idVec := stream.NewInt64Vector(numRows)
	catVec := stream.NewStringVector(numRows)
	amtVec := stream.NewInt64Vector(numRows)

	categories := []string{"TECH", "HEALTH", "ENERGY", "FINANCE", "CONSUMER"}

	for i := 0; i < numRows; i++ {
		idVec.Append(int64(i + 1))
		catVec.Append(categories[i%len(categories)])
		amtVec.Append(int64((i*17)%1000 + 10))
	}

	streamSchema := stream.Schema{
		Fields: []stream.Field{
			{Name: "id", Type: stream.TypeInt64},
			{Name: "category", Type: stream.TypeString},
			{Name: "amount", Type: stream.TypeInt64},
		},
	}

	chunk, _ := stream.NewChunk(streamSchema, []stream.Vector{idVec, catVec, amtVec})
	return chunk, schema
}

func BenchmarkQuery_VectorizedFilter(b *testing.B) {
	const rows = 10000
	chunk, schema := createBenchmarkChunk(rows)

	filterExpr := &query.BinaryExpr{
		Left:  &query.ColumnExpr{ColIndex: 2, Name: "amount"},
		Op:    ">",
		Right: &query.LiteralExpr{Type: query.TypeInt64, Value: int64(500)},
	}

	b.ResetTimer()
	b.SetBytes(rows * 24)

	for i := 0; i < b.N; i++ {
		scan := query.NewMemoryScanOperator(schema, []*stream.Chunk{chunk})
		filter := query.NewFilterOperator(scan, filterExpr)
		_ = filter.Init()
		for {
			out, err := filter.Next()
			if err != nil || out == nil {
				break
			}
		}
	}
}

func BenchmarkQuery_VectorizedHashAggregate(b *testing.B) {
	const rows = 10000
	chunk, schema := createBenchmarkChunk(rows)

	outSchema := query.NewSchema(
		query.Field{Name: "category", Type: query.TypeString},
		query.Field{Name: "count", Type: query.TypeInt64},
		query.Field{Name: "total_amount", Type: query.TypeInt64},
		query.Field{Name: "avg_amount", Type: query.TypeFloat64},
	)

	b.ResetTimer()
	b.SetBytes(rows * 24)

	for i := 0; i < b.N; i++ {
		scan := query.NewMemoryScanOperator(schema, []*stream.Chunk{chunk})
		agg := query.NewHashAggregateOperator(
			scan,
			[]query.Expr{&query.ColumnExpr{ColIndex: 1, Name: "category"}},
			[]string{"category"},
			[]query.AggExpr{
				{Func: query.AggCount, Alias: "count", Output: query.TypeInt64},
				{Func: query.AggSum, Expr: &query.ColumnExpr{ColIndex: 2, Name: "amount"}, Alias: "total_amount", Output: query.TypeInt64},
				{Func: query.AggAvg, Expr: &query.ColumnExpr{ColIndex: 2, Name: "amount"}, Alias: "avg_amount", Output: query.TypeFloat64},
			},
			outSchema,
		)
		_ = agg.Init()
		for {
			out, err := agg.Next()
			if err != nil || out == nil {
				break
			}
		}
	}
}

func BenchmarkQuery_VectorizedSort(b *testing.B) {
	const rows = 5000
	chunk, schema := createBenchmarkChunk(rows)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := query.NewMemoryScanOperator(schema, []*stream.Chunk{chunk})
		sortOp := query.NewSortOperator(scan, []query.SortKey{
			{Expr: &query.ColumnExpr{ColIndex: 2, Name: "amount"}, Direction: query.SortDesc},
		})
		_ = sortOp.Init()
		for {
			out, err := sortOp.Next()
			if err != nil || out == nil {
				break
			}
		}
	}
}

func BenchmarkQuery_Engine_SQLExecution(b *testing.B) {
	const rows = 1000
	chunk, schema := createBenchmarkChunk(rows)

	engine := query.NewQueryEngine()
	engine.RegisterTable("records", schema, func() query.Operator {
		return query.NewMemoryScanOperator(schema, []*stream.Chunk{chunk})
	})

	sql := "SELECT id, category, amount FROM records WHERE amount > 300 ORDER BY amount DESC LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := engine.ExecuteRows(sql)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(rows) != 10 {
			b.Fatalf("expected 10 rows, got %d", len(rows))
		}
	}
}
