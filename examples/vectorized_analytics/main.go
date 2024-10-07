package main

import (
	"fmt"
	"os"

	"github.com/Aneesh495/meridian/pkg/query"
	"github.com/Aneesh495/meridian/pkg/stream"
)

func main() {
	fmt.Println("=== Meridian Example: Vectorized Analytical SQL Engine ===")
	fmt.Println()

	// 1. Build Trades table schema and columnar chunk
	tradesSchema := query.NewSchema(
		query.Field{Name: "trade_id", Type: query.TypeInt64},
		query.Field{Name: "symbol", Type: query.TypeString},
		query.Field{Name: "price", Type: query.TypeInt64},
		query.Field{Name: "volume", Type: query.TypeInt64},
		query.Field{Name: "venue", Type: query.TypeString},
	)

	tradeIDVec := stream.NewInt64Vector(6)
	symbolVec := stream.NewStringVector(6)
	priceVec := stream.NewInt64Vector(6)
	volVec := stream.NewInt64Vector(6)
	venueVec := stream.NewStringVector(6)

	tradeData := []struct {
		id    int64
		sym   string
		price int64
		vol   int64
		venue string
	}{
		{101, "AAPL", 182, 500, "NASDAQ"},
		{102, "MSFT", 425, 250, "NASDAQ"},
		{103, "AAPL", 185, 300, "BATS"},
		{104, "NVDA", 125, 1200, "NYSE"},
		{105, "MSFT", 430, 400, "NYSE"},
		{106, "GOOG", 175, 800, "NASDAQ"},
	}

	for _, d := range tradeData {
		tradeIDVec.Append(d.id)
		symbolVec.Append(d.sym)
		priceVec.Append(d.price)
		volVec.Append(d.vol)
		venueVec.Append(d.venue)
	}

	streamSchema := stream.Schema{
		Fields: []stream.Field{
			{Name: "trade_id", Type: stream.TypeInt64},
			{Name: "symbol", Type: stream.TypeString},
			{Name: "price", Type: stream.TypeInt64},
			{Name: "volume", Type: stream.TypeInt64},
			{Name: "venue", Type: stream.TypeString},
		},
	}

	chunk, err := stream.NewChunk(streamSchema, []stream.Vector{
		tradeIDVec, symbolVec, priceVec, volVec, venueVec,
	})
	if err != nil {
		fmt.Printf("ERROR: Failed to allocate chunk: %v\n", err)
		os.Exit(1)
	}

	// 2. Initialize QueryEngine and register tables
	engine := query.NewQueryEngine()
	engine.RegisterTable("trades", tradesSchema, func() query.Operator {
		return query.NewMemoryScanOperator(tradesSchema, []*stream.Chunk{chunk})
	})

	// 3. Execute declarative SQL query: Filtering, Projection, Sorting, and Limit
	sql1 := "SELECT symbol, price, volume FROM trades WHERE price > 180 ORDER BY price DESC LIMIT 3"
	fmt.Printf("1. Executing SQL: %s\n", sql1)

	rows1, err := engine.ExecuteRows(sql1)
	if err != nil {
		fmt.Printf("ERROR: Execution failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("   +--------+-------+--------+")
	fmt.Println("   | SYMBOL | PRICE | VOLUME |")
	fmt.Println("   +--------+-------+--------+")
	for _, row := range rows1 {
		fmt.Printf("   | %-6s | %5d | %6d |\n", row.Values[0], row.Values[1], row.Values[2])
	}
	fmt.Println("   +--------+-------+--------+")
	fmt.Println()

	// 4. Vectorized Hash Aggregate: Group by symbol, calculating COUNT, SUM(volume), and AVG(price)
	fmt.Println("2. Executing Vectorized Hash Aggregation (GROUP BY symbol)...")
	scanOp := query.NewMemoryScanOperator(tradesSchema, []*stream.Chunk{chunk})
	aggOutSchema := query.NewSchema(
		query.Field{Name: "symbol", Type: query.TypeString},
		query.Field{Name: "trade_count", Type: query.TypeInt64},
		query.Field{Name: "total_volume", Type: query.TypeInt64},
		query.Field{Name: "avg_price", Type: query.TypeFloat64},
	)

	aggOp := query.NewHashAggregateOperator(
		scanOp,
		[]query.Expr{&query.ColumnExpr{ColIndex: 1, Name: "symbol"}},
		[]string{"symbol"},
		[]query.AggExpr{
			{Func: query.AggCount, Alias: "trade_count", Output: query.TypeInt64},
			{Func: query.AggSum, Expr: &query.ColumnExpr{ColIndex: 3, Name: "volume"}, Alias: "total_volume", Output: query.TypeInt64},
			{Func: query.AggAvg, Expr: &query.ColumnExpr{ColIndex: 2, Name: "price"}, Alias: "avg_price", Output: query.TypeFloat64},
		},
		aggOutSchema,
	)

	if err := aggOp.Init(); err != nil {
		fmt.Printf("ERROR: Aggregation init failed: %v\n", err)
		os.Exit(1)
	}

	aggChunk, err := aggOp.Next()
	if err != nil {
		fmt.Printf("ERROR: Aggregation Next failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("   +--------+-------------+--------------+-----------+")
	fmt.Println("   | SYMBOL | TRADE_COUNT | TOTAL_VOLUME | AVG_PRICE |")
	fmt.Println("   +--------+-------------+--------------+-----------+")

	symCol := aggChunk.Column(0).(*stream.StringVector)
	cntCol := aggChunk.Column(1).(*stream.Int64Vector)
	volCol := aggChunk.Column(2).(*stream.Int64Vector)
	avgCol := aggChunk.Column(3).(*stream.Float64Vector)

	for r := 0; r < aggChunk.NumRows(); r++ {
		sym, _ := symCol.Get(r)
		cnt, _ := cntCol.Get(r)
		v, _ := volCol.Get(r)
		a, _ := avgCol.Get(r)
		fmt.Printf("   | %-6s | %11d | %12d | %9.2f |\n", sym, cnt, v, a)
	}
	fmt.Println("   +--------+-------------+--------------+-----------+")
	fmt.Println()

	// 5. Invariant checks
	fmt.Println("3. Verifying Invariants:")
	if len(rows1) == 3 && rows1[0].Values[0] == "MSFT" && rows1[0].Values[1] == int64(430) {
		fmt.Println("   - Sorting and limit semantics verified.")
	} else {
		fmt.Println("FAILURE: Query 1 results incorrect!")
		os.Exit(1)
	}

	if aggChunk.NumRows() == 4 {
		fmt.Println("   - Hash aggregation group cardinality verified.")
	} else {
		fmt.Println("FAILURE: Aggregation cardinality mismatch!")
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("SUCCESS: Vectorized relational query engine executed all plans smoothly!")
}
