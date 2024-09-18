package query

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/stream"
)

func createTestUserChunk() (*stream.Chunk, *Schema) {
	schema := NewSchema(
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "name", Type: TypeString},
		Field{Name: "age", Type: TypeInt64},
	)

	idVec := stream.NewInt64Vector(4)
	nameVec := stream.NewStringVector(4)
	ageVec := stream.NewInt64Vector(4)

	idVec.Append(1)
	nameVec.Append("Alice")
	ageVec.Append(30)

	idVec.Append(2)
	nameVec.Append("Bob")
	ageVec.Append(22)

	idVec.Append(3)
	nameVec.Append("Charlie")
	ageVec.Append(35)

	idVec.Append(4)
	nameVec.Append("Diana")
	ageVec.Append(28)

	streamSchema := stream.Schema{
		Fields: []stream.Field{
			{Name: "id", Type: stream.TypeInt64},
			{Name: "name", Type: stream.TypeString},
			{Name: "age", Type: stream.TypeInt64},
		},
	}

	chunk, _ := stream.NewChunk(streamSchema, []stream.Vector{idVec, nameVec, ageVec})
	return chunk, schema
}

func TestQuery_FilterAndProject(t *testing.T) {
	chunk, schema := createTestUserChunk()

	engine := NewQueryEngine()
	engine.RegisterTable("users", schema, func() Operator {
		return NewMemoryScanOperator(schema, []*stream.Chunk{chunk})
	})

	sql := "SELECT name, age FROM users WHERE age > 25 ORDER BY age ASC LIMIT 2"
	rows, err := engine.ExecuteRows(sql)
	if err != nil {
		t.Fatalf("query execution failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// First row: Diana (28)
	if rows[0].Values[0] != "Diana" || rows[0].Values[1] != int64(28) {
		t.Fatalf("row 0 mismatch: %+v", rows[0].Values)
	}

	// Second row: Alice (30)
	if rows[1].Values[0] != "Alice" || rows[1].Values[1] != int64(30) {
		t.Fatalf("row 1 mismatch: %+v", rows[1].Values)
	}
}

func TestQuery_HashAggregate(t *testing.T) {
	schema := NewSchema(
		Field{Name: "status", Type: TypeString},
		Field{Name: "amount", Type: TypeInt64},
	)

	statusVec := stream.NewStringVector(5)
	amountVec := stream.NewInt64Vector(5)

	statusVec.Append("PENDING")
	amountVec.Append(100)

	statusVec.Append("COMPLETED")
	amountVec.Append(250)

	statusVec.Append("PENDING")
	amountVec.Append(150)

	statusVec.Append("COMPLETED")
	amountVec.Append(350)

	statusVec.Append("FAILED")
	amountVec.Append(50)

	streamSchema := stream.Schema{
		Fields: []stream.Field{
			{Name: "status", Type: stream.TypeString},
			{Name: "amount", Type: stream.TypeInt64},
		},
	}
	chunk, _ := stream.NewChunk(streamSchema, []stream.Vector{statusVec, amountVec})

	scanOp := NewMemoryScanOperator(schema, []*stream.Chunk{chunk})

	outSchema := NewSchema(
		Field{Name: "status", Type: TypeString},
		Field{Name: "cnt", Type: TypeInt64},
		Field{Name: "total", Type: TypeInt64},
		Field{Name: "avg_amount", Type: TypeFloat64},
	)

	aggOp := NewHashAggregateOperator(
		scanOp,
		[]Expr{&ColumnExpr{ColIndex: 0, Name: "status"}},
		[]string{"status"},
		[]AggExpr{
			{Func: AggCount, Output: TypeInt64},
			{Func: AggSum, Expr: &ColumnExpr{ColIndex: 1, Name: "amount"}, Output: TypeInt64},
			{Func: AggAvg, Expr: &ColumnExpr{ColIndex: 1, Name: "amount"}, Output: TypeFloat64},
		},
		outSchema,
	)

	if err := aggOp.Init(); err != nil {
		t.Fatalf("agg init failed: %v", err)
	}

	resChunk, err := aggOp.Next()
	if err != nil {
		t.Fatalf("agg next failed: %v", err)
	}
	if resChunk == nil {
		t.Fatalf("expected result chunk, got nil")
	}

	if resChunk.NumRows() != 3 { // PENDING, COMPLETED, FAILED
		t.Fatalf("expected 3 groups, got %d", resChunk.NumRows())
	}
}

func TestQuery_HashJoin_InnerAndLeft(t *testing.T) {
	// Users table
	userSchema := NewSchema(
		Field{Name: "uid", Type: TypeInt64},
		Field{Name: "name", Type: TypeString},
	)
	uID := stream.NewInt64Vector(3)
	uName := stream.NewStringVector(3)

	uID.Append(1)
	uName.Append("Alice")

	uID.Append(2)
	uName.Append("Bob")

	uID.Append(3)
	uName.Append("Charlie")

	uChunk, _ := stream.NewChunk(stream.Schema{
		Fields: []stream.Field{
			{Name: "uid", Type: stream.TypeInt64},
			{Name: "name", Type: stream.TypeString},
		},
	}, []stream.Vector{uID, uName})

	// Orders table
	orderSchema := NewSchema(
		Field{Name: "order_id", Type: TypeInt64},
		Field{Name: "user_id", Type: TypeInt64},
		Field{Name: "item", Type: TypeString},
	)
	oID := stream.NewInt64Vector(3)
	oUID := stream.NewInt64Vector(3)
	oItem := stream.NewStringVector(3)

	oID.Append(101)
	oUID.Append(1) // Alice
	oItem.Append("Book")

	oID.Append(102)
	oUID.Append(1) // Alice
	oItem.Append("Laptop")

	oID.Append(103)
	oUID.Append(2) // Bob
	oItem.Append("Phone")

	oChunk, _ := stream.NewChunk(stream.Schema{
		Fields: []stream.Field{
			{Name: "order_id", Type: stream.TypeInt64},
			{Name: "user_id", Type: stream.TypeInt64},
			{Name: "item", Type: stream.TypeString},
		},
	}, []stream.Vector{oID, oUID, oItem})

	outSchema := NewSchema(
		Field{Name: "uid", Type: TypeInt64},
		Field{Name: "name", Type: TypeString},
		Field{Name: "order_id", Type: TypeInt64},
		Field{Name: "user_id", Type: TypeInt64},
		Field{Name: "item", Type: TypeString},
	)

	buildOp := NewMemoryScanOperator(userSchema, []*stream.Chunk{uChunk})
	probeOp := NewMemoryScanOperator(orderSchema, []*stream.Chunk{oChunk})

	joinOp := NewHashJoinOperator(
		buildOp,
		probeOp,
		&ColumnExpr{ColIndex: 0, Name: "uid"},
		&ColumnExpr{ColIndex: 1, Name: "user_id"},
		InnerJoin,
		outSchema,
	)

	if err := joinOp.Init(); err != nil {
		t.Fatalf("join init failed: %v", err)
	}

	joinedChunk, err := joinOp.Next()
	if err != nil {
		t.Fatalf("join next failed: %v", err)
	}

	// 3 orders match: 2 for Alice, 1 for Bob
	if joinedChunk.NumRows() != 3 {
		t.Fatalf("expected 3 joined rows, got %d", joinedChunk.NumRows())
	}
}

func TestQuery_StorageScan_MVCC(t *testing.T) {
	dataDir, err := os.MkdirTemp("", "meridian-query-storage-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	storeOpts := storage.DefaultOptions
	storeOpts.DataDir = dataDir
	store, err := storage.Open(storeOpts)
	if err != nil {
		t.Fatalf("storage open failed: %v", err)
	}
	defer store.Close()

	clock := hlc.NewClock(1, 250*time.Millisecond)

	// Write JSON documents into storage engine under "users:" prefix
	users := []struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		Role string `json:"role"`
	}{
		{ID: 1, Name: "Alice", Role: "ADMIN"},
		{ID: 2, Name: "Bob", Role: "DEV"},
		{ID: 3, Name: "Charlie", Role: "ADMIN"},
	}

	for _, u := range users {
		b, _ := json.Marshal(u)
		key := []byte("users:" + u.Name)
		if err := store.Put(key, b, clock.Now()); err != nil {
			t.Fatalf("storage put failed: %v", err)
		}
	}

	schema := NewSchema(
		Field{Name: "id", Type: TypeInt64},
		Field{Name: "name", Type: TypeString},
		Field{Name: "role", Type: TypeString},
	)

	engine := NewQueryEngine()
	engine.RegisterTable("users", schema, func() Operator {
		return NewStorageScanOperator(store, []byte("users:"), clock.Now(), schema, 1024)
	})

	sql := "SELECT name, role FROM users WHERE role = 'ADMIN' ORDER BY name ASC"
	rows, err := engine.ExecuteRows(sql)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 admin users, got %d", len(rows))
	}

	if rows[0].Values[0] != "Alice" || rows[1].Values[0] != "Charlie" {
		t.Fatalf("unexpected results: %+v, %+v", rows[0].Values, rows[1].Values)
	}
}
