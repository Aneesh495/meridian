package stream

import (
	"io"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/hlc"
)

func TestVectorizedChunkCreation(t *testing.T) {
	schema := Schema{
		Fields: []Field{
			{Name: "id", Type: TypeInt64},
			{Name: "price", Type: TypeFloat64},
			{Name: "symbol", Type: TypeString},
		},
	}

	idCol := NewInt64Vector(3)
	idCol.Append(1)
	idCol.Append(2)
	idCol.Append(3)

	priceCol := NewFloat64Vector(3)
	priceCol.Append(150.25)
	priceCol.AppendNull()
	priceCol.Append(152.00)

	symbolCol := NewStringVector(3)
	symbolCol.Append("AAPL")
	symbolCol.Append("AAPL")
	symbolCol.Append("GOOG")

	chunk, err := NewChunk(schema, []Vector{idCol, priceCol, symbolCol})
	if err != nil {
		t.Fatalf("failed to create chunk: %v", err)
	}

	if chunk.NumRows() != 3 || chunk.NumCols() != 3 {
		t.Fatalf("expected 3 rows and 3 cols, got %dx%d", chunk.NumRows(), chunk.NumCols())
	}

	// Verify null handling
	val, ok := priceCol.Get(1)
	if ok || val != 0 {
		t.Fatalf("row 1 should be null")
	}
}

func TestVectorizedFilterOperator(t *testing.T) {
	schema := Schema{
		Fields: []Field{
			{Name: "sensor_id", Type: TypeInt64},
			{Name: "reading", Type: TypeFloat64},
		},
	}

	idCol := NewInt64Vector(4)
	idCol.Append(10)
	idCol.Append(20)
	idCol.Append(30)
	idCol.Append(40)

	readingCol := NewFloat64Vector(4)
	readingCol.Append(15.5)
	readingCol.Append(98.2) // > 50
	readingCol.Append(33.0)
	readingCol.Append(75.8) // > 50

	chunk, _ := NewChunk(schema, []Vector{idCol, readingCol})
	source := NewSliceSource([]*Chunk{chunk})

	// Filter: reading > 50.0
	filter := NewFilterOperator(source, func(c *Chunk, row int) bool {
		readingV := c.Column(1).(*Float64Vector)
		val, ok := readingV.Get(row)
		return ok && val > 50.0
	})

	out, _, err := filter.Next()
	if err != nil {
		t.Fatalf("filter execution failed: %v", err)
	}

	if out.NumRows() != 2 {
		t.Fatalf("expected 2 filtered rows, got %d", out.NumRows())
	}

	outID := out.Column(0).(*Int64Vector)
	val1, _ := outID.Get(0)
	val2, _ := outID.Get(1)
	if val1 != 20 || val2 != 40 {
		t.Fatalf("expected filtered IDs [20, 40], got [%d, %d]", val1, val2)
	}

	// Next should hit EOF
	_, _, err = filter.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF after final chunk, got %v", err)
	}
}

func TestTumblingWindowAggregator(t *testing.T) {
	wa := NewWindowAggregator()
	windowSize := 100 * time.Millisecond

	// Feed 3 events into window [1000, 1100)
	w1 := AssignTumblingWindows(1020, windowSize)
	wa.Add(w1, "device-1", 10.0)
	wa.Add(w1, "device-1", 20.0)
	wa.Add(w1, "device-1", 30.0)

	// Feed 1 event into window [1100, 1200)
	w2 := AssignTumblingWindows(1150, windowSize)
	wa.Add(w2, "device-1", 50.0)

	// Advance watermark to 1100 -> only w1 should flush
	wm1 := hlc.NewTimestamp(1100*int64(time.Millisecond), 0, 1)
	emitted := wa.TriggerFlushedWindows(wm1)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 window emitted at wm=1100, got %d", len(emitted))
	}

	res := emitted[0]
	if res.Count != 3 || res.Sum != 60.0 || res.Min != 10.0 || res.Max != 30.0 || res.Mean != 20.0 {
		t.Fatalf("window statistics mismatch: %+v", res)
	}

	// Advance watermark to 1250 -> w2 should flush
	wm2 := hlc.NewTimestamp(1250*int64(time.Millisecond), 0, 1)
	emitted2 := wa.TriggerFlushedWindows(wm2)

	if len(emitted2) != 1 {
		t.Fatalf("expected 1 window emitted at wm=1250, got %d", len(emitted2))
	}
	if emitted2[0].Sum != 50.0 {
		t.Fatalf("expected w2 sum = 50.0, got %f", emitted2[0].Sum)
	}
}
