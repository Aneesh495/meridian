package query

import (
	"encoding/json"

	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/storage/mvcc"
	"github.com/Aneesh495/meridian/pkg/stream"
)

// StorageScanOperator scans multi-version key-value records from the storage engine,
// parsing row payloads into columnar Chunk record batches.
type StorageScanOperator struct {
	storage   *storage.Engine
	prefix    []byte
	readTS    hlc.Timestamp
	schema    *Schema
	batchSize int
	iter      *mvcc.SnapshotIterator
	exhausted bool
}

// NewStorageScanOperator creates a new StorageScanOperator.
func NewStorageScanOperator(store *storage.Engine, prefix []byte, readTS hlc.Timestamp, schema *Schema, batchSize int) *StorageScanOperator {
	if batchSize <= 0 {
		batchSize = 1024
	}
	return &StorageScanOperator{
		storage:   store,
		prefix:    prefix,
		readTS:    readTS,
		schema:    schema,
		batchSize: batchSize,
	}
}

func (s *StorageScanOperator) Init() error {
	s.iter = s.storage.NewSnapshotIterator(s.prefix, s.readTS)
	s.exhausted = false
	return nil
}

func (s *StorageScanOperator) Next() (*stream.Chunk, error) {
	if s.exhausted {
		return nil, nil // EOF
	}

	vectors := make([]stream.Vector, s.schema.FieldCount())
	for i, f := range s.schema.Fields {
		switch f.Type {
		case TypeInt64:
			vectors[i] = stream.NewInt64Vector(s.batchSize)
		case TypeFloat64:
			vectors[i] = stream.NewFloat64Vector(s.batchSize)
		case TypeString:
			vectors[i] = stream.NewStringVector(s.batchSize)
		case TypeBool:
			vectors[i] = stream.NewBoolVector(s.batchSize)
		}
	}

	rowsCollected := 0

	for rowsCollected < s.batchSize {
		if !s.iter.Next() {
			s.exhausted = true
			break
		}

		entry := s.iter.Entry()
		if entry == nil {
			continue
		}

		// Parse JSON map or string payload
		var doc map[string]any
		if err := json.Unmarshal(entry.Value, &doc); err != nil {
			// Fallback: single string value in first column
			doc = map[string]any{"_key": string(entry.Key), "_val": string(entry.Value)}
		}

		for cIdx, f := range s.schema.Fields {
			val, found := doc[f.Name]
			if !found {
				vectors[cIdx].SetNull(rowsCollected)
			} else {
				appendValueToVector(vectors[cIdx], val)
			}
		}

		rowsCollected++
	}

	if rowsCollected == 0 {
		return nil, nil // EOF
	}

	var streamFields []stream.Field
	for _, f := range s.schema.Fields {
		streamFields = append(streamFields, stream.Field{
			Name: f.Name,
			Type: stream.DataType(f.Type + 1),
		})
	}

	return stream.NewChunk(stream.Schema{Fields: streamFields}, vectors)
}

func (s *StorageScanOperator) Close() error {
	s.exhausted = true
	return nil
}

func (s *StorageScanOperator) Schema() *Schema {
	return s.schema
}
