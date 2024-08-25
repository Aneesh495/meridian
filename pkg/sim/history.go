package sim

import (
	"fmt"
	"sort"
	"sync"
)

// OpType identifies the kind of operation recorded in concurrent history.
type OpType string

const (
	OpRead  OpType = "READ"
	OpWrite OpType = "WRITE"
	OpCAS   OpType = "CAS"
	OpTxn   OpType = "TXN"
)

// Operation represents an invocation and corresponding return in a concurrent execution trace.
type Operation struct {
	ID        int
	ClientID  int
	Type      OpType
	Input     any
	Output    any
	StartNano int64
	EndNano   int64
	Ok        bool
}

// String provides a human-readable representation of the operation.
func (op Operation) String() string {
	status := "OK"
	if !op.Ok {
		status = "FAIL"
	}
	return fmt.Sprintf("Op[%d, Client=%d, %s, In=%v, Out=%v, [%d..%d], %s]",
		op.ID, op.ClientID, op.Type, op.Input, op.Output, op.StartNano, op.EndNano, status)
}

// History collects a complete concurrent execution trace for linearizability checking.
type History struct {
	mu         sync.Mutex
	operations []Operation
}

// NewHistory initializes an empty execution history.
func NewHistory() *History {
	return &History{
		operations: make([]Operation, 0, 512),
	}
}

// Record adds an executed operation to history.
func (h *History) Record(op Operation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	op.ID = len(h.operations)
	h.operations = append(h.operations, op)
}

// RecordRead records a read operation.
func (h *History) RecordRead(clientID int, key string, val any, startNano, endNano int64, ok bool) {
	h.Record(Operation{
		ClientID:  clientID,
		Type:      OpRead,
		Input:     key,
		Output:    val,
		StartNano: startNano,
		EndNano:   endNano,
		Ok:        ok,
	})
}

// RecordWrite records a write operation.
func (h *History) RecordWrite(clientID int, key string, val any, startNano, endNano int64, ok bool) {
	h.Record(Operation{
		ClientID:  clientID,
		Type:      OpWrite,
		Input:     struct{ Key string; Val any }{Key: key, Val: val},
		Output:    val,
		StartNano: startNano,
		EndNano:   endNano,
		Ok:        ok,
	})
}

// Operations returns a copy of all recorded operations ordered by invocation time.
func (h *History) Operations() []Operation {
	h.mu.Lock()
	defer h.mu.Unlock()

	ops := make([]Operation, len(h.operations))
	copy(ops, h.operations)

	sort.Slice(ops, func(i, j int) bool {
		if ops[i].StartNano == ops[j].StartNano {
			return ops[i].EndNano < ops[j].EndNano
		}
		return ops[i].StartNano < ops[j].StartNano
	})

	return ops
}

// Len returns the count of recorded operations.
func (h *History) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.operations)
}
