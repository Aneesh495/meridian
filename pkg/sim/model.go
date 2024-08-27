package sim

import (
	"bytes"
	"fmt"
	"reflect"
)

// Model defines the sequential specification against which concurrent histories are checked.
type Model interface {
	// Init returns the starting state of the sequential specification.
	Init() any

	// Step applies an operation input to current state, returning the new state, expected output, and validity.
	Step(state any, op Operation) (newState any, expectedOutput any, ok bool)

	// Equal determines if observed output matches expected output.
	Equal(observed, expected any) bool
}

// RegisterInput models read, write, or compare-and-swap operations on a single value register.
type RegisterInput struct {
	Op       OpType
	Value    any
	Expected any // for CAS
}

// RegisterModel specifies a sequential atomic register.
type RegisterModel struct {
	InitialValue any
}

// Init returns the initial register state.
func (rm RegisterModel) Init() any {
	return rm.InitialValue
}

// Step computes sequential register transitions.
func (rm RegisterModel) Step(state any, op Operation) (any, any, bool) {
	switch op.Type {
	case OpRead:
		// Reading returns the current state unchanged
		return state, state, true

	case OpWrite:
		// Writing unconditionally replaces state with new value
		val := op.Input
		if writePayload, ok := op.Input.(struct{ Key string; Val any }); ok {
			val = writePayload.Val
		}
		return val, val, true

	case OpCAS:
		inp, ok := op.Input.(RegisterInput)
		if !ok {
			return state, nil, false
		}
		if rm.Equal(state, inp.Expected) {
			return inp.Value, true, true
		}
		return state, false, true
	}

	return state, nil, false
}

// Equal verifies output equality for registers.
func (rm RegisterModel) Equal(observed, expected any) bool {
	if observed == nil && expected == nil {
		return true
	}
	if observed == nil || expected == nil {
		return false
	}

	// Compare byte slices
	b1, ok1 := observed.([]byte)
	b2, ok2 := expected.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(b1, b2)
	}

	return reflect.DeepEqual(observed, expected)
}

// KVWriteInput represents a key-value write operation.
type KVWriteInput struct {
	Key   string
	Value []byte
}

// KVModel specifies a sequential dictionary key-value store.
type KVModel struct{}

// Init returns an empty key-value map.
func (km KVModel) Init() any {
	return make(map[string][]byte)
}

// Step applies key-value transitions to an immutable-copy dictionary.
func (km KVModel) Step(state any, op Operation) (any, any, bool) {
	store := state.(map[string][]byte)

	// Create copy of map to preserve state immutability in search tree
	nextStore := make(map[string][]byte, len(store)+1)
	for k, v := range store {
		nextStore[k] = v
	}

	switch op.Type {
	case OpRead:
		key, ok := op.Input.(string)
		if !ok {
			return state, nil, false
		}
		val, found := nextStore[key]
		if !found {
			return state, nil, true // not found
		}
		return state, val, true

	case OpWrite:
		switch inp := op.Input.(type) {
		case KVWriteInput:
			nextStore[inp.Key] = inp.Value
			return nextStore, inp.Value, true
		case struct{ Key string; Val any }:
			var b []byte
			switch v := inp.Val.(type) {
			case []byte:
				b = v
			case string:
				b = []byte(v)
			default:
				b = []byte(fmt.Sprint(v))
			}
			nextStore[inp.Key] = b
			return nextStore, b, true
		}
	}

	return state, nil, false
}

// Equal checks equality between observed and expected key-value outputs.
func (km KVModel) Equal(observed, expected any) bool {
	if observed == nil && expected == nil {
		return true
	}
	if observed == nil || expected == nil {
		return false
	}

	b1, ok1 := observed.([]byte)
	b2, ok2 := expected.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(b1, b2)
	}

	return reflect.DeepEqual(observed, expected)
}
