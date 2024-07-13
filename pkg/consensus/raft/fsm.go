package raft

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// StateMachine defines the deterministic application state engine.
type StateMachine interface {
	Apply(entry protocol.RaftLogEntry) (any, error)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// KeyValueFSM is an in-memory key-value state machine implementing StateMachine.
type KeyValueFSM struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewKeyValueFSM creates a clean KeyValueFSM.
func NewKeyValueFSM() *KeyValueFSM {
	return &KeyValueFSM{
		data: make(map[string][]byte),
	}
}

// Apply executes a replicated command.
func (fsm *KeyValueFSM) Apply(entry protocol.RaftLogEntry) (any, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	switch entry.Type {
	case core.OpPut:
		var cmd struct {
			Key   string `json:"k"`
			Value []byte `json:"v"`
		}
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			return nil, err
		}
		fsm.data[cmd.Key] = cmd.Value
		return cmd.Value, nil

	case core.OpDelete:
		var cmd struct {
			Key string `json:"k"`
		}
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			return nil, err
		}
		delete(fsm.data, cmd.Key)
		return nil, nil

	case core.OpCompareAndSwap:
		var cmd struct {
			Key      string `json:"k"`
			Expected []byte `json:"e"`
			Desired  []byte `json:"d"`
		}
		if err := json.Unmarshal(entry.Data, &cmd); err != nil {
			return nil, err
		}
		curr, exists := fsm.data[cmd.Key]
		if (!exists && len(cmd.Expected) == 0) || (exists && bytes.Equal(curr, cmd.Expected)) {
			fsm.data[cmd.Key] = cmd.Desired
			return true, nil
		}
		return false, nil
	}

	return nil, nil
}

// Get reads directly from the state machine without log replication.
func (fsm *KeyValueFSM) Get(key string) ([]byte, bool) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	val, ok := fsm.data[key]
	if !ok {
		return nil, false
	}
	res := make([]byte, len(val))
	copy(res, val)
	return res, true
}

// Snapshot serializes all state into a byte slice.
func (fsm *KeyValueFSM) Snapshot() ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return json.Marshal(fsm.data)
}

// Restore resets the state from a snapshot byte slice.
func (fsm *KeyValueFSM) Restore(snapshot []byte) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	var restored map[string][]byte
	if err := json.Unmarshal(snapshot, &restored); err != nil {
		return err
	}
	fsm.data = restored
	return nil
}
