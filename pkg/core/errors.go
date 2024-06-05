package core

import (
	"errors"
	"fmt"
)

// Standard sentinel errors across Meridian subsystems.
var (
	ErrNotFound               = errors.New("meridian: item not found")
	ErrKeyExists              = errors.New("meridian: key already exists")
	ErrKeyLocked              = errors.New("meridian: key is locked by active transaction")
	ErrTxnConflict            = errors.New("meridian: transaction write conflict")
	ErrTxnAborted             = errors.New("meridian: transaction was aborted")
	ErrTxnNotActive           = errors.New("meridian: transaction is not in active state")
	ErrLockTimeout            = errors.New("meridian: lock acquisition timeout")
	ErrDeadlockVictim          = errors.New("meridian: transaction wounded to prevent deadlock")
	ErrNotLeader              = errors.New("meridian: node is not the consensus leader")
	ErrLeaderLost             = errors.New("meridian: consensus leadership was lost")
	ErrStaleTerm              = errors.New("meridian: term is stale")
	ErrTermMismatch           = errors.New("meridian: term mismatch in replication log")
	ErrLogCompacted           = errors.New("meridian: requested log index has been compacted")
	ErrSnapshotInProgress     = errors.New("meridian: snapshot installation currently in progress")
	ErrWorkflowCompleted      = errors.New("meridian: workflow has already completed")
	ErrWorkflowNotFound       = errors.New("meridian: workflow execution not found")
	ErrActivityFailed         = errors.New("meridian: activity execution failed")
	ErrDeterministicViolation = errors.New("meridian: non-deterministic execution detected during replay")
	ErrPartitionUnreachable   = errors.New("meridian: target cluster partition is unreachable")
	ErrCorruptedRecord        = errors.New("meridian: corrupted storage record detected")
	ErrCRC32Mismatch          = errors.New("meridian: CRC32 checksum mismatch")
	ErrInvalidFrame           = errors.New("meridian: invalid protocol frame header")
	ErrBufferOverflow         = errors.New("meridian: buffer capacity exceeded")
	ErrRingBufferFull         = errors.New("meridian: circular ring buffer is full")
	ErrClosed                 = errors.New("meridian: subsystem closed")
)

// ErrorCode categorizes errors for client handling and wire responses.
type ErrorCode uint16

const (
	CodeOK ErrorCode = 0
	CodeNotFound ErrorCode = 404
	CodeConflict ErrorCode = 409
	CodeLocked ErrorCode = 423
	CodeNotLeader ErrorCode = 501
	CodeStaleTerm ErrorCode = 502
	CodeStorageError ErrorCode = 503
	CodeInternal ErrorCode = 500
)

// Error represents a rich structured error with context.
type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
	Shard   ShardID
	Node    NodeID
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("meridian [code=%d node=%d shard=%d]: %s: %v", e.Code, e.Node, e.Shard, e.Message, e.Cause)
	}
	return fmt.Sprintf("meridian [code=%d node=%d shard=%d]: %s", e.Code, e.Node, e.Shard, e.Message)
}

func (e *Error) Unwrap() error {
	return e.Cause
}

func (e *Error) Is(target error) bool {
	if e.Cause != nil && errors.Is(e.Cause, target) {
		return true
	}
	return false
}

// NewError creates a new Meridian error structure.
func NewError(code ErrorCode, msg string, cause error) *Error {
	return &Error{
		Code:    code,
		Message: msg,
		Cause:   cause,
	}
}
