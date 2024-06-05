package core

import (
	"fmt"
	"strconv"
)

// NodeID represents a cluster node identifier.
type NodeID uint64

func (id NodeID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

// ShardID represents an autonomous consensus partition or shard.
type ShardID uint64

func (s ShardID) String() string {
	return strconv.FormatUint(uint64(s), 10)
}

// Term represents a Raft logical consensus epoch term.
type Term uint64

// Index represents an entry position in a replicated log.
type Index uint64

// SequenceNum represents an monotonically increasing event or message sequence number.
type SequenceNum uint64

// TxnID represents a globally unique transaction identifier.
type TxnID string

func (t TxnID) String() string {
	return string(t)
}

// WorkflowID represents a unique persistent workflow identifier.
type WorkflowID string

func (w WorkflowID) String() string {
	return string(w)
}

// RunID identifies a specific execution attempt of a workflow instance.
type RunID string

func (r RunID) String() string {
	return string(r)
}

// ActivityID identifies an activity scheduled by a workflow.
type ActivityID string

func (a ActivityID) String() string {
	return string(a)
}

// NodeRole denotes the consensus state of a participant node.
type NodeRole uint8

const (
	RoleFollower NodeRole = iota
	RoleCandidate
	RoleLeader
	RoleLearner
)

func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	case RoleLearner:
		return "Learner"
	default:
		return fmt.Sprintf("NodeRole(%d)", r)
	}
}

// OpType represents a low-level mutation or query operation.
type OpType uint8

const (
	OpPut OpType = iota + 1
	OpGet
	OpDelete
	OpCompareAndSwap
	OpScan
	OpBatch
)

func (o OpType) String() string {
	switch o {
	case OpPut:
		return "PUT"
	case OpGet:
		return "GET"
	case OpDelete:
		return "DELETE"
	case OpCompareAndSwap:
		return "CAS"
	case OpScan:
		return "SCAN"
	case OpBatch:
		return "BATCH"
	default:
		return fmt.Sprintf("OpType(%d)", o)
	}
}

// TxnStatus defines the lifecycle phase of an ACID transaction.
type TxnStatus uint8

const (
	TxnPending TxnStatus = iota
	TxnActive
	TxnPrewritten
	TxnCommitted
	TxnAborted
)

func (s TxnStatus) String() string {
	switch s {
	case TxnPending:
		return "PENDING"
	case TxnActive:
		return "ACTIVE"
	case TxnPrewritten:
		return "PREWRITTEN"
	case TxnCommitted:
		return "COMMITTED"
	case TxnAborted:
		return "ABORTED"
	default:
		return fmt.Sprintf("TxnStatus(%d)", s)
	}
}

// WorkflowStatus defines the durable workflow execution state.
type WorkflowStatus uint8

const (
	WorkflowPending WorkflowStatus = iota
	WorkflowRunning
	WorkflowCompleted
	WorkflowFailed
	WorkflowTimedOut
	WorkflowCompensated
)

func (s WorkflowStatus) String() string {
	switch s {
	case WorkflowPending:
		return "PENDING"
	case WorkflowRunning:
		return "RUNNING"
	case WorkflowCompleted:
		return "COMPLETED"
	case WorkflowFailed:
		return "FAILED"
	case WorkflowTimedOut:
		return "TIMED_OUT"
	case WorkflowCompensated:
		return "COMPENSATED"
	default:
		return fmt.Sprintf("WorkflowStatus(%d)", s)
	}
}

// Closer abstracts resources that can be gracefully closed.
type Closer interface {
	Close() error
}

// Flusher abstracts state engines capable of forcing data persistence.
type Flusher interface {
	Flush() error
}

// Serializable abstracts binary encode/decode operations.
type Serializable interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(data []byte) error
}
