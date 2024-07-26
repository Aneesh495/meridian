package txn

import (
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// IsolationLevel defines the transactional consistency guarantee.
type IsolationLevel uint8

const (
	SnapshotIsolation IsolationLevel = iota
	Serializable
)

// Options configures transaction execution behavior.
type Options struct {
	Isolation IsolationLevel
	LockTTL   time.Duration
	Priority  int
	ReadOnly  bool
}

// DefaultOptions provides standard transaction settings.
var DefaultOptions = Options{
	Isolation: SnapshotIsolation,
	LockTTL:   5 * time.Second,
	Priority:  0,
	ReadOnly:  false,
}

// Mutation specifies a key-value modification within a transaction.
type Mutation = protocol.Mutation

// Descriptor contains the complete metadata of a distributed transaction.
type Descriptor struct {
	TxnID       core.TxnID
	Status      core.TxnStatus
	StartTS     hlc.Timestamp
	CommitTS    hlc.Timestamp
	PrimaryLock []byte
	Mutations   []Mutation
}
