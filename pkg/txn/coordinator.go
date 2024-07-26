package txn

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

var (
	ErrTxnRolledBack = errors.New("meridian/txn: transaction rolled back")
)

// StorageLookup allows coordinator to resolve shard engines directly or via network.
type StorageLookup interface {
	GetParticipant(key []byte) (*Participant, error)
}

// Coordinator orchestrates distributed 2-Phase Commit transactions.
type Coordinator struct {
	clock     *hlc.Clock
	lookup    StorageLookup
	woundWait *WoundWaitEngine
}

// NewCoordinator creates a new 2PC Coordinator.
func NewCoordinator(clock *hlc.Clock, lookup StorageLookup, ww *WoundWaitEngine) *Coordinator {
	return &Coordinator{
		clock:     clock,
		lookup:    lookup,
		woundWait: ww,
	}
}

// Begin initializes a new distributed transaction.
func (c *Coordinator) Begin(opts Options) *Transaction {
	startTS := c.clock.Now()
	txnID := core.TxnID(fmt.Sprintf("txn-%d-%s", startTS.WallTime, core.GenerateID(6)))

	return &Transaction{
		coord:     c,
		txnID:     txnID,
		startTS:   startTS,
		opts:      opts,
		writes:    make(map[string]Mutation),
		reads:     make(map[string][]byte),
		status:    core.TxnActive,
	}
}

// Transaction manages the lifecycle of an individual distributed ACID transaction.
type Transaction struct {
	mu        sync.RWMutex
	coord     *Coordinator
	txnID     core.TxnID
	startTS   hlc.Timestamp
	commitTS  hlc.Timestamp
	opts      Options
	writes    map[string]Mutation
	reads     map[string][]byte
	status    core.TxnStatus
}

// TxnID returns the unique transaction identifier.
func (tx *Transaction) TxnID() core.TxnID {
	return tx.txnID
}

// Put buffers a write mutation.
func (tx *Transaction) Put(key []byte, value []byte) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writes[string(key)] = Mutation{
		Op:    core.OpPut,
		Key:   key,
		Value: value,
	}
}

// Delete buffers a deletion mutation.
func (tx *Transaction) Delete(key []byte) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writes[string(key)] = Mutation{
		Op:  core.OpDelete,
		Key: key,
	}
}

// Get reads the latest visible version of key at tx.startTS.
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	tx.mu.Lock()
	// Check uncommitted local writes first
	if mut, ok := tx.writes[string(key)]; ok {
		tx.mu.Unlock()
		if mut.Op == core.OpDelete {
			return nil, core.ErrNotFound
		}
		return mut.Value, nil
	}
	tx.mu.Unlock()

	part, err := tx.coord.lookup.GetParticipant(key)
	if err != nil {
		return nil, err
	}

	val, _, err := part.storage.Get(key, tx.startTS)
	if err != nil {
		return nil, err
	}

	tx.mu.Lock()
	tx.reads[string(key)] = val
	tx.mu.Unlock()

	return val, nil
}

// Commit executes 2-Phase Commit across all participant shards.
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.status != core.TxnActive {
		return core.ErrTxnNotActive
	}

	// Read-only transactions commit trivially
	if len(tx.writes) == 0 {
		tx.status = core.TxnCommitted
		return nil
	}

	// Verify transaction was not wounded
	if tx.coord.woundWait.IsWounded(tx.txnID) {
		tx.status = core.TxnAborted
		return core.ErrDeadlockVictim
	}

	// Group mutations by participant
	mutationsByPart := make(map[*Participant][]Mutation)
	var primaryKey []byte
	var primaryPart *Participant

	for _, mut := range tx.writes {
		part, err := tx.coord.lookup.GetParticipant(mut.Key)
		if err != nil {
			return err
		}
		mutationsByPart[part] = append(mutationsByPart[part], mut)
		if primaryKey == nil {
			primaryKey = mut.Key
			primaryPart = part
		}
	}

	// Phase 1: Prewrite primary lock first, then secondaries
	prewriteReq := &protocol.PrewriteRequest{
		TxnID:       tx.txnID,
		PrimaryLock: primaryKey,
		LockTTL:     uint64(tx.opts.LockTTL.Milliseconds()),
		StartTS:     tx.startTS,
		MinCommitTS: tx.coord.clock.Now(),
		Mutations:   mutationsByPart[primaryPart],
	}

	resp, err := primaryPart.HandlePrewrite(prewriteReq)
	if err != nil || !resp.Success {
		_ = tx.rollbackLocked(mutationsByPart)
		return fmt.Errorf("primary prewrite failed: %v (err: %s)", err, resp.Error)
	}

	// Prewrite secondaries
	for part, muts := range mutationsByPart {
		if part == primaryPart {
			continue
		}
		secReq := &protocol.PrewriteRequest{
			TxnID:       tx.txnID,
			PrimaryLock: primaryKey,
			LockTTL:     uint64(tx.opts.LockTTL.Milliseconds()),
			StartTS:     tx.startTS,
			MinCommitTS: tx.coord.clock.Now(),
			Mutations:   muts,
		}
		resp, err := part.HandlePrewrite(secReq)
		if err != nil || !resp.Success {
			_ = tx.rollbackLocked(mutationsByPart)
			return fmt.Errorf("secondary prewrite failed: %v", err)
		}
	}

	// Phase 2: Commit
	// Commit primary key first
	tx.commitTS = tx.coord.clock.Now()
	commitReq := &protocol.CommitRequest{
		TxnID:    tx.txnID,
		CommitTS: tx.commitTS,
		Keys:     [][]byte{primaryKey},
	}

	commitResp, err := primaryPart.HandleCommit(commitReq)
	if err != nil || !commitResp.Success {
		_ = tx.rollbackLocked(mutationsByPart)
		return fmt.Errorf("primary commit failed: %w", err)
	}

	// Primary committed! Transaction is now officially committed.
	tx.status = core.TxnCommitted

	// Asynchronously or sequentially commit all secondaries
	for part, muts := range mutationsByPart {
		var secKeys [][]byte
		for _, m := range muts {
			if string(m.Key) != string(primaryKey) {
				secKeys = append(secKeys, m.Key)
			}
		}
		if len(secKeys) > 0 {
			secCommitReq := &protocol.CommitRequest{
				TxnID:    tx.txnID,
				CommitTS: tx.commitTS,
				Keys:     secKeys,
			}
			_, _ = part.HandleCommit(secCommitReq)
		}
	}

	return nil
}

// Rollback aborts the transaction and releases all held locks.
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.status != core.TxnActive {
		return nil
	}

	mutationsByPart := make(map[*Participant][]Mutation)
	for _, mut := range tx.writes {
		part, err := tx.coord.lookup.GetParticipant(mut.Key)
		if err == nil {
			mutationsByPart[part] = append(mutationsByPart[part], mut)
		}
	}

	return tx.rollbackLocked(mutationsByPart)
}

func (tx *Transaction) rollbackLocked(mutationsByPart map[*Participant][]Mutation) error {
	tx.status = core.TxnAborted
	for part, muts := range mutationsByPart {
		keys := make([][]byte, len(muts))
		for i, m := range muts {
			keys[i] = m.Key
		}
		_, _ = part.HandleRollback(&protocol.RollbackRequest{
			TxnID: tx.txnID,
			Keys:  keys,
		})
	}
	return nil
}
