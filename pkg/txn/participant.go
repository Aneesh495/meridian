package txn

import (
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/storage/mvcc"
)

// Participant handles 2PC participant duties on a local shard.
type Participant struct {
	shardID   core.ShardID
	storage   *storage.Engine
	lockTable *LockTable
	woundWait *WoundWaitEngine
}

// NewParticipant creates a local shard transaction participant.
func NewParticipant(shardID core.ShardID, engine *storage.Engine, lt *LockTable, ww *WoundWaitEngine) *Participant {
	return &Participant{
		shardID:   shardID,
		storage:   engine,
		lockTable: lt,
		woundWait: ww,
	}
}

// HandlePrewrite processes Phase 1 prewrite intents for mutations assigned to this shard.
func (p *Participant) HandlePrewrite(req *protocol.PrewriteRequest) (*protocol.PrewriteResponse, error) {
	if p.woundWait.IsWounded(req.TxnID) {
		return &protocol.PrewriteResponse{
			Success: false,
			Error:   core.ErrDeadlockVictim.Error(),
		}, nil
	}

	for _, mut := range req.Mutations {
		lock := &mvcc.Lock{
			Primary:     req.PrimaryLock,
			TxnID:       req.TxnID,
			StartTS:     req.StartTS,
			MinCommitTS: req.MinCommitTS,
			TTL:         req.LockTTL,
			Op:          mut.Op,
			Value:       mut.Value,
		}

		if err := p.storage.PutLock(mut.Key, lock); err != nil {
			return &protocol.PrewriteResponse{
				Success: false,
				Error:   err.Error(),
			}, nil
		}
	}

	return &protocol.PrewriteResponse{Success: true}, nil
}

// HandleCommit processes Phase 2 commit: converts locks to committed MVCC versions.
func (p *Participant) HandleCommit(req *protocol.CommitRequest) (*protocol.CommitResponse, error) {
	for _, key := range req.Keys {
		if err := p.storage.CommitLock(key, req.CommitTS); err != nil {
			return &protocol.CommitResponse{
				Success: false,
				Error:   err.Error(),
			}, nil
		}
	}

	p.woundWait.CleanTxn(req.TxnID)
	return &protocol.CommitResponse{Success: true}, nil
}

// HandleRollback cleans up active locks when a transaction aborts.
func (p *Participant) HandleRollback(req *protocol.RollbackRequest) (*protocol.RollbackResponse, error) {
	for _, key := range req.Keys {
		_ = p.storage.RollbackLock(key)
	}

	p.woundWait.CleanTxn(req.TxnID)
	return &protocol.RollbackResponse{Success: true}, nil
}
