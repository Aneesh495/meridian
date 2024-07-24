package txn

import (
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
)

// ConflictAction dictates the resolution when two transactions contend for the same key lock.
type ConflictAction uint8

const (
	ActionWait ConflictAction = iota
	ActionWound
	ActionAbort
)

func (a ConflictAction) String() string {
	switch a {
	case ActionWait:
		return "WAIT"
	case ActionWound:
		return "WOUND"
	case ActionAbort:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}

// WoundWaitEngine implements the classic non-preemptive Wound-Wait deadlock prevention algorithm.
// Rule:
// Let T_req be the requesting transaction, and T_hold be the transaction currently holding the lock.
// If T_req is OLDER than T_hold (T_req.StartTS < T_hold.StartTS):
//     T_req WOUNDS T_hold (T_hold is aborted and forced to roll back).
// If T_req is YOUNGER than T_hold (T_req.StartTS >= T_hold.StartTS):
//     T_req WAITS for T_hold to finish.
// This guarantees that waiting edges in the transaction dependency graph only point from younger to older,
// mathematically preventing any cyclic dependency and eliminating deadlocks!
type WoundWaitEngine struct {
	mu           sync.RWMutex
	woundedTxns  map[core.TxnID]bool
	waitChannels map[core.TxnID]chan struct{}
}

// NewWoundWaitEngine creates a new WoundWaitEngine.
func NewWoundWaitEngine() *WoundWaitEngine {
	return &WoundWaitEngine{
		woundedTxns:  make(map[core.TxnID]bool),
		waitChannels: make(map[core.TxnID]chan struct{}),
	}
}

// EvaluateConflict decides whether the requesting transaction should wait or wound the lock holder.
func (w *WoundWaitEngine) EvaluateConflict(requester, holder *Descriptor) ConflictAction {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if requester was already wounded
	if w.woundedTxns[requester.TxnID] {
		return ActionAbort
	}

	// Older wounds younger
	if requester.StartTS.Less(holder.StartTS) {
		w.woundedTxns[holder.TxnID] = true
		// Wake up any waiters on holder
		if ch, ok := w.waitChannels[holder.TxnID]; ok {
			close(ch)
			delete(w.waitChannels, holder.TxnID)
		}
		return ActionWound
	}

	// Younger waits for older
	return ActionWait
}

// IsWounded checks if txnID has been wounded by an older transaction.
func (w *WoundWaitEngine) IsWounded(txnID core.TxnID) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.woundedTxns[txnID]
}

// CleanTxn cleans up tracking state when a transaction finishes (commits or rolls back).
func (w *WoundWaitEngine) CleanTxn(txnID core.TxnID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.woundedTxns, txnID)
	if ch, ok := w.waitChannels[txnID]; ok {
		close(ch)
		delete(w.waitChannels, txnID)
	}
}
