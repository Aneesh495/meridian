package txn

import (
	"testing"

	"github.com/Aneesh495/meridian/pkg/hlc"
)

func TestWoundWaitDeadlockPrevention(t *testing.T) {
	ww := NewWoundWaitEngine()

	t1 := &Descriptor{
		TxnID:   "txn-1-old",
		StartTS: hlc.NewTimestamp(100, 0, 1),
	}

	t2 := &Descriptor{
		TxnID:   "txn-2-young",
		StartTS: hlc.NewTimestamp(200, 0, 1),
	}

	action := ww.EvaluateConflict(t1, t2)
	if action != ActionWound {
		t.Fatalf("expected ActionWound when older contends with younger, got %v", action)
	}
	if !ww.IsWounded(t2.TxnID) {
		t.Fatalf("expected t2 to be marked as wounded")
	}

	ww.CleanTxn(t2.TxnID)
	action = ww.EvaluateConflict(t2, t1)
	if action != ActionWait {
		t.Fatalf("expected ActionWait when younger contends with older, got %v", action)
	}
	if ww.IsWounded(t1.TxnID) {
		t.Fatalf("older transaction should not be wounded")
	}
}
