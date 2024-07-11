package raft

import (
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

func TestRaftLogAppendAndSlice(t *testing.T) {
	l := NewLog()

	entries := []protocol.RaftLogEntry{
		{Term: 1, Index: 1, Data: []byte("cmd1")},
		{Term: 1, Index: 2, Data: []byte("cmd2")},
		{Term: 2, Index: 3, Data: []byte("cmd3")},
	}
	l.Append(entries...)

	if l.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", l.LastIndex())
	}
	if l.LastTerm() != 2 {
		t.Fatalf("expected last term 2, got %d", l.LastTerm())
	}

	term, err := l.Term(2)
	if err != nil || term != 1 {
		t.Fatalf("expected term 1 for index 2, got %d (err: %v)", term, err)
	}

	slice, err := l.Slice(2, 10)
	if err != nil || len(slice) != 2 {
		t.Fatalf("expected 2 sliced entries, got %d (err: %v)", len(slice), err)
	}
	if slice[0].Index != 2 || slice[1].Index != 3 {
		t.Fatalf("sliced entries index mismatch")
	}
}

func TestRaftLogTruncateAndCompact(t *testing.T) {
	l := NewLog()
	for i := 1; i <= 10; i++ {
		l.Append(protocol.RaftLogEntry{Term: 1, Index: core.Index(i)})
	}

	// Truncate from index 8 onwards -> remaining: 0..7
	l.Truncate(8)
	if l.LastIndex() != 7 {
		t.Fatalf("expected last index 7 after truncate, got %d", l.LastIndex())
	}

	// Compact up to index 4
	if err := l.Compact(4, 1); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if _, err := l.Term(3); err != core.ErrLogCompacted {
		t.Fatalf("expected ErrLogCompacted for index 3, got %v", err)
	}

	term4, err := l.Term(4)
	if err != nil || term4 != 1 {
		t.Fatalf("expected term 1 for compacted index 4, got %d", term4)
	}

	if l.LastIndex() != 7 {
		t.Fatalf("expected last index 7 after compaction, got %d", l.LastIndex())
	}
}
