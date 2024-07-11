package raft

import (
	"sync"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

// Log manages the sequence of replicated consensus log entries.
type Log struct {
	mu           sync.RWMutex
	entries      []protocol.RaftLogEntry
	compactIndex core.Index
	compactTerm  core.Term
	appliedIndex core.Index
	commitIndex  core.Index
}

// NewLog creates a new Log initialized with a dummy root entry at index 0.
func NewLog() *Log {
	return &Log{
		entries: []protocol.RaftLogEntry{
			{Term: 0, Index: 0},
		},
		compactIndex: 0,
		compactTerm:  0,
		appliedIndex: 0,
		commitIndex:  0,
	}
}

// LastIndex returns the index of the highest entry currently recorded.
func (l *Log) LastIndex() core.Index {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the highest entry recorded.
func (l *Log) LastTerm() core.Term {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// Term returns the term associated with the given log index.
func (l *Log) Term(idx core.Index) (core.Term, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if idx == l.compactIndex {
		return l.compactTerm, nil
	}
	if idx < l.compactIndex {
		return 0, core.ErrLogCompacted
	}

	offset := int(idx - l.compactIndex)
	if offset >= len(l.entries) {
		return 0, core.ErrNotFound
	}

	return l.entries[offset].Term, nil
}

// Append appends a slice of entries to the log.
func (l *Log) Append(entries ...protocol.RaftLogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// Truncate drops all entries starting from index onwards (used during log conflict resolution).
func (l *Log) Truncate(from core.Index) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if from <= l.compactIndex {
		return
	}

	offset := int(from - l.compactIndex)
	if offset < len(l.entries) {
		l.entries = l.entries[:offset]
	}
}

// Slice returns entries starting at fromIndex up to max entries.
func (l *Log) Slice(fromIndex core.Index, max int) ([]protocol.RaftLogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if fromIndex <= l.compactIndex {
		return nil, core.ErrLogCompacted
	}

	offset := int(fromIndex - l.compactIndex)
	if offset >= len(l.entries) {
		return nil, nil
	}

	end := offset + max
	if end > len(l.entries) {
		end = len(l.entries)
	}

	res := make([]protocol.RaftLogEntry, end-offset)
	copy(res, l.entries[offset:end])
	return res, nil
}

// Compact discards entries up to and including toIndex, updating compactIndex and compactTerm.
func (l *Log) Compact(toIndex core.Index, term core.Term) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if toIndex <= l.compactIndex {
		return nil
	}

	offset := int(toIndex - l.compactIndex)
	if offset >= len(l.entries) {
		l.entries = []protocol.RaftLogEntry{{Term: term, Index: toIndex}}
	} else {
		remaining := make([]protocol.RaftLogEntry, len(l.entries)-offset)
		copy(remaining, l.entries[offset:])
		remaining[0] = protocol.RaftLogEntry{Term: term, Index: toIndex}
		l.entries = remaining
	}

	l.compactIndex = toIndex
	l.compactTerm = term
	return nil
}
