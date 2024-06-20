package protocol

import (
	"bytes"
	"testing"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

func TestRequestVoteCodec(t *testing.T) {
	args := &RequestVoteArgs{
		Term:         12,
		CandidateID:  core.NodeID(3),
		LastLogIndex: 1050,
		LastLogTerm:  11,
		IsPreVote:    true,
	}

	encoded := EncodeRequestVoteArgs(args)
	decoded, err := DecodeRequestVoteArgs(encoded)
	if err != nil {
		t.Fatalf("failed to decode RequestVoteArgs: %v", err)
	}

	if *args != *decoded {
		t.Fatalf("mismatch: original=%+v, decoded=%+v", args, decoded)
	}
}

func TestAppendEntriesCodec(t *testing.T) {
	args := &AppendEntriesArgs{
		Term:         5,
		LeaderID:     core.NodeID(1),
		PrevLogIndex: 100,
		PrevLogTerm:  4,
		LeaderCommit: 98,
		Entries: []RaftLogEntry{
			{Term: 5, Index: 101, Type: core.OpPut, Data: []byte("k1=v1")},
			{Term: 5, Index: 102, Type: core.OpDelete, Data: []byte("k2")},
		},
	}

	encoded := EncodeAppendEntriesArgs(args)
	decoded, err := DecodeAppendEntriesArgs(encoded)
	if err != nil {
		t.Fatalf("failed to decode AppendEntriesArgs: %v", err)
	}

	if args.Term != decoded.Term || args.LeaderID != decoded.LeaderID || len(args.Entries) != len(decoded.Entries) {
		t.Fatalf("mismatch in decoded AppendEntries")
	}

	for i := range args.Entries {
		if !bytes.Equal(args.Entries[i].Data, decoded.Entries[i].Data) {
			t.Fatalf("entry data mismatch at index %d", i)
		}
	}
}

func TestPrewriteRequestCodec(t *testing.T) {
	req := &PrewriteRequest{
		TxnID:       "txn-987654321",
		PrimaryLock: []byte("accounts:001"),
		LockTTL:     5000,
		StartTS:     hlc.NewTimestamp(1000000, 1, 1),
		MinCommitTS: hlc.NewTimestamp(1000000, 2, 1),
		Mutations: []Mutation{
			{Op: core.OpPut, Key: []byte("accounts:001"), Value: []byte("950.00")},
			{Op: core.OpPut, Key: []byte("accounts:002"), Value: []byte("1050.00")},
		},
	}

	encoded := EncodePrewriteRequest(req)
	decoded, err := DecodePrewriteRequest(encoded)
	if err != nil {
		t.Fatalf("failed to decode PrewriteRequest: %v", err)
	}

	if req.TxnID != decoded.TxnID || !bytes.Equal(req.PrimaryLock, decoded.PrimaryLock) {
		t.Fatalf("prewrite decoded mismatch")
	}
	if len(req.Mutations) != len(decoded.Mutations) {
		t.Fatalf("mutations count mismatch")
	}
	if !bytes.Equal(req.Mutations[0].Value, decoded.Mutations[0].Value) {
		t.Fatalf("mutation value mismatch")
	}
}
