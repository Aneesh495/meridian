package protocol

import (
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
)

// RaftLogEntry represents an individual replicated command.
type RaftLogEntry struct {
	Term  core.Term
	Index core.Index
	Type  core.OpType
	Data  []byte
}

// RequestVoteArgs holds parameters for Raft election votes.
type RequestVoteArgs struct {
	Term         core.Term
	CandidateID  core.NodeID
	LastLogIndex core.Index
	LastLogTerm  core.Term
	IsPreVote    bool
}

// RequestVoteReply holds the response to a vote request.
type RequestVoteReply struct {
	Term        core.Term
	VoteGranted bool
	LeaderID    core.NodeID
}

// AppendEntriesArgs holds log entries replicated from leader to followers.
type AppendEntriesArgs struct {
	Term         core.Term
	LeaderID     core.NodeID
	PrevLogIndex core.Index
	PrevLogTerm  core.Term
	LeaderCommit core.Index
	Entries      []RaftLogEntry
}

// AppendEntriesReply holds the response to AppendEntries.
type AppendEntriesReply struct {
	Term         core.Term
	Success      bool
	MatchIndex   core.Index
	RejectReason string
}

// InstallSnapshotArgs carries chunks of a state machine snapshot.
type InstallSnapshotArgs struct {
	Term              core.Term
	LeaderID          core.NodeID
	LastIncludedIndex core.Index
	LastIncludedTerm  core.Term
	Offset            uint64
	Data              []byte
	Done              bool
}

// InstallSnapshotReply confirms receipt of snapshot chunks.
type InstallSnapshotReply struct {
	Term          core.Term
	BytesAccepted uint64
}

// Mutation represents a key-value write operation.
type Mutation struct {
	Op    core.OpType
	Key   []byte
	Value []byte
}

// PrewriteRequest initiates Phase 1 of distributed 2PC.
type PrewriteRequest struct {
	TxnID       core.TxnID
	PrimaryLock []byte
	Mutations   []Mutation
	LockTTL     uint64
	StartTS     hlc.Timestamp
	MinCommitTS hlc.Timestamp
}

// PrewriteResponse returns the result of Phase 1 prewrite.
type PrewriteResponse struct {
	Success bool
	Error   string
}

// CommitRequest initiates Phase 2 commit in 2PC.
type CommitRequest struct {
	TxnID    core.TxnID
	CommitTS hlc.Timestamp
	Keys     [][]byte
}

// CommitResponse confirms Phase 2 commit.
type CommitResponse struct {
	Success bool
	Error   string
}

// RollbackRequest aborts an active transaction and releases locks.
type RollbackRequest struct {
	TxnID core.TxnID
	Keys  [][]byte
}

// RollbackResponse confirms rollback.
type RollbackResponse struct {
	Success bool
	Error   string
}

// KVGetRequest retrieves a key at a specific read snapshot timestamp.
type KVGetRequest struct {
	Key    []byte
	ReadTS hlc.Timestamp
}

// KVGetResponse returns the retrieved value.
type KVGetResponse struct {
	Value []byte
	Found bool
	TS    hlc.Timestamp
	Error string
}

// KVPutRequest stores a key-value pair.
type KVPutRequest struct {
	Key   []byte
	Value []byte
	TS    hlc.Timestamp
}

// KVPutResponse confirms write.
type KVPutResponse struct {
	Success bool
	Error   string
}

// EncodeRequestVoteArgs serializes RequestVoteArgs.
func EncodeRequestVoteArgs(args *RequestVoteArgs) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteUint64(uint64(args.Term))
	buf.WriteUint64(uint64(args.CandidateID))
	buf.WriteUint64(uint64(args.LastLogIndex))
	buf.WriteUint64(uint64(args.LastLogTerm))
	if args.IsPreVote {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeRequestVoteArgs deserializes RequestVoteArgs.
func DecodeRequestVoteArgs(data []byte) (*RequestVoteArgs, error) {
	r := core.WrapByteBuffer(data)
	term, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	cand, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	lastIdx, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	lastTerm, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	preVoteByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	return &RequestVoteArgs{
		Term:         core.Term(term),
		CandidateID:  core.NodeID(cand),
		LastLogIndex: core.Index(lastIdx),
		LastLogTerm:  core.Term(lastTerm),
		IsPreVote:    preVoteByte == 1,
	}, nil
}

// EncodeRequestVoteReply serializes RequestVoteReply.
func EncodeRequestVoteReply(reply *RequestVoteReply) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteUint64(uint64(reply.Term))
	if reply.VoteGranted {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteUint64(uint64(reply.LeaderID))

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeRequestVoteReply deserializes RequestVoteReply.
func DecodeRequestVoteReply(data []byte) (*RequestVoteReply, error) {
	r := core.WrapByteBuffer(data)
	term, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	grantedByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	leader, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	return &RequestVoteReply{
		Term:        core.Term(term),
		VoteGranted: grantedByte == 1,
		LeaderID:    core.NodeID(leader),
	}, nil
}

// EncodeAppendEntriesArgs serializes AppendEntriesArgs.
func EncodeAppendEntriesArgs(args *AppendEntriesArgs) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteUint64(uint64(args.Term))
	buf.WriteUint64(uint64(args.LeaderID))
	buf.WriteUint64(uint64(args.PrevLogIndex))
	buf.WriteUint64(uint64(args.PrevLogTerm))
	buf.WriteUint64(uint64(args.LeaderCommit))
	buf.WriteUvarint(uint64(len(args.Entries)))

	for _, entry := range args.Entries {
		buf.WriteUint64(uint64(entry.Term))
		buf.WriteUint64(uint64(entry.Index))
		buf.WriteByte(byte(entry.Type))
		buf.WriteBytes(entry.Data)
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeAppendEntriesArgs deserializes AppendEntriesArgs.
func DecodeAppendEntriesArgs(data []byte) (*AppendEntriesArgs, error) {
	r := core.WrapByteBuffer(data)
	term, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	leader, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	prevIdx, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	prevTerm, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	leaderCommit, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	numEntries, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	entries := make([]RaftLogEntry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		eTerm, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}
		eIdx, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}
		opByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		eData, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		entries[i] = RaftLogEntry{
			Term:  core.Term(eTerm),
			Index: core.Index(eIdx),
			Type:  core.OpType(opByte),
			Data:  eData,
		}
	}

	return &AppendEntriesArgs{
		Term:         core.Term(term),
		LeaderID:     core.NodeID(leader),
		PrevLogIndex: core.Index(prevIdx),
		PrevLogTerm:  core.Term(prevTerm),
		LeaderCommit: core.Index(leaderCommit),
		Entries:      entries,
	}, nil
}

// EncodeAppendEntriesReply serializes AppendEntriesReply.
func EncodeAppendEntriesReply(reply *AppendEntriesReply) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteUint64(uint64(reply.Term))
	if reply.Success {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteUint64(uint64(reply.MatchIndex))
	buf.WriteString(reply.RejectReason)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeAppendEntriesReply deserializes AppendEntriesReply.
func DecodeAppendEntriesReply(data []byte) (*AppendEntriesReply, error) {
	r := core.WrapByteBuffer(data)
	term, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	successByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	matchIdx, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}
	reason, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	return &AppendEntriesReply{
		Term:         core.Term(term),
		Success:      successByte == 1,
		MatchIndex:   core.Index(matchIdx),
		RejectReason: reason,
	}, nil
}

// EncodePrewriteRequest serializes PrewriteRequest.
func EncodePrewriteRequest(req *PrewriteRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteString(string(req.TxnID))
	buf.WriteBytes(req.PrimaryLock)
	buf.WriteUint64(req.LockTTL)

	var tsBuf [hlc.BinarySize]byte
	req.StartTS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	req.MinCommitTS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	buf.WriteUvarint(uint64(len(req.Mutations)))
	for _, m := range req.Mutations {
		buf.WriteByte(byte(m.Op))
		buf.WriteBytes(m.Key)
		buf.WriteBytes(m.Value)
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodePrewriteRequest deserializes PrewriteRequest.
func DecodePrewriteRequest(data []byte) (*PrewriteRequest, error) {
	r := core.WrapByteBuffer(data)
	txnID, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	primaryLock, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	lockTTL, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	tsBytes1, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	startTS, err := hlc.Decode(tsBytes1)
	if err != nil {
		return nil, err
	}

	tsBytes2, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	minCommitTS, err := hlc.Decode(tsBytes2)
	if err != nil {
		return nil, err
	}

	numMuts, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	mutations := make([]Mutation, numMuts)
	for i := uint64(0); i < numMuts; i++ {
		opByte, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		k, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		v, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		mutations[i] = Mutation{
			Op:    core.OpType(opByte),
			Key:   k,
			Value: v,
		}
	}

	return &PrewriteRequest{
		TxnID:       core.TxnID(txnID),
		PrimaryLock: primaryLock,
		LockTTL:     lockTTL,
		StartTS:     startTS,
		MinCommitTS: minCommitTS,
		Mutations:   mutations,
	}, nil
}
