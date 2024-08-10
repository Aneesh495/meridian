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

// EncodePrewriteResponse serializes PrewriteResponse.
func EncodePrewriteResponse(resp *PrewriteResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	if resp.Success {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteString(resp.Error)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodePrewriteResponse deserializes PrewriteResponse.
func DecodePrewriteResponse(data []byte) (*PrewriteResponse, error) {
	r := core.WrapByteBuffer(data)
	s, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &PrewriteResponse{
		Success: s == 1,
		Error:   errMsg,
	}, nil
}

// EncodeCommitRequest serializes CommitRequest.
func EncodeCommitRequest(req *CommitRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteString(string(req.TxnID))
	var tsBuf [hlc.BinarySize]byte
	req.CommitTS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	buf.WriteUvarint(uint64(len(req.Keys)))
	for _, k := range req.Keys {
		buf.WriteBytes(k)
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeCommitRequest deserializes CommitRequest.
func DecodeCommitRequest(data []byte) (*CommitRequest, error) {
	r := core.WrapByteBuffer(data)
	txnID, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	tsBytes, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	commitTS, err := hlc.Decode(tsBytes)
	if err != nil {
		return nil, err
	}
	numKeys, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, numKeys)
	for i := uint64(0); i < numKeys; i++ {
		k, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	return &CommitRequest{
		TxnID:    core.TxnID(txnID),
		CommitTS: commitTS,
		Keys:     keys,
	}, nil
}

// EncodeCommitResponse serializes CommitResponse.
func EncodeCommitResponse(resp *CommitResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	if resp.Success {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteString(resp.Error)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeCommitResponse deserializes CommitResponse.
func DecodeCommitResponse(data []byte) (*CommitResponse, error) {
	r := core.WrapByteBuffer(data)
	s, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &CommitResponse{
		Success: s == 1,
		Error:   errMsg,
	}, nil
}

// EncodeRollbackRequest serializes RollbackRequest.
func EncodeRollbackRequest(req *RollbackRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteString(string(req.TxnID))
	buf.WriteUvarint(uint64(len(req.Keys)))
	for _, k := range req.Keys {
		buf.WriteBytes(k)
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeRollbackRequest deserializes RollbackRequest.
func DecodeRollbackRequest(data []byte) (*RollbackRequest, error) {
	r := core.WrapByteBuffer(data)
	txnID, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	numKeys, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, numKeys)
	for i := uint64(0); i < numKeys; i++ {
		k, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		keys[i] = k
	}
	return &RollbackRequest{
		TxnID: core.TxnID(txnID),
		Keys:  keys,
	}, nil
}

// EncodeRollbackResponse serializes RollbackResponse.
func EncodeRollbackResponse(resp *RollbackResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	if resp.Success {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteString(resp.Error)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeRollbackResponse deserializes RollbackResponse.
func DecodeRollbackResponse(data []byte) (*RollbackResponse, error) {
	r := core.WrapByteBuffer(data)
	s, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &RollbackResponse{
		Success: s == 1,
		Error:   errMsg,
	}, nil
}

// EncodeKVGetRequest serializes KVGetRequest.
func EncodeKVGetRequest(req *KVGetRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteBytes(req.Key)
	var tsBuf [hlc.BinarySize]byte
	req.ReadTS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVGetRequest deserializes KVGetRequest.
func DecodeKVGetRequest(data []byte) (*KVGetRequest, error) {
	r := core.WrapByteBuffer(data)
	k, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	tsBytes, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	readTS, err := hlc.Decode(tsBytes)
	if err != nil {
		return nil, err
	}
	return &KVGetRequest{
		Key:    k,
		ReadTS: readTS,
	}, nil
}

// EncodeKVGetResponse serializes KVGetResponse.
func EncodeKVGetResponse(resp *KVGetResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	if resp.Found {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteBytes(resp.Value)
	var tsBuf [hlc.BinarySize]byte
	resp.TS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])
	buf.WriteString(resp.Error)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVGetResponse deserializes KVGetResponse.
func DecodeKVGetResponse(data []byte) (*KVGetResponse, error) {
	r := core.WrapByteBuffer(data)
	foundByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	val, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	tsBytes, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	ts, err := hlc.Decode(tsBytes)
	if err != nil {
		return nil, err
	}
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &KVGetResponse{
		Found: foundByte == 1,
		Value: val,
		TS:    ts,
		Error: errMsg,
	}, nil
}

// EncodeKVPutRequest serializes KVPutRequest.
func EncodeKVPutRequest(req *KVPutRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteBytes(req.Key)
	buf.WriteBytes(req.Value)
	var tsBuf [hlc.BinarySize]byte
	req.TS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVPutRequest deserializes KVPutRequest.
func DecodeKVPutRequest(data []byte) (*KVPutRequest, error) {
	r := core.WrapByteBuffer(data)
	k, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	v, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	tsBytes, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	ts, err := hlc.Decode(tsBytes)
	if err != nil {
		return nil, err
	}
	return &KVPutRequest{
		Key:   k,
		Value: v,
		TS:    ts,
	}, nil
}

// EncodeKVPutResponse serializes KVPutResponse.
func EncodeKVPutResponse(resp *KVPutResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	if resp.Success {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.WriteString(resp.Error)

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVPutResponse deserializes KVPutResponse.
func DecodeKVPutResponse(data []byte) (*KVPutResponse, error) {
	r := core.WrapByteBuffer(data)
	s, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &KVPutResponse{
		Success: s == 1,
		Error:   errMsg,
	}, nil
}

// KVScanRequest requests a range or prefix scan over multi-version keys.
type KVScanRequest struct {
	Prefix []byte
	Limit  uint32
	ReadTS hlc.Timestamp
}

// KVScanEntry is a scanned key-value pair with version timestamp.
type KVScanEntry struct {
	Key   []byte
	Value []byte
	TS    hlc.Timestamp
}

// KVScanResponse returns scanned key-value pairs.
type KVScanResponse struct {
	Entries []KVScanEntry
	Error   string
}

// EncodeKVScanRequest serializes KVScanRequest.
func EncodeKVScanRequest(req *KVScanRequest) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteBytes(req.Prefix)
	buf.WriteUint32(req.Limit)
	var tsBuf [hlc.BinarySize]byte
	req.ReadTS.Encode(tsBuf[:])
	buf.WriteRaw(tsBuf[:])

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVScanRequest deserializes KVScanRequest.
func DecodeKVScanRequest(data []byte) (*KVScanRequest, error) {
	r := core.WrapByteBuffer(data)
	prefix, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	limit, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	tsBytes, err := r.ReadRaw(hlc.BinarySize)
	if err != nil {
		return nil, err
	}
	readTS, err := hlc.Decode(tsBytes)
	if err != nil {
		return nil, err
	}
	return &KVScanRequest{
		Prefix: prefix,
		Limit:  limit,
		ReadTS: readTS,
	}, nil
}

// EncodeKVScanResponse serializes KVScanResponse.
func EncodeKVScanResponse(resp *KVScanResponse) []byte {
	buf := core.GlobalByteBufferPool.Get()
	defer core.GlobalByteBufferPool.Put(buf)

	buf.WriteString(resp.Error)
	buf.WriteUvarint(uint64(len(resp.Entries)))
	var tsBuf [hlc.BinarySize]byte
	for _, e := range resp.Entries {
		buf.WriteBytes(e.Key)
		buf.WriteBytes(e.Value)
		e.TS.Encode(tsBuf[:])
		buf.WriteRaw(tsBuf[:])
	}

	res := make([]byte, buf.Len())
	copy(res, buf.Bytes())
	return res
}

// DecodeKVScanResponse deserializes KVScanResponse.
func DecodeKVScanResponse(data []byte) (*KVScanResponse, error) {
	r := core.WrapByteBuffer(data)
	errMsg, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	numEntries, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}
	entries := make([]KVScanEntry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		k, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		v, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}
		tsBytes, err := r.ReadRaw(hlc.BinarySize)
		if err != nil {
			return nil, err
		}
		ts, err := hlc.Decode(tsBytes)
		if err != nil {
			return nil, err
		}
		entries[i] = KVScanEntry{
			Key:   k,
			Value: v,
			TS:    ts,
		}
	}
	return &KVScanResponse{
		Entries: entries,
		Error:   errMsg,
	}, nil
}
