package protocol

import (
	"fmt"
)

// MsgType identifies the RPC message opcode in protocol frames.
type MsgType uint16

const (
	// System / Control
	MsgPing MsgType = 0x0001
	MsgPong MsgType = 0x0002
	MsgError MsgType = 0x0003

	// Raft Consensus (0x0100 - 0x01FF)
	MsgRequestVote          MsgType = 0x0101
	MsgRequestVoteResp      MsgType = 0x0102
	MsgAppendEntries        MsgType = 0x0103
	MsgAppendEntriesResp    MsgType = 0x0104
	MsgInstallSnapshot      MsgType = 0x0105
	MsgInstallSnapshotResp  MsgType = 0x0106
	MsgRaftHeartbeat        MsgType = 0x0107
	MsgRaftHeartbeatResp    MsgType = 0x0108

	// Distributed 2PC Transactions (0x0200 - 0x02FF)
	MsgTxnPrewrite          MsgType = 0x0201
	MsgTxnPrewriteResp      MsgType = 0x0202
	MsgTxnCommit            MsgType = 0x0203
	MsgTxnCommitResp        MsgType = 0x0204
	MsgTxnRollback          MsgType = 0x0205
	MsgTxnRollbackResp      MsgType = 0x0206
	MsgTxnResolveLock       MsgType = 0x0207
	MsgTxnResolveLockResp   MsgType = 0x0208

	// Storage & MVCC (0x0300 - 0x03FF)
	MsgKVGet                MsgType = 0x0301
	MsgKVGetResp            MsgType = 0x0302
	MsgKVPut                MsgType = 0x0303
	MsgKVPutResp            MsgType = 0x0304
	MsgKVDelete             MsgType = 0x0305
	MsgKVDeleteResp         MsgType = 0x0306
	MsgKVScan               MsgType = 0x0307
	MsgKVScanResp           MsgType = 0x0308

	// Durable Workflows (0x0400 - 0x04FF)
	MsgWorkflowStart        MsgType = 0x0401
	MsgWorkflowStartResp    MsgType = 0x0402
	MsgWorkflowSignal       MsgType = 0x0403
	MsgWorkflowSignalResp   MsgType = 0x0404
	MsgWorkflowQuery        MsgType = 0x0405
	MsgWorkflowQueryResp    MsgType = 0x0406
	MsgWorkflowHistory      MsgType = 0x0407
	MsgWorkflowHistoryResp  MsgType = 0x0408

	// Cluster SWIM Gossip (0x0500 - 0x05FF)
	MsgSWIMPing             MsgType = 0x0501
	MsgSWIMPingReq          MsgType = 0x0502
	MsgSWIMAck              MsgType = 0x0503

	// Streaming & Vector Processing (0x0600 - 0x06FF)
	MsgStreamSubscribe      MsgType = 0x0601
	MsgStreamChunk          MsgType = 0x0602
	MsgStreamWatermark      MsgType = 0x0603
	MsgStreamCancel         MsgType = 0x0604
)

func (m MsgType) String() string {
	switch m {
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	case MsgError:
		return "ERROR"
	case MsgRequestVote:
		return "REQUEST_VOTE"
	case MsgRequestVoteResp:
		return "REQUEST_VOTE_RESP"
	case MsgAppendEntries:
		return "APPEND_ENTRIES"
	case MsgAppendEntriesResp:
		return "APPEND_ENTRIES_RESP"
	case MsgInstallSnapshot:
		return "INSTALL_SNAPSHOT"
	case MsgInstallSnapshotResp:
		return "INSTALL_SNAPSHOT_RESP"
	case MsgTxnPrewrite:
		return "TXN_PREWRITE"
	case MsgTxnPrewriteResp:
		return "TXN_PREWRITE_RESP"
	case MsgTxnCommit:
		return "TXN_COMMIT"
	case MsgTxnCommitResp:
		return "TXN_COMMIT_RESP"
	case MsgTxnRollback:
		return "TXN_ROLLBACK"
	case MsgTxnRollbackResp:
		return "TXN_ROLLBACK_RESP"
	case MsgKVGet:
		return "KV_GET"
	case MsgKVGetResp:
		return "KV_GET_RESP"
	case MsgKVPut:
		return "KV_PUT"
	case MsgKVPutResp:
		return "KV_PUT_RESP"
	case MsgWorkflowStart:
		return "WORKFLOW_START"
	case MsgWorkflowStartResp:
		return "WORKFLOW_START_RESP"
	case MsgWorkflowSignal:
		return "WORKFLOW_SIGNAL"
	case MsgWorkflowSignalResp:
		return "WORKFLOW_SIGNAL_RESP"
	case MsgWorkflowQuery:
		return "WORKFLOW_QUERY"
	case MsgWorkflowQueryResp:
		return "WORKFLOW_QUERY_RESP"
	case MsgSWIMPing:
		return "SWIM_PING"
	case MsgSWIMAck:
		return "SWIM_ACK"
	case MsgStreamChunk:
		return "STREAM_CHUNK"
	case MsgStreamWatermark:
		return "STREAM_WATERMARK"
	default:
		return fmt.Sprintf("MsgType(0x%04X)", uint16(m))
	}
}
