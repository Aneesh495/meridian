package server

import (
	"bytes"
	"errors"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/consensus/multiraft"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/txn"
)

// Handler dispatches incoming multiplexed protocol frames to the appropriate engine.
type Handler struct {
	nodeID      core.NodeID
	storage     *storage.Engine
	multiraft   *multiraft.Coordinator
	txnCoord    *txn.Coordinator
	participant *txn.Participant
}

// NewHandler creates a new frame Handler.
func NewHandler(nodeID core.NodeID, storage *storage.Engine, mr *multiraft.Coordinator, tc *txn.Coordinator, p *txn.Participant) *Handler {
	return &Handler{
		nodeID:      nodeID,
		storage:     storage,
		multiraft:   mr,
		txnCoord:    tc,
		participant: p,
	}
}

// SetParticipant registers or updates the local shard 2PC participant.
func (h *Handler) SetParticipant(p *txn.Participant) {
	h.participant = p
}

// HandleFrame routes a received frame and returns a response frame if appropriate.
func (h *Handler) HandleFrame(from core.NodeID, frame *protocol.Frame) (*protocol.Frame, error) {
	switch frame.Type {
	case protocol.MsgPing:
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgPong, []byte("pong"))
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgKVGet:
		var key []byte
		var readTS hlc.Timestamp

		if req, err := protocol.DecodeKVGetRequest(frame.Payload); err == nil && len(req.Key) > 0 {
			key = req.Key
			readTS = req.ReadTS
		} else {
			key = frame.Payload
		}

		if readTS == hlc.ZeroTimestamp && h.txnCoord != nil {
			readTS = h.txnCoord.Clock().Now()
		}

		val, ts, err := h.storage.Get(key, readTS)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				respPayload := protocol.EncodeKVGetResponse(&protocol.KVGetResponse{
					Found: false,
					Error: err.Error(),
				})
				resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVGetResp, respPayload)
				resp.Flags |= protocol.FlagResponse
				return resp, nil
			}
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte(err.Error()))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}

		respPayload := protocol.EncodeKVGetResponse(&protocol.KVGetResponse{
			Found: true,
			Value: val,
			TS:    ts,
		})
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVGetResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgKVPut:
		req, err := protocol.DecodeKVPutRequest(frame.Payload)
		if err != nil {
			// Fallback: parse raw key/val split by null delimiter
			idx := bytes.IndexByte(frame.Payload, 0)
			if idx > 0 {
				fb := KVPutFallback(frame.Payload[:idx], frame.Payload[idx+1:], hlc.ZeroTimestamp)
				req = &fb
			} else {
				errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte("invalid put payload"))
				errResp.Flags |= protocol.FlagResponse
				return errResp, nil
			}
		}

		commitTS := req.TS
		if commitTS == hlc.ZeroTimestamp && h.txnCoord != nil {
			commitTS = h.txnCoord.Clock().Now()
		}

		if err := h.storage.Put(req.Key, req.Value, commitTS); err != nil {
			respPayload := protocol.EncodeKVPutResponse(&protocol.KVPutResponse{
				Success: false,
				Error:   err.Error(),
			})
			resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVPutResp, respPayload)
			resp.Flags |= protocol.FlagResponse
			return resp, nil
		}

		respPayload := protocol.EncodeKVPutResponse(&protocol.KVPutResponse{Success: true})
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVPutResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgKVDelete:
		key := frame.Payload
		var commitTS hlc.Timestamp
		if h.txnCoord != nil {
			commitTS = h.txnCoord.Clock().Now()
		}

		if err := h.storage.Delete(key, commitTS); err != nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte(err.Error()))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVDeleteResp, []byte("ok"))
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgKVScan:
		req, err := protocol.DecodeKVScanRequest(frame.Payload)
		if err != nil {
			req = &protocol.KVScanRequest{
				Prefix: frame.Payload,
				Limit:  100,
			}
		}
		if req.Limit == 0 {
			req.Limit = 100
		}
		readTS := req.ReadTS
		if readTS == hlc.ZeroTimestamp && h.txnCoord != nil {
			readTS = h.txnCoord.Clock().Now()
		}

		it := h.storage.NewSnapshotIterator(req.Prefix, readTS)
		var entries []protocol.KVScanEntry
		for it.Next() {
			entry := it.Entry()
			if entry != nil {
				entries = append(entries, protocol.KVScanEntry{
					Key:   entry.Key,
					Value: entry.Value,
					TS:    entry.Timestamp,
				})
			}
			if uint32(len(entries)) >= req.Limit {
				break
			}
		}

		respPayload := protocol.EncodeKVScanResponse(&protocol.KVScanResponse{
			Entries: entries,
		})
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVScanResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgTxnPrewrite:
		if h.participant == nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte("no participant configured"))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}
		req, err := protocol.DecodePrewriteRequest(frame.Payload)
		if err != nil {
			return nil, err
		}
		res, err := h.participant.HandlePrewrite(req)
		if err != nil {
			return nil, err
		}
		respPayload := protocol.EncodePrewriteResponse(res)
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgTxnPrewriteResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgTxnCommit:
		if h.participant == nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte("no participant configured"))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}
		req, err := protocol.DecodeCommitRequest(frame.Payload)
		if err != nil {
			return nil, err
		}
		res, err := h.participant.HandleCommit(req)
		if err != nil {
			return nil, err
		}
		respPayload := protocol.EncodeCommitResponse(res)
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgTxnCommitResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgTxnRollback:
		if h.participant == nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte("no participant configured"))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}
		req, err := protocol.DecodeRollbackRequest(frame.Payload)
		if err != nil {
			return nil, err
		}
		res, err := h.participant.HandleRollback(req)
		if err != nil {
			return nil, err
		}
		respPayload := protocol.EncodeRollbackResponse(res)
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgTxnRollbackResp, respPayload)
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgRequestVote, protocol.MsgAppendEntries, protocol.MsgInstallSnapshot:
		if h.multiraft != nil {
			// Shard ID 1 as default primary group for standalone tests
			_ = h.multiraft.Step(1, from, frame.Type, frame.Payload)
		}
		return nil, nil
	}

	return nil, nil
}

// KVPutFallback constructs a fallback put request.
func KVPutFallback(k, v []byte, ts hlc.Timestamp) protocol.KVPutRequest {
	return protocol.KVPutRequest{Key: k, Value: v, TS: ts}
}
