package server

import (
	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/consensus/multiraft"
	"github.com/Aneesh495/meridian/pkg/protocol"
	"github.com/Aneesh495/meridian/pkg/storage"
	"github.com/Aneesh495/meridian/pkg/txn"
)

// Handler dispatches incoming multiplexed protocol frames to the appropriate engine.
type Handler struct {
	nodeID    core.NodeID
	storage   *storage.Engine
	multiraft *multiraft.Coordinator
	txnCoord  *txn.Coordinator
}

// NewHandler creates a new frame Handler.
func NewHandler(nodeID core.NodeID, storage *storage.Engine, mr *multiraft.Coordinator, tc *txn.Coordinator) *Handler {
	return &Handler{
		nodeID:    nodeID,
		storage:   storage,
		multiraft: mr,
		txnCoord:  tc,
	}
}

// HandleFrame routes a received frame and returns a response frame if appropriate.
func (h *Handler) HandleFrame(from core.NodeID, frame *protocol.Frame) (*protocol.Frame, error) {
	switch frame.Type {
	case protocol.MsgPing:
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgPong, []byte("pong"))
		resp.Flags |= protocol.FlagResponse
		return resp, nil

	case protocol.MsgKVGet:
		// Simple direct storage read
		// Payload: key
		readTS := h.txnCoord.Clock().Now()
		val, _, err := h.storage.Get(frame.Payload, readTS)
		if err != nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte(err.Error()))
			errResp.Flags |= protocol.FlagResponse
			return errResp, nil
		}
		resp := protocol.NewFrame(frame.StreamID, protocol.MsgKVGetResp, val)
		resp.Flags |= protocol.FlagResponse
		return resp, nil
	}

	return nil, nil
}
