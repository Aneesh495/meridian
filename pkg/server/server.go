package server

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Aneesh495/meridian/pkg/protocol"
)

var (
	ErrServerClosed = errors.New("meridian/server: server is closed")
)

// Server coordinates the TCP network transport and dispatches multiplexed streams.
type Server struct {
	cfg        Config
	handler    *Handler
	listener   net.Listener
	connsMu    sync.RWMutex
	conns      map[*protocol.ManagedConn]struct{}
	activeStreams atomic.Int64
	totalRequests atomic.Uint64
	totalBytes    atomic.Uint64
	closed     atomic.Bool
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

// NewServer initializes a Meridian TCP RPC server.
func NewServer(cfg Config, handler *Handler) *Server {
	return &Server{
		cfg:      cfg,
		handler:  handler,
		conns:    make(map[*protocol.ManagedConn]struct{}),
		stopChan: make(chan struct{}),
	}
}

// Start binds to the configured listen address and begins accepting connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("meridian/server: failed to bind %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Addr returns the bound network listener address.
func (s *Server) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		mc := protocol.WrapManagedConn(conn, true, protocol.DefaultConnOptions)
		s.connsMu.Lock()
		s.conns[mc] = struct{}{}
		s.connsMu.Unlock()

		s.wg.Add(1)
		go s.connServeLoop(mc)
	}
}

func (s *Server) connServeLoop(mc *protocol.ManagedConn) {
	defer func() {
		mc.Close()
		s.connsMu.Lock()
		delete(s.conns, mc)
		s.connsMu.Unlock()
		s.wg.Done()
	}()

	for {
		if s.closed.Load() {
			return
		}

		stream, err := mc.AcceptStream(500 * time.Millisecond)
		if err != nil {
			if errors.Is(err, protocol.ErrStreamTimeout) {
				continue
			}
			return
		}

		s.activeStreams.Add(1)
		go s.handleStream(stream)
	}
}

func (s *Server) handleStream(stream *protocol.Stream) {
	defer func() {
		s.activeStreams.Add(-1)
		stream.Close()
	}()

	for {
		frame, err := stream.Receive(5 * time.Second)
		if err != nil {
			return
		}

		s.totalRequests.Add(1)
		s.totalBytes.Add(uint64(len(frame.Payload)))

		resp, err := s.handler.HandleFrame(s.cfg.NodeID, frame)
		if err != nil {
			errResp := protocol.NewFrame(frame.StreamID, protocol.MsgError, []byte(err.Error()))
			errResp.Flags |= protocol.FlagResponse
			_ = stream.Send(errResp.Type, errResp.Payload)
			return
		}

		if resp != nil {
			if err := stream.Send(resp.Type, resp.Payload); err != nil {
				return
			}
		}

		// Single request-response per stream channel
		return
	}
}

// ActiveConnections returns the count of open client connections.
func (s *Server) ActiveConnections() int {
	s.connsMu.RLock()
	defer s.connsMu.RUnlock()
	return len(s.conns)
}

// ActiveStreams returns the count of concurrently executing stream requests.
func (s *Server) ActiveStreams() int64 {
	return s.activeStreams.Load()
}

// TotalRequests returns the cumulative count of handled RPC frames.
func (s *Server) TotalRequests() uint64 {
	return s.totalRequests.Load()
}

// TotalBytes returns the cumulative payload bytes processed.
func (s *Server) TotalBytes() uint64 {
	return s.totalBytes.Load()
}

// Stop gracefully stops the server listener and closes active connections.
func (s *Server) Stop() error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.stopChan)

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	s.connsMu.Lock()
	for mc := range s.conns {
		mc.Close()
	}
	s.connsMu.Unlock()

	s.wg.Wait()
	return err
}
