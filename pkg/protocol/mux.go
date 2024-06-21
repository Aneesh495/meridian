package protocol

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrMuxClosed = errors.New("meridian/protocol: multiplexer is closed")
	ErrStreamTimeout = errors.New("meridian/protocol: stream operation timed out")
)

// Stream represents an individual bidirectional logical RPC channel.
type Stream struct {
	id       uint64
	mux      *StreamMux
	inbox    chan *Frame
	closed   atomic.Bool
	closeMu  sync.Mutex
}

// ID returns the 64-bit stream identifier.
func (s *Stream) ID() uint64 {
	return s.id
}

// Send transmits a frame over this stream.
func (s *Stream) Send(msgType MsgType, payload []byte) error {
	if s.closed.Load() {
		return ErrMuxClosed
	}
	frame := NewFrame(s.id, msgType, payload)
	return s.mux.writeFrame(frame)
}

// Receive waits for the next incoming frame on this stream.
func (s *Stream) Receive(timeout time.Duration) (*Frame, error) {
	if s.closed.Load() {
		return nil, ErrMuxClosed
	}

	if timeout <= 0 {
		select {
		case f, ok := <-s.inbox:
			if !ok {
				return nil, io.EOF
			}
			return f, nil
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case f, ok := <-s.inbox:
		if !ok {
			return nil, io.EOF
		}
		return f, nil
	case <-timer.C:
		return nil, ErrStreamTimeout
	}
}

// Close closes the stream and unregisters it from the multiplexer.
func (s *Stream) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed.Swap(true) {
		return nil
	}
	s.mux.removeStream(s.id)
	return nil
}

// StreamMux multiplexes multiple concurrent streams across a single network connection.
type StreamMux struct {
	conn       net.Conn
	nextID     uint64
	streamsMu  sync.RWMutex
	streams    map[uint64]*Stream
	acceptChan chan *Stream
	writeQueue chan *Frame
	closed     atomic.Bool
	closeOnce  sync.Once
	done       chan struct{}
}

// NewStreamMux wraps a network connection with multiplexing capabilities.
func NewStreamMux(conn net.Conn, isServer bool) *StreamMux {
	var initialID uint64 = 1
	if isServer {
		initialID = 2 // server initiated streams use even IDs, clients odd IDs
	}

	m := &StreamMux{
		conn:       conn,
		nextID:     initialID,
		streams:    make(map[uint64]*Stream),
		acceptChan: make(chan *Stream, 256),
		writeQueue: make(chan *Frame, 1024),
		done:       make(chan struct{}),
	}

	go m.readLoop()
	go m.writeLoop()

	return m
}

func (m *StreamMux) writeFrame(f *Frame) error {
	if m.closed.Load() {
		return ErrMuxClosed
	}
	select {
	case m.writeQueue <- f:
		return nil
	case <-m.done:
		return ErrMuxClosed
	}
}

func (m *StreamMux) writeLoop() {
	for {
		select {
		case f := <-m.writeQueue:
			if err := f.WriteTo(m.conn); err != nil {
				m.Close()
				return
			}
		case <-m.done:
			return
		}
	}
}

func (m *StreamMux) readLoop() {
	for {
		frame, err := ReadFrame(m.conn)
		if err != nil {
			m.Close()
			return
		}

		// Handle heartbeat frames immediately
		if frame.Flags&FlagHeartbeat != 0 {
			continue
		}

		m.streamsMu.Lock()
		stream, exists := m.streams[frame.StreamID]
		if !exists {
			// Inbound new stream
			stream = &Stream{
				id:    frame.StreamID,
				mux:   m,
				inbox: make(chan *Frame, 64),
			}
			m.streams[frame.StreamID] = stream
			m.streamsMu.Unlock()

			select {
			case m.acceptChan <- stream:
			default:
				// Buffer full, drop or close
			}
		} else {
			m.streamsMu.Unlock()
		}

		select {
		case stream.inbox <- frame:
		default:
			// Stream queue overflow
		}
	}
}

// OpenStream allocates a new logical stream initiated by this endpoint.
func (m *StreamMux) OpenStream() (*Stream, error) {
	if m.closed.Load() {
		return nil, ErrMuxClosed
	}

	id := atomic.AddUint64(&m.nextID, 2)
	stream := &Stream{
		id:    id,
		mux:   m,
		inbox: make(chan *Frame, 64),
	}

	m.streamsMu.Lock()
	m.streams[id] = stream
	m.streamsMu.Unlock()

	return stream, nil
}

// AcceptStream waits for a remote peer to initiate a new stream.
func (m *StreamMux) AcceptStream(timeout time.Duration) (*Stream, error) {
	if m.closed.Load() {
		return nil, ErrMuxClosed
	}

	if timeout <= 0 {
		select {
		case s, ok := <-m.acceptChan:
			if !ok {
				return nil, ErrMuxClosed
			}
			return s, nil
		case <-m.done:
			return nil, ErrMuxClosed
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case s, ok := <-m.acceptChan:
		if !ok {
			return nil, ErrMuxClosed
		}
		return s, nil
	case <-timer.C:
		return nil, ErrStreamTimeout
	case <-m.done:
		return nil, ErrMuxClosed
	}
}

func (m *StreamMux) removeStream(id uint64) {
	m.streamsMu.Lock()
	if s, ok := m.streams[id]; ok {
		delete(m.streams, id)
		close(s.inbox)
	}
	m.streamsMu.Unlock()
}

// Close closes the underlying network connection and cleans up all active streams.
func (m *StreamMux) Close() error {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.done)
		_ = m.conn.Close()

		m.streamsMu.Lock()
		for _, s := range m.streams {
			s.closed.Store(true)
			close(s.inbox)
		}
		m.streams = make(map[uint64]*Stream)
		m.streamsMu.Unlock()
	})
	return nil
}
