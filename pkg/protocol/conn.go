package protocol

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnOptions configures network connection parameters.
type ConnOptions struct {
	ConnectTimeout   time.Duration
	HeartbeatPeriod  time.Duration
	MaxRetryInterval time.Duration
	MaxRetries       int
}

// DefaultConnOptions provides sensible defaults for low-latency cluster communication.
var DefaultConnOptions = ConnOptions{
	ConnectTimeout:   3 * time.Second,
	HeartbeatPeriod:  1 * time.Second,
	MaxRetryInterval: 5 * time.Second,
	MaxRetries:       5,
}

// ManagedConn manages a resilient, auto-reconnecting multiplexed connection to a remote node.
type ManagedConn struct {
	targetAddr string
	opts       ConnOptions
	mu         sync.RWMutex
	mux        *StreamMux
	rawConn    net.Conn
	closed     atomic.Bool
	stopChan   chan struct{}
}

// DialManagedConn connects to a remote address and establishes a managed connection.
func DialManagedConn(targetAddr string, opts ConnOptions) (*ManagedConn, error) {
	conn, err := net.DialTimeout("tcp", targetAddr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	mc := &ManagedConn{
		targetAddr: targetAddr,
		opts:       opts,
		rawConn:    conn,
		mux:        NewStreamMux(conn, false),
		stopChan:   make(chan struct{}),
	}

	go mc.heartbeatLoop()
	return mc, nil
}

// WrapManagedConn wraps an existing accepted net.Conn into a ManagedConn.
func WrapManagedConn(conn net.Conn, isServer bool, opts ConnOptions) *ManagedConn {
	mc := &ManagedConn{
		targetAddr: conn.RemoteAddr().String(),
		opts:       opts,
		rawConn:    conn,
		mux:        NewStreamMux(conn, isServer),
		stopChan:   make(chan struct{}),
	}
	go mc.heartbeatLoop()
	return mc
}

// OpenStream opens a new logical stream over the managed connection.
func (mc *ManagedConn) OpenStream() (*Stream, error) {
	mc.mu.RLock()
	mux := mc.mux
	mc.mu.RUnlock()

	if mux == nil || mc.closed.Load() {
		return nil, ErrMuxClosed
	}

	return mux.OpenStream()
}

// AcceptStream awaits an incoming stream over the managed connection.
func (mc *ManagedConn) AcceptStream(timeout time.Duration) (*Stream, error) {
	mc.mu.RLock()
	mux := mc.mux
	mc.mu.RUnlock()

	if mux == nil || mc.closed.Load() {
		return nil, ErrMuxClosed
	}

	return mux.AcceptStream(timeout)
}

func (mc *ManagedConn) heartbeatLoop() {
	ticker := time.NewTicker(mc.opts.HeartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if mc.closed.Load() {
				return
			}
			mc.mu.RLock()
			mux := mc.mux
			mc.mu.RUnlock()

			if mux != nil {
				hb := NewFrame(0, MsgPing, nil)
				hb.Flags |= FlagHeartbeat
				_ = mux.writeFrame(hb)
			}
		case <-mc.stopChan:
			return
		}
	}
}

// Close gracefully closes the connection.
func (mc *ManagedConn) Close() error {
	if mc.closed.Swap(true) {
		return nil
	}
	close(mc.stopChan)

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.mux != nil {
		_ = mc.mux.Close()
	}
	if mc.rawConn != nil {
		_ = mc.rawConn.Close()
	}
	return nil
}

// ConnPool manages a thread-safe pool of persistent ManagedConns to peer nodes.
type ConnPool struct {
	mu    sync.RWMutex
	conns map[string]*ManagedConn
	opts  ConnOptions
}

// NewConnPool creates a new connection pool.
func NewConnPool(opts ConnOptions) *ConnPool {
	return &ConnPool{
		conns: make(map[string]*ManagedConn),
		opts:  opts,
	}
}

// Get acquires or dials a ManagedConn for targetAddr.
func (p *ConnPool) Get(targetAddr string) (*ManagedConn, error) {
	p.mu.RLock()
	mc, exists := p.conns[targetAddr]
	p.mu.RUnlock()

	if exists && !mc.closed.Load() {
		return mc, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double check
	if mc, exists = p.conns[targetAddr]; exists && !mc.closed.Load() {
		return mc, nil
	}

	newMC, err := DialManagedConn(targetAddr, p.opts)
	if err != nil {
		return nil, err
	}

	p.conns[targetAddr] = newMC
	return newMC, nil
}

// Close closes all pooled connections.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for addr, mc := range p.conns {
		if err := mc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(p.conns, addr)
	}
	return firstErr
}
