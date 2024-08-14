package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/hlc"
	"github.com/Aneesh495/meridian/pkg/protocol"
)

var (
	ErrNoServersAvailable = errors.New("meridian/client: no reachable cluster nodes")
	ErrTxnAborted         = errors.New("meridian/client: transaction aborted")
)

// ClientOptions configures connection and retry parameters for the Meridian client.
type ClientOptions struct {
	Timeout       time.Duration
	RetryAttempts int
	RetryBackoff  time.Duration
}

// DefaultClientOptions returns robust defaults for client network communication.
var DefaultClientOptions = ClientOptions{
	Timeout:       5 * time.Second,
	RetryAttempts: 3,
	RetryBackoff:  50 * time.Millisecond,
}

// Client provides an ergonomic, thread-safe Go API for accessing a Meridian cluster.
type Client struct {
	mu       sync.RWMutex
	addrs    []string
	opts     ClientOptions
	conns    map[string]*protocol.ManagedConn
	clock    *hlc.Clock
	nextIdx  int
	closed   bool
}

// NewClient creates and connects a Meridian client.
func NewClient(addrs []string, opts ClientOptions) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("meridian/client: at least one server address required")
	}

	c := &Client{
		addrs: addrs,
		opts:  opts,
		conns: make(map[string]*protocol.ManagedConn),
		clock: hlc.NewClock(1, 250*time.Millisecond),
	}

	return c, nil
}

func (c *Client) getConn() (*protocol.ManagedConn, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, "", ErrServerClosed
	}

	for i := 0; i < len(c.addrs); i++ {
		addr := c.addrs[(c.nextIdx+i)%len(c.addrs)]
		if mc, ok := c.conns[addr]; ok {
			return mc, addr, nil
		}

		mc, err := protocol.DialManagedConn(addr, protocol.DefaultConnOptions)
		if err == nil {
			c.conns[addr] = mc
			c.nextIdx = (c.nextIdx + i + 1) % len(c.addrs)
			return mc, addr, nil
		}
	}

	return nil, "", ErrNoServersAvailable
}

func (c *Client) call(ctx context.Context, msgType protocol.MsgType, payload []byte) (*protocol.Frame, error) {
	var lastErr error

	for attempt := 0; attempt < c.opts.RetryAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		mc, addr, err := c.getConn()
		if err != nil {
			lastErr = err
			time.Sleep(c.opts.RetryBackoff)
			continue
		}

		stream, err := mc.OpenStream()
		if err != nil {
			c.mu.Lock()
			delete(c.conns, addr)
			c.mu.Unlock()
			lastErr = err
			time.Sleep(c.opts.RetryBackoff)
			continue
		}

		if err := stream.Send(msgType, payload); err != nil {
			_ = stream.Close()
			c.mu.Lock()
			delete(c.conns, addr)
			c.mu.Unlock()
			lastErr = err
			time.Sleep(c.opts.RetryBackoff)
			continue
		}

		resp, err := stream.Receive(c.opts.Timeout)
		_ = stream.Close()

		if err != nil {
			lastErr = err
			time.Sleep(c.opts.RetryBackoff)
			continue
		}

		if resp.Type == protocol.MsgError {
			return nil, errors.New(string(resp.Payload))
		}

		return resp, nil
	}

	return nil, fmt.Errorf("meridian/client: request failed after %d retries: %w", c.opts.RetryAttempts, lastErr)
}

// Ping probes server liveness.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.call(ctx, protocol.MsgPing, []byte("ping"))
	if err != nil {
		return err
	}
	if resp.Type != protocol.MsgPong {
		return fmt.Errorf("unexpected ping response: %v", resp.Type)
	}
	return nil
}

// Get reads the latest visible value for key at the current hybrid clock time.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	req := &protocol.KVGetRequest{
		Key:    key,
		ReadTS: c.clock.Now(),
	}
	payload := protocol.EncodeKVGetRequest(req)

	resp, err := c.call(ctx, protocol.MsgKVGet, payload)
	if err != nil {
		return nil, err
	}

	res, err := protocol.DecodeKVGetResponse(resp.Payload)
	if err != nil {
		return nil, err
	}

	if !res.Found {
		return nil, core.ErrNotFound
	}

	return res.Value, nil
}

// Put writes key-value at the current hybrid clock commit timestamp.
func (c *Client) Put(ctx context.Context, key []byte, value []byte) error {
	req := &protocol.KVPutRequest{
		Key:   key,
		Value: value,
		TS:    c.clock.Now(),
	}
	payload := protocol.EncodeKVPutRequest(req)

	resp, err := c.call(ctx, protocol.MsgKVPut, payload)
	if err != nil {
		return err
	}

	res, err := protocol.DecodeKVPutResponse(resp.Payload)
	if err != nil {
		return err
	}

	if !res.Success {
		return errors.New(res.Error)
	}

	return nil
}

// Delete marks a key deleted with a tombstone.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	_, err := c.call(ctx, protocol.MsgKVDelete, key)
	return err
}

// Scan retrieves up to limit keys matching prefix.
func (c *Client) Scan(ctx context.Context, prefix []byte, limit uint32) ([]protocol.KVScanEntry, error) {
	req := &protocol.KVScanRequest{
		Prefix: prefix,
		Limit:  limit,
		ReadTS: c.clock.Now(),
	}
	payload := protocol.EncodeKVScanRequest(req)

	resp, err := c.call(ctx, protocol.MsgKVScan, payload)
	if err != nil {
		return nil, err
	}

	res, err := protocol.DecodeKVScanResponse(resp.Payload)
	if err != nil {
		return nil, err
	}

	if res.Error != "" {
		return nil, errors.New(res.Error)
	}

	return res.Entries, nil
}

// Close closes all pooled server connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	for _, mc := range c.conns {
		mc.Close()
	}
	c.conns = make(map[string]*protocol.ManagedConn)
	return nil
}
