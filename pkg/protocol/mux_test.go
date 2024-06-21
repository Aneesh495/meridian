package protocol

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

func TestStreamMuxConcurrentRPC(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	clientMux := NewStreamMux(c1, false)
	serverMux := NewStreamMux(c2, true)
	defer clientMux.Close()
	defer serverMux.Close()

	const numStreams = 20
	var wg sync.WaitGroup
	wg.Add(numStreams * 2)

	// Server handler
	go func() {
		for i := 0; i < numStreams; i++ {
			stream, err := serverMux.AcceptStream(2 * time.Second)
			if err != nil {
				t.Errorf("server accept stream failed: %v", err)
				return
			}

			go func(s *Stream) {
				defer wg.Done()
				frame, err := s.Receive(2 * time.Second)
				if err != nil {
					t.Errorf("server receive failed: %v", err)
					return
				}

				respPayload := append([]byte("echo:"), frame.Payload...)
				if err := s.Send(MsgPong, respPayload); err != nil {
					t.Errorf("server send failed: %v", err)
					return
				}
			}(stream)
		}
	}()

	// Client callers
	for i := 0; i < numStreams; i++ {
		go func(idx int) {
			defer wg.Done()
			stream, err := clientMux.OpenStream()
			if err != nil {
				t.Errorf("client open stream failed: %v", err)
				return
			}
			defer stream.Close()

			payload := []byte{byte(idx), 0xAA, 0xBB, 0xCC}
			if err := stream.Send(MsgPing, payload); err != nil {
				t.Errorf("client send failed: %v", err)
				return
			}

			resp, err := stream.Receive(2 * time.Second)
			if err != nil {
				t.Errorf("client receive failed: %v", err)
				return
			}

			expected := append([]byte("echo:"), payload...)
			if !bytes.Equal(resp.Payload, expected) {
				t.Errorf("echo mismatch: got %v, expected %v", resp.Payload, expected)
			}
		}(i)
	}

	wg.Wait()
}
