package protocol

import (
	"net"
	"testing"
	"time"
)

func TestManagedConnHeartbeat(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	opts := ConnOptions{
		ConnectTimeout:  1 * time.Second,
		HeartbeatPeriod: 50 * time.Millisecond,
	}

	mcClient := WrapManagedConn(c1, false, opts)
	mcServer := WrapManagedConn(c2, true, opts)
	defer mcClient.Close()
	defer mcServer.Close()

	// Allow a few heartbeats to exchange silently
	time.Sleep(150 * time.Millisecond)

	// Stream communication should remain active and clean
	sClient, err := mcClient.OpenStream()
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer sClient.Close()

	if err := sClient.Send(MsgPing, []byte("data-after-heartbeats")); err != nil {
		t.Fatalf("failed to send: %v", err)
	}

	sServer, err := mcServer.AcceptStream(1 * time.Second)
	if err != nil {
		t.Fatalf("failed to accept stream: %v", err)
	}
	defer sServer.Close()

	frame, err := sServer.Receive(1 * time.Second)
	if err != nil {
		t.Fatalf("failed to receive: %v", err)
	}

	if string(frame.Payload) != "data-after-heartbeats" {
		t.Fatalf("payload mismatch: %s", string(frame.Payload))
	}
}
