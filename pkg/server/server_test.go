package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
)

func createTestNode(t *testing.T, id core.NodeID, addr string) *MeridianNode {
	t.Helper()

	dataDir, err := os.MkdirTemp("", fmt.Sprintf("meridian-test-node-%d-*", id))
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := DefaultConfig(id, addr)
	cfg.DataDir = dataDir

	node, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("failed to initialize node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	t.Cleanup(func() {
		_ = node.Stop()
		_ = os.RemoveAll(dataDir)
	})

	return node
}

func TestServer_PingPong(t *testing.T) {
	node := createTestNode(t, 1, "127.0.0.1:0")

	client, err := NewClient([]string{node.Addr()}, DefaultClientOptions)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestServer_KVCrud(t *testing.T) {
	node := createTestNode(t, 1, "127.0.0.1:0")

	client, err := NewClient([]string{node.Addr()}, DefaultClientOptions)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Put
	key := []byte("account:alice:balance")
	val := []byte("1000")
	if err := client.Put(ctx, key, val); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Get
	got, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("expected %s, got %s", val, got)
	}

	// Scan
	prefix := []byte("account:alice:")
	entries, err := client.Scan(ctx, prefix, 10)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if !bytes.Equal(entries[0].Key, key) {
		t.Fatalf("expected key %s, got %s", key, entries[0].Key)
	}

	// Delete
	if err := client.Delete(ctx, key); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Get after delete should return not found
	_, err = client.Get(ctx, key)
	if err == nil {
		t.Fatalf("expected not found error after delete, got nil")
	}
}

func TestServer_MultiNodeFailover(t *testing.T) {
	node1 := createTestNode(t, 1, "127.0.0.1:0")
	node2 := createTestNode(t, 2, "127.0.0.1:0")

	client, err := NewClient([]string{node1.Addr(), node2.Addr()}, DefaultClientOptions)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Write to cluster
	if err := client.Put(ctx, []byte("failover:key"), []byte("failover:val")); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Stop node 1
	_ = node1.Stop()

	// Client should transparently retry and reach node 2
	// Write new key to surviving node
	if err := client.Put(ctx, []byte("surviving:key"), []byte("surviving:val")); err != nil {
		t.Fatalf("put after failover failed: %v", err)
	}

	got, err := client.Get(ctx, []byte("surviving:key"))
	if err != nil {
		t.Fatalf("get after failover failed: %v", err)
	}
	if string(got) != "surviving:val" {
		t.Fatalf("expected surviving:val, got %s", got)
	}
}
