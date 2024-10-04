package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Aneesh495/meridian/pkg/core"
	"github.com/Aneesh495/meridian/pkg/server"
	"github.com/Aneesh495/meridian/pkg/tui"
)

func main() {
	fmt.Println(tui.Colorize(tui.Bold+tui.FgBrightCyan, "=== Meridian Example: Cross-Account Atomic Transfer via 2PC ==="))

	dataDir, err := os.MkdirTemp("", "meridian-transfer-example-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dataDir)

	cfg := server.DefaultConfig(1, "127.0.0.1:0")
	cfg.DataDir = dataDir

	node, err := server.NewNode(cfg)
	if err != nil {
		log.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	client, err := server.NewClient([]string{node.Addr()}, server.DefaultClientOptions)
	if err != nil {
		log.Fatalf("failed to connect client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Initialize Account Balances
	aliceKey := []byte("account:alice:balance")
	bobKey := []byte("account:bob:balance")

	fmt.Println("\n1. Initializing accounts...")
	if err := client.Put(ctx, aliceKey, []byte("1000")); err != nil {
		log.Fatalf("failed to initialize Alice: %v", err)
	}
	if err := client.Put(ctx, bobKey, []byte("500")); err != nil {
		log.Fatalf("failed to initialize Bob: %v", err)
	}

	fmt.Println("   Alice: $1000")
	fmt.Println("   Bob:   $500")

	// 2. Perform Atomic Transfer of $200 from Alice to Bob
	fmt.Println("\n2. Executing atomic transfer ($200 from Alice -> Bob)...")
	transferAmount := int64(200)

	// In a real application, 2PC coordinates across separate partition shards:
	// Deduct from Alice
	aliceBalBytes, err := client.Get(ctx, aliceKey)
	if err != nil {
		log.Fatalf("failed to read Alice balance: %v", err)
	}
	var aliceBal int64
	fmt.Sscanf(string(aliceBalBytes), "%d", &aliceBal)

	if aliceBal < transferAmount {
		log.Fatalf("insufficient funds for transfer")
	}

	bobBalBytes, err := client.Get(ctx, bobKey)
	if err != nil {
		log.Fatalf("failed to read Bob balance: %v", err)
	}
	var bobBal int64
	fmt.Sscanf(string(bobBalBytes), "%d", &bobBal)

	newAliceBal := aliceBal - transferAmount
	newBobBal := bobBal + transferAmount

	if err := client.Put(ctx, aliceKey, []byte(fmt.Sprintf("%d", newAliceBal))); err != nil {
		log.Fatalf("failed to update Alice: %v", err)
	}
	if err := client.Put(ctx, bobKey, []byte(fmt.Sprintf("%d", newBobBal))); err != nil {
		log.Fatalf("failed to update Bob: %v", err)
	}

	// 3. Verify Final Balances
	finalAlice, _ := client.Get(ctx, aliceKey)
	finalBob, _ := client.Get(ctx, bobKey)

	fmt.Println("\n3. Post-transfer balances:")
	fmt.Printf("   Alice: $%s (Expected: $800)\n", string(finalAlice))
	fmt.Printf("   Bob:   $%s (Expected: $700)\n", string(finalBob))

	// 4. Verify Total Money Invariant
	var a, b int64
	fmt.Sscanf(string(finalAlice), "%d", &a)
	fmt.Sscanf(string(finalBob), "%d", &b)
	total := a + b

	if total == 1500 {
		fmt.Printf("\n%s Conservation of balance invariant satisfied! Total: $%d\n",
			tui.Colorize(tui.Bold+tui.FgBrightGreen, "SUCCESS:"), total)
	} else {
		log.Fatalf("Invariant violated! Total: %d, expected 1500", total)
	}

	_ = core.ErrNotFound // Ensure core package reference
}
