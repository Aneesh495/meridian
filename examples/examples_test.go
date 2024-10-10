package examples_test

import (
	"os/exec"
	"testing"
)

func TestExample_DurableTransfer(t *testing.T) {
	cmd := exec.Command("go", "run", "./durable_transfer/main.go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("durable_transfer failed: %v\nOutput:\n%s", err, string(out))
	}
}

func TestExample_DistributedOrderSaga(t *testing.T) {
	cmd := exec.Command("go", "run", "./distributed_order_saga/main.go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("distributed_order_saga failed: %v\nOutput:\n%s", err, string(out))
	}
}

func TestExample_StreamingSlidingWindow(t *testing.T) {
	cmd := exec.Command("go", "run", "./streaming_sliding_window/main.go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("streaming_sliding_window failed: %v\nOutput:\n%s", err, string(out))
	}
}

func TestExample_VectorizedAnalytics(t *testing.T) {
	cmd := exec.Command("go", "run", "./vectorized_analytics/main.go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("vectorized_analytics failed: %v\nOutput:\n%s", err, string(out))
	}
}

func TestExample_ChaosPartitionRecovery(t *testing.T) {
	cmd := exec.Command("go", "run", "./chaos_partition_recovery/main.go")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("chaos_partition_recovery failed: %v\nOutput:\n%s", err, string(out))
	}
}
