package art

import (
	"fmt"
	"sort"
	"testing"
)

func TestARTBasicInsertAndSearch(t *testing.T) {
	tree := NewTree()

	tree.Insert([]byte("apple"), []byte("red"))
	tree.Insert([]byte("apply"), []byte("verb"))
	tree.Insert([]byte("banana"), []byte("yellow"))
	tree.Insert([]byte("app"), []byte("application"))

	if tree.Size() != 4 {
		t.Fatalf("expected size 4, got %d", tree.Size())
	}

	val, found := tree.Search([]byte("apple"))
	if !found || string(val) != "red" {
		t.Fatalf("expected 'red', got '%s'", string(val))
	}

	val, found = tree.Search([]byte("app"))
	if !found || string(val) != "application" {
		t.Fatalf("expected 'application', got '%s'", string(val))
	}

	_, found = tree.Search([]byte("orange"))
	if found {
		t.Fatalf("unexpected search hit for non-existent key")
	}

	// Update existing key
	old, updated := tree.Insert([]byte("apple"), []byte("green"))
	if !updated || string(old) != "red" {
		t.Fatalf("expected update of old value 'red'")
	}
	val, _ = tree.Search([]byte("apple"))
	if string(val) != "green" {
		t.Fatalf("expected updated value 'green', got '%s'", string(val))
	}
}

func TestARTScaleGrowShrink(t *testing.T) {
	tree := NewTree()
	const count = 1000

	keys := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
		tree.Insert(keys[i], []byte(fmt.Sprintf("val-%d", i)))
	}

	if tree.Size() != count {
		t.Fatalf("expected size %d, got %d", count, tree.Size())
	}

	for i := 0; i < count; i++ {
		val, found := tree.Search(keys[i])
		if !found {
			t.Fatalf("missing key: %s", string(keys[i]))
		}
		expected := fmt.Sprintf("val-%d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch: expected %s, got %s", expected, string(val))
		}
	}

	// Delete half the keys
	for i := 0; i < count/2; i++ {
		val, deleted := tree.Delete(keys[i])
		if !deleted {
			t.Fatalf("failed to delete key: %s", string(keys[i]))
		}
		expected := fmt.Sprintf("val-%d", i)
		if string(val) != expected {
			t.Fatalf("deleted value mismatch")
		}
	}

	if tree.Size() != count/2 {
		t.Fatalf("expected size %d after deletion, got %d", count/2, tree.Size())
	}
}

func TestARTIteratorOrdered(t *testing.T) {
	tree := NewTree()

	inputs := []string{
		"users:100",
		"users:102",
		"users:101",
		"orders:50",
		"users:105",
	}

	for _, s := range inputs {
		tree.Insert([]byte(s), []byte("data"))
	}

	it := tree.NewIterator([]byte("users:"))
	var collected []string
	for it.HasNext() {
		leaf, ok := it.Next()
		if ok {
			collected = append(collected, string(leaf.Key))
		}
	}

	if len(collected) != 4 {
		t.Fatalf("expected 4 user keys, got %d", len(collected))
	}

	if !sort.StringsAreSorted(collected) {
		t.Fatalf("iterator results must be sorted: %v", collected)
	}

	// Verify exact order
	expected := []string{"users:100", "users:101", "users:102", "users:105"}
	for i := range expected {
		if collected[i] != expected[i] {
			t.Fatalf("order mismatch at %d: %s vs %s", i, collected[i], expected[i])
		}
	}
}
