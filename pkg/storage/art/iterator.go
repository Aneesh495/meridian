package art

import (
	"bytes"
)

// Iterator provides in-order traversal of key-value pairs matching a prefix.
type Iterator struct {
	tree    *Tree
	prefix  []byte
	leaves  []*Leaf
	currIdx int
}

// NewIterator creates an Iterator over keys with the given prefix.
func (t *Tree) NewIterator(prefix []byte) *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var leaves []*Leaf
	t.collectLeaves(t.root, prefix, 0, &leaves)

	return &Iterator{
		tree:    t,
		prefix:  prefix,
		leaves:  leaves,
		currIdx: 0,
	}
}

func (t *Tree) collectLeaves(node Node, prefix []byte, depth int, leaves *[]*Leaf) {
	if node == nil {
		return
	}

	if node.Type() == TypeLeaf {
		leaf := node.(*Leaf)
		if bytes.HasPrefix(leaf.Key, prefix) {
			*leaves = append(*leaves, leaf)
		}
		return
	}

	// Traversal in sorted order
	switch n := node.(type) {
	case *Node4:
		for i := 0; i < int(n.num); i++ {
			t.collectLeaves(n.children[i], prefix, depth+1, leaves)
		}
	case *Node16:
		for i := 0; i < int(n.num); i++ {
			t.collectLeaves(n.children[i], prefix, depth+1, leaves)
		}
	case *Node48:
		for b := 0; b < 256; b++ {
			slot := n.childIndex[b]
			if slot != 0xFF {
				t.collectLeaves(n.children[slot], prefix, depth+1, leaves)
			}
		}
	case *Node256:
		for b := 0; b < 256; b++ {
			if n.children[b] != nil {
				t.collectLeaves(n.children[b], prefix, depth+1, leaves)
			}
		}
	}
}

// HasNext returns true if unvisited entries remain.
func (it *Iterator) HasNext() bool {
	return it.currIdx < len(it.leaves)
}

// Next advances and returns the next Leaf.
func (it *Iterator) Next() (*Leaf, bool) {
	if !it.HasNext() {
		return nil, false
	}
	leaf := it.leaves[it.currIdx]
	it.currIdx++
	return leaf, true
}
