package art

import (
	"bytes"
	"sync"
)

// Tree represents a synchronized Adaptive Radix Tree.
type Tree struct {
	mu   sync.RWMutex
	root Node
	size int
}

// NewTree creates an empty Adaptive Radix Tree.
func NewTree() *Tree {
	return &Tree{}
}

// Size returns the total count of key-value pairs stored.
func (t *Tree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}

// Search retrieves the value associated with key.
func (t *Tree) Search(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	curr := t.root
	depth := 0

	for curr != nil {
		if curr.Type() == TypeLeaf {
			leaf := curr.(*Leaf)
			if bytes.Equal(leaf.Key, key) {
				return leaf.Value, true
			}
			return nil, false
		}

		prefix := getNodePrefix(curr)
		if prefix.length > 0 {
			matchLen := prefix.match(key, depth)
			if matchLen < prefix.length {
				return nil, false
			}
			depth += prefix.length
		}

		if depth == len(key) {
			val, has := getNodeValue(curr)
			return val, has
		}

		b := key[depth]
		curr = findChild(curr, b)
		depth++
	}

	return nil, false
}

// Insert associates key with value, returning previous value if updated.
func (t *Tree) Insert(key []byte, value []byte) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var oldVal []byte
	var updated bool
	t.root, oldVal, updated = t.insertRecursive(t.root, key, value, 0)
	if !updated {
		t.size++
	}
	return oldVal, updated
}

func (t *Tree) insertRecursive(node Node, key []byte, value []byte, depth int) (Node, []byte, bool) {
	if node == nil {
		return &Leaf{Key: cloneBytes(key), Value: cloneBytes(value)}, nil, false
	}

	if node.Type() == TypeLeaf {
		leaf := node.(*Leaf)
		if bytes.Equal(leaf.Key, key) {
			old := leaf.Value
			leaf.Value = cloneBytes(value)
			return leaf, old, true
		}

		// New internal Node4 to split the existing leaf and new key
		newN4 := &Node4{}
		commonLen := 0
		for depth+commonLen < len(key) && depth+commonLen < len(leaf.Key) && key[depth+commonLen] == leaf.Key[depth+commonLen] {
			commonLen++
		}

		if commonLen > 0 {
			newN4.prefix = Prefix{
				data:   cloneBytes(key[depth : depth+commonLen]),
				length: commonLen,
			}
		}

		depth += commonLen

		if depth == len(key) {
			newN4.hasVal = true
			newN4.val = cloneBytes(value)
			if depth < len(leaf.Key) {
				newN4.addChild(leaf.Key[depth], leaf)
			}
			return newN4, nil, false
		}

		if depth == len(leaf.Key) {
			newN4.hasVal = true
			newN4.val = leaf.Value
			newN4.addChild(key[depth], &Leaf{Key: cloneBytes(key), Value: cloneBytes(value)})
			return newN4, nil, false
		}

		newN4.addChild(leaf.Key[depth], leaf)
		newN4.addChild(key[depth], &Leaf{Key: cloneBytes(key), Value: cloneBytes(value)})
		return newN4, nil, false
	}

	// Internal node
	prefix := getNodePrefix(node)
	if prefix.length > 0 {
		matchLen := prefix.match(key, depth)
		if matchLen < prefix.length {
			splitNode := &Node4{
				prefix: Prefix{
					data:   cloneBytes(prefix.data[:matchLen]),
					length: matchLen,
				},
			}

			oldPrefixByte := prefix.data[matchLen]
			remainingPrefix := Prefix{
				data:   cloneBytes(prefix.data[matchLen+1:]),
				length: prefix.length - matchLen - 1,
			}
			setNodePrefix(node, remainingPrefix)
			splitNode.addChild(oldPrefixByte, node)

			if depth+matchLen == len(key) {
				splitNode.hasVal = true
				splitNode.val = cloneBytes(value)
			} else {
				newByte := key[depth+matchLen]
				splitNode.addChild(newByte, &Leaf{Key: cloneBytes(key), Value: cloneBytes(value)})
			}
			return splitNode, nil, false
		}
		depth += prefix.length
	}

	if depth == len(key) {
		oldVal, hadVal := getNodeValue(node)
		setNodeValue(node, cloneBytes(value), true)
		return node, oldVal, hadVal
	}

	b := key[depth]
	child := findChild(node, b)
	if child != nil {
		newChild, oldVal, updated := t.insertRecursive(child, key, value, depth+1)
		replaceChild(node, b, newChild)
		return node, oldVal, updated
	}

	newLeaf := &Leaf{Key: cloneBytes(key), Value: cloneBytes(value)}
	newNode := addChildOrGrow(node, b, newLeaf)
	return newNode, nil, false
}

// Delete removes the key, returning deleted value.
func (t *Tree) Delete(key []byte) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var deletedVal []byte
	var deleted bool
	t.root, deletedVal, deleted = t.deleteRecursive(t.root, key, 0)
	if deleted {
		t.size--
	}
	return deletedVal, deleted
}

func (t *Tree) deleteRecursive(node Node, key []byte, depth int) (Node, []byte, bool) {
	if node == nil {
		return nil, nil, false
	}

	if node.Type() == TypeLeaf {
		leaf := node.(*Leaf)
		if bytes.Equal(leaf.Key, key) {
			return nil, leaf.Value, true
		}
		return node, nil, false
	}

	prefix := getNodePrefix(node)
	if prefix.length > 0 {
		matchLen := prefix.match(key, depth)
		if matchLen < prefix.length {
			return node, nil, false
		}
		depth += prefix.length
	}

	if depth == len(key) {
		val, hadVal := getNodeValue(node)
		if hadVal {
			setNodeValue(node, nil, false)
			return node, val, true
		}
		return node, nil, false
	}

	b := key[depth]
	child := findChild(node, b)
	if child == nil {
		return node, nil, false
	}

	newChild, val, deleted := t.deleteRecursive(child, key, depth+1)
	if !deleted {
		return node, nil, false
	}

	if newChild == nil {
		deleteChild(node, b)
	} else {
		replaceChild(node, b, newChild)
	}

	return node, val, true
}

func getNodeValue(node Node) ([]byte, bool) {
	switch n := node.(type) {
	case *Node4:
		return n.val, n.hasVal
	case *Node16:
		return n.val, n.hasVal
	case *Node48:
		return n.val, n.hasVal
	case *Node256:
		return n.val, n.hasVal
	}
	return nil, false
}

func setNodeValue(node Node, val []byte, has bool) {
	switch n := node.(type) {
	case *Node4:
		n.val = val
		n.hasVal = has
	case *Node16:
		n.val = val
		n.hasVal = has
	case *Node48:
		n.val = val
		n.hasVal = has
	case *Node256:
		n.val = val
		n.hasVal = has
	}
}

func findChild(node Node, b byte) Node {
	switch n := node.(type) {
	case *Node4:
		c, _ := n.findChild(b)
		return c
	case *Node16:
		c, _ := n.findChild(b)
		return c
	case *Node48:
		c, _ := n.findChild(b)
		return c
	case *Node256:
		c, _ := n.findChild(b)
		return c
	}
	return nil
}

func replaceChild(node Node, b byte, child Node) {
	switch n := node.(type) {
	case *Node4:
		_, idx := n.findChild(b)
		if idx != -1 {
			n.children[idx] = child
		}
	case *Node16:
		_, idx := n.findChild(b)
		if idx != -1 {
			n.children[idx] = child
		}
	case *Node48:
		_, idx := n.findChild(b)
		if idx != -1 {
			n.children[idx] = child
		}
	case *Node256:
		n.children[b] = child
	}
}

func deleteChild(node Node, b byte) {
	switch n := node.(type) {
	case *Node4:
		n.deleteChild(b)
	case *Node16:
		n.deleteChild(b)
	case *Node48:
		n.deleteChild(b)
	case *Node256:
		n.deleteChild(b)
	}
}

func addChildOrGrow(node Node, b byte, child Node) Node {
	switch n := node.(type) {
	case *Node4:
		if n.addChild(b, child) {
			return n
		}
		n16 := n.grow()
		n16.addChild(b, child)
		return n16
	case *Node16:
		if n.addChild(b, child) {
			return n
		}
		n48 := n.grow()
		n48.addChild(b, child)
		return n48
	case *Node48:
		if n.addChild(b, child) {
			return n
		}
		n256 := n.grow()
		n256.addChild(b, child)
		return n256
	case *Node256:
		n.addChild(b, child)
		return n
	}
	return node
}

func getNodePrefix(node Node) Prefix {
	switch n := node.(type) {
	case *Node4:
		return n.prefix
	case *Node16:
		return n.prefix
	case *Node48:
		return n.prefix
	case *Node256:
		return n.prefix
	}
	return Prefix{}
}

func setNodePrefix(node Node, p Prefix) {
	switch n := node.(type) {
	case *Node4:
		n.prefix = p
	case *Node16:
		n.prefix = p
	case *Node48:
		n.prefix = p
	case *Node256:
		n.prefix = p
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}
