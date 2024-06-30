package art

// Node16 stores up to 16 children with keys kept in sorted order.
type Node16 struct {
	prefix   Prefix
	keys     [16]byte
	children [16]Node
	num      uint8
	val      []byte
	hasVal   bool
}

func (n *Node16) Type() NodeType {
	return TypeNode16
}

func (n *Node16) findChild(b byte) (Node, int) {
	for i := 0; i < int(n.num); i++ {
		if n.keys[i] == b {
			return n.children[i], i
		}
	}
	return nil, -1
}

func (n *Node16) addChild(b byte, child Node) bool {
	if n.num >= 16 {
		return false
	}

	idx := 0
	for idx < int(n.num) && n.keys[idx] < b {
		idx++
	}

	for i := int(n.num); i > idx; i-- {
		n.keys[i] = n.keys[i-1]
		n.children[i] = n.children[i-1]
	}

	n.keys[idx] = b
	n.children[idx] = child
	n.num++
	return true
}

func (n *Node16) deleteChild(b byte) bool {
	_, idx := n.findChild(b)
	if idx == -1 {
		return false
	}

	for i := idx; i < int(n.num)-1; i++ {
		n.keys[i] = n.keys[i+1]
		n.children[i] = n.children[i+1]
	}
	n.children[n.num-1] = nil
	n.num--
	return true
}

func (n *Node16) shrink() *Node4 {
	n4 := &Node4{
		prefix: n.prefix,
		num:    n.num,
		val:    n.val,
		hasVal: n.hasVal,
	}
	copy(n4.keys[:], n.keys[:n.num])
	copy(n4.children[:], n.children[:n.num])
	return n4
}

func (n *Node16) grow() *Node48 {
	n48 := &Node48{
		prefix: n.prefix,
		num:    n.num,
		val:    n.val,
		hasVal: n.hasVal,
	}
	for i := range n48.childIndex {
		n48.childIndex[i] = 0xFF
	}
	for i := 0; i < int(n.num); i++ {
		n48.childIndex[n.keys[i]] = byte(i)
		n48.children[i] = n.children[i]
	}
	return n48
}
