package art

// Node4 stores up to 4 children with keys kept in sorted order.
type Node4 struct {
	prefix   Prefix
	keys     [4]byte
	children [4]Node
	num      uint8
	val      []byte
	hasVal   bool
}

func (n *Node4) Type() NodeType {
	return TypeNode4
}

func (n *Node4) findChild(b byte) (Node, int) {
	for i := 0; i < int(n.num); i++ {
		if n.keys[i] == b {
			return n.children[i], i
		}
	}
	return nil, -1
}

func (n *Node4) addChild(b byte, child Node) bool {
	if n.num >= 4 {
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

func (n *Node4) deleteChild(b byte) bool {
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

func (n *Node4) grow() *Node16 {
	n16 := &Node16{
		prefix: n.prefix,
		num:    n.num,
		val:    n.val,
		hasVal: n.hasVal,
	}
	copy(n16.keys[:], n.keys[:n.num])
	copy(n16.children[:], n.children[:n.num])
	return n16
}
