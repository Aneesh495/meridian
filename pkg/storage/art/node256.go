package art

// Node256 stores up to 256 direct children indexed by byte value.
type Node256 struct {
	prefix   Prefix
	children [256]Node
	num      uint16
	val      []byte
	hasVal   bool
}

func (n *Node256) Type() NodeType {
	return TypeNode256
}

func (n *Node256) findChild(b byte) (Node, int) {
	child := n.children[b]
	if child == nil {
		return nil, -1
	}
	return child, int(b)
}

func (n *Node256) addChild(b byte, child Node) bool {
	if n.children[b] == nil {
		n.num++
	}
	n.children[b] = child
	return true
}

func (n *Node256) deleteChild(b byte) bool {
	if n.children[b] == nil {
		return false
	}
	n.children[b] = nil
	n.num--
	return true
}

func (n *Node256) shrink() *Node48 {
	n48 := &Node48{
		prefix: n.prefix,
		num:    uint8(n.num),
		val:    n.val,
		hasVal: n.hasVal,
	}
	for i := range n48.childIndex {
		n48.childIndex[i] = 0xFF
	}

	slot := 0
	for b := 0; b < 256; b++ {
		if n.children[b] != nil {
			n48.childIndex[b] = byte(slot)
			n48.children[slot] = n.children[b]
			slot++
		}
	}
	return n48
}
