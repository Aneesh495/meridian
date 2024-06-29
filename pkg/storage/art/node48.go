package art

// Node48 stores up to 48 children using an indirect 256-byte index map.
type Node48 struct {
	prefix     Prefix
	childIndex [256]byte
	children   [48]Node
	num        uint8
}

func (n *Node48) Type() NodeType {
	return TypeNode48
}

func (n *Node48) findChild(b byte) (Node, int) {
	idx := n.childIndex[b]
	if idx == 0xFF {
		return nil, -1
	}
	return n.children[idx], int(idx)
}

func (n *Node48) addChild(b byte, child Node) bool {
	if n.num >= 48 {
		return false
	}

	// Find first empty slot in children array
	slot := 0
	for slot < 48 && n.children[slot] != nil {
		slot++
	}
	if slot == 48 {
		return false
	}

	n.children[slot] = child
	n.childIndex[b] = byte(slot)
	n.num++
	return true
}

func (n *Node48) deleteChild(b byte) bool {
	idx := n.childIndex[b]
	if idx == 0xFF {
		return false
	}

	n.children[idx] = nil
	n.childIndex[b] = 0xFF
	n.num--
	return true
}

func (n *Node48) shrink() *Node16 {
	n16 := &Node16{
		prefix: n.prefix,
		num:    n.num,
	}
	idx := 0
	for b := 0; b < 256; b++ {
		slot := n.childIndex[b]
		if slot != 0xFF {
			n16.keys[idx] = byte(b)
			n16.children[idx] = n.children[slot]
			idx++
		}
	}
	return n16
}

func (n *Node48) grow() *Node256 {
	n256 := &Node256{
		prefix: n.prefix,
		num:    uint16(n.num),
	}
	for b := 0; b < 256; b++ {
		slot := n.childIndex[b]
		if slot != 0xFF {
			n256.children[b] = n.children[slot]
		}
	}
	return n256
}
