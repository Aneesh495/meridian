package art

// NodeType identifies the adaptive capacity of an internal radix node.
type NodeType uint8

const (
	TypeLeaf NodeType = iota
	TypeNode4
	TypeNode16
	TypeNode48
	TypeNode256
)

// Node is the universal interface implemented by all ART nodes.
type Node interface {
	Type() NodeType
}

// Leaf represents a terminal key-value mapping.
type Leaf struct {
	Key   []byte
	Value []byte
}

func (l *Leaf) Type() NodeType {
	return TypeLeaf
}

// Prefix represents a shared compressed key path common to all children.
type Prefix struct {
	data   []byte
	length int
}

func (p *Prefix) match(key []byte, depth int) int {
	idx := 0
	for idx < p.length && depth+idx < len(key) {
		if p.data[idx] != key[depth+idx] {
			return idx
		}
		idx++
	}
	return idx
}
