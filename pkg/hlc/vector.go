package hlc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	"github.com/Aneesh495/meridian/pkg/core"
)

// VectorClock maintains causal version vectors across cluster nodes.
type VectorClock struct {
	versions map[core.NodeID]uint64
}

// NewVectorClock creates an empty VectorClock.
func NewVectorClock() *VectorClock {
	return &VectorClock{
		versions: make(map[core.NodeID]uint64),
	}
}

// Clone creates an independent deep copy of the vector clock.
func (vc *VectorClock) Clone() *VectorClock {
	cpy := make(map[core.NodeID]uint64, len(vc.versions))
	for k, v := range vc.versions {
		cpy[k] = v
	}
	return &VectorClock{versions: cpy}
}

// Get returns the logical version for the given node.
func (vc *VectorClock) Get(node core.NodeID) uint64 {
	return vc.versions[node]
}

// Set explicitly sets the version for a node.
func (vc *VectorClock) Set(node core.NodeID, version uint64) {
	vc.versions[node] = version
}

// Increment advances the version of the specified node by 1.
func (vc *VectorClock) Increment(node core.NodeID) uint64 {
	vc.versions[node]++
	return vc.versions[node]
}

// Merge takes the element-wise maximum across all nodes between vc and other.
func (vc *VectorClock) Merge(other *VectorClock) {
	if other == nil {
		return
	}
	for node, ver := range other.versions {
		if ver > vc.versions[node] {
			vc.versions[node] = ver
		}
	}
}

// Equal returns true if both vector clocks are identical.
func (vc *VectorClock) Equal(other *VectorClock) bool {
	if other == nil {
		return len(vc.versions) == 0
	}
	for node, v := range vc.versions {
		if other.versions[node] != v {
			return false
		}
	}
	for node, v := range other.versions {
		if vc.versions[node] != v {
			return false
		}
	}
	return true
}

// Dominates returns true if vc is strictly causally after other:
// vc >= other on all components, and vc > other on at least one component.
func (vc *VectorClock) Dominates(other *VectorClock) bool {
	if other == nil {
		return len(vc.versions) > 0
	}

	greaterOnAtLeastOne := false

	// Check all nodes in other
	for node, otherVer := range other.versions {
		vcVer := vc.versions[node]
		if vcVer < otherVer {
			return false
		}
		if vcVer > otherVer {
			greaterOnAtLeastOne = true
		}
	}

	// Check any nodes in vc that other does not have
	for node, vcVer := range vc.versions {
		if _, ok := other.versions[node]; !ok && vcVer > 0 {
			greaterOnAtLeastOne = true
		}
	}

	return greaterOnAtLeastOne
}

// ConcurrentWith returns true if neither vc dominates other nor other dominates vc.
func (vc *VectorClock) ConcurrentWith(other *VectorClock) bool {
	if vc.Equal(other) {
		return false
	}
	return !vc.Dominates(other) && !other.Dominates(vc)
}

// String formats the vector clock deterministically sorted by NodeID.
func (vc *VectorClock) String() string {
	keys := make([]uint64, 0, len(vc.versions))
	for k := range vc.versions {
		keys = append(keys, uint64(k))
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	var sb strings.Builder
	sb.WriteString("[")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%d:%d", k, vc.versions[core.NodeID(k)]))
	}
	sb.WriteString("]")
	return sb.String()
}

// Encode binary encodes the vector clock.
func (vc *VectorClock) Encode() []byte {
	buf := new(bytes.Buffer)
	// Write number of entries
	_ = binary.Write(buf, binary.BigEndian, uint32(len(vc.versions)))

	// Sort keys for deterministic byte encoding
	keys := make([]uint64, 0, len(vc.versions))
	for k := range vc.versions {
		keys = append(keys, uint64(k))
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, k := range keys {
		_ = binary.Write(buf, binary.BigEndian, k)
		_ = binary.Write(buf, binary.BigEndian, vc.versions[core.NodeID(k)])
	}
	return buf.Bytes()
}

// DecodeVectorClock restores a vector clock from binary bytes.
func DecodeVectorClock(data []byte) (*VectorClock, error) {
	if len(data) < 4 {
		return nil, core.ErrBufferOverflow
	}
	count := binary.BigEndian.Uint32(data[0:4])
	expectedLen := 4 + int(count)*16
	if len(data) < expectedLen {
		return nil, core.ErrBufferOverflow
	}

	vc := NewVectorClock()
	offset := 4
	for i := uint32(0); i < count; i++ {
		nodeID := binary.BigEndian.Uint64(data[offset : offset+8])
		ver := binary.BigEndian.Uint64(data[offset+8 : offset+16])
		vc.versions[core.NodeID(nodeID)] = ver
		offset += 16
	}
	return vc, nil
}
