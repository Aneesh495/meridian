package multiraft

import (
	"bytes"

	"github.com/Aneesh495/meridian/pkg/core"
)

// RangePartition defines a contiguous key range assigned to a consensus shard.
type RangePartition struct {
	ShardID  core.ShardID
	StartKey []byte
	EndKey   []byte // empty EndKey denotes unbounded upper limit
	Peers    []core.NodeID
}

// Contains returns true if key falls within the half-open interval [StartKey, EndKey).
func (p *RangePartition) Contains(key []byte) bool {
	if bytes.Compare(key, p.StartKey) < 0 {
		return false
	}
	if len(p.EndKey) > 0 && bytes.Compare(key, p.EndKey) >= 0 {
		return false
	}
	return true
}
