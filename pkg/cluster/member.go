package cluster

import (
	"fmt"

	"github.com/Aneesh495/meridian/pkg/core"
)

// MemberStatus describes the lifecycle phase of a cluster node in the SWIM protocol.
type MemberStatus uint8

const (
	StatusAlive MemberStatus = iota
	StatusSuspect
	StatusDead
	StatusLeft
)

func (s MemberStatus) String() string {
	switch s {
	case StatusAlive:
		return "ALIVE"
	case StatusSuspect:
		return "SUSPECT"
	case StatusDead:
		return "DEAD"
	case StatusLeft:
		return "LEFT"
	default:
		return fmt.Sprintf("Status(%d)", s)
	}
}

// Member encapsulates node network identity, incarnation epoch, and health status.
type Member struct {
	ID          core.NodeID
	Addr        string
	Status      MemberStatus
	Incarnation uint64
	Tags        map[string]string
}

// Clone creates a deep copy of the member record.
func (m *Member) Clone() *Member {
	tags := make(map[string]string, len(m.Tags))
	for k, v := range m.Tags {
		tags[k] = v
	}
	return &Member{
		ID:          m.ID,
		Addr:        m.Addr,
		Status:      m.Status,
		Incarnation: m.Incarnation,
		Tags:        tags,
	}
}
