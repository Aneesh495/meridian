package hlc

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/Aneesh495/meridian/pkg/core"
)

// Timestamp represents a Hybrid Logical Clock timestamp.
// It provides a total order over all distributed events.
type Timestamp struct {
	WallTime int64       // Physical wall-clock time in nanoseconds
	Logical  uint16      // Logical counter for events occurring at identical physical times
	NodeID   core.NodeID // Node identifier for deterministic tie-breaking
}

// ZeroTimestamp is the initial zero-valued timestamp.
var ZeroTimestamp = Timestamp{}

// NewTimestamp creates a new Timestamp.
func NewTimestamp(wallTime int64, logical uint16, nodeID core.NodeID) Timestamp {
	return Timestamp{
		WallTime: wallTime,
		Logical:  logical,
		NodeID:   nodeID,
	}
}

// Less returns true if t is strictly causally before other.
func (t Timestamp) Less(other Timestamp) bool {
	if t.WallTime != other.WallTime {
		return t.WallTime < other.WallTime
	}
	if t.Logical != other.Logical {
		return t.Logical < other.Logical
	}
	return t.NodeID < other.NodeID
}

// Equal returns true if t and other are identical.
func (t Timestamp) Equal(other Timestamp) bool {
	return t.WallTime == other.WallTime && t.Logical == other.Logical && t.NodeID == other.NodeID
}

// Greater returns true if t is strictly causally after other.
func (t Timestamp) Greater(other Timestamp) bool {
	return other.Less(t)
}

// IsZero returns true if the timestamp is unset.
func (t Timestamp) IsZero() bool {
	return t.WallTime == 0 && t.Logical == 0 && t.NodeID == 0
}

// String returns a human-readable representation: <wall>.<logical>@<node>.
func (t Timestamp) String() string {
	return fmt.Sprintf("%d.%d@%d", t.WallTime, t.Logical, t.NodeID)
}

// ParseTimestamp parses a timestamp from string format.
func ParseTimestamp(s string) (Timestamp, error) {
	atIdx := strings.LastIndex(s, "@")
	if atIdx == -1 {
		return ZeroTimestamp, fmt.Errorf("malformed timestamp, missing @: %s", s)
	}

	nodePart := s[atIdx+1:]
	nodeID, err := strconv.ParseUint(nodePart, 10, 64)
	if err != nil {
		return ZeroTimestamp, fmt.Errorf("invalid node ID in timestamp: %w", err)
	}

	timePart := s[:atIdx]
	dotIdx := strings.Index(timePart, ".")
	if dotIdx == -1 {
		return ZeroTimestamp, fmt.Errorf("malformed timestamp, missing dot: %s", s)
	}

	wallTime, err := strconv.ParseInt(timePart[:dotIdx], 10, 64)
	if err != nil {
		return ZeroTimestamp, fmt.Errorf("invalid wall time in timestamp: %w", err)
	}

	logical, err := strconv.ParseUint(timePart[dotIdx+1:], 10, 16)
	if err != nil {
		return ZeroTimestamp, fmt.Errorf("invalid logical counter in timestamp: %w", err)
	}

	return Timestamp{
		WallTime: wallTime,
		Logical:  uint16(logical),
		NodeID:   core.NodeID(nodeID),
	}, nil
}

// BinarySize is the fixed size in bytes of an encoded Timestamp (18 bytes).
const BinarySize = 8 + 2 + 8

// Encode writes the timestamp into a fixed 18-byte slice.
func (t Timestamp) Encode(dst []byte) {
	if len(dst) < BinarySize {
		panic("destination slice too small for timestamp encoding")
	}
	binary.BigEndian.PutUint64(dst[0:8], uint64(t.WallTime))
	binary.BigEndian.PutUint16(dst[8:10], t.Logical)
	binary.BigEndian.PutUint64(dst[10:18], uint64(t.NodeID))
}

// Decode reads a Timestamp from an 18-byte slice.
func Decode(src []byte) (Timestamp, error) {
	if len(src) < BinarySize {
		return ZeroTimestamp, core.ErrBufferOverflow
	}
	return Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(src[0:8])),
		Logical:  binary.BigEndian.Uint16(src[8:10]),
		NodeID:   core.NodeID(binary.BigEndian.Uint64(src[10:18])),
	}, nil
}
