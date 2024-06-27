package wal

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/Aneesh495/meridian/pkg/core"
)

// RecoveryResult contains the state recovered from WAL replay.
type RecoveryResult struct {
	Records   []Record
	LastTerm  core.Term
	LastIndex core.Index
	Segments  []*Segment
}

// Recover scans and recovers all valid WAL entries from the given directory.
func Recover(dir string) (*RecoveryResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return &RecoveryResult{}, nil
		}
		return nil, err
	}

	var segIDs []uint64
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".wal") && !entry.IsDir() {
			base := strings.TrimSuffix(name, ".wal")
			id, err := strconv.ParseUint(base, 10, 64)
			if err == nil {
				segIDs = append(segIDs, id)
			}
		}
	}

	sort.Slice(segIDs, func(i, j int) bool { return segIDs[i] < segIDs[j] })

	var recovered []Record
	var lastTerm core.Term
	var lastIndex core.Index
	var openedSegments []*Segment

	for _, id := range segIDs {
		seg, err := OpenSegment(dir, id, false)
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %d: %w", id, err)
		}
		openedSegments = append(openedSegments, seg)

		reader := NewReader(seg)
		for {
			rec, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Corrupted entry detected at tail: truncate segment to last valid offset
				validOffset := reader.Offset()
				_ = seg.file.Truncate(validOffset)
				seg.size = uint64(validOffset)
				break
			}

			recovered = append(recovered, *rec)
			if rec.Term > lastTerm {
				lastTerm = rec.Term
			}
			if rec.Index > lastIndex {
				lastIndex = rec.Index
			}
		}
	}

	return &RecoveryResult{
		Records:   recovered,
		LastTerm:  lastTerm,
		LastIndex: lastIndex,
		Segments:  openedSegments,
	}, nil
}
