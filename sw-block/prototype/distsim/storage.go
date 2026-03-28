package distsim

import "sort"

type SnapshotState struct {
	ID    string
	LSN   uint64
	State map[uint64]uint64
}

type Storage struct {
	WAL          []Write
	Extent       map[uint64]uint64
	ReceivedLSN  uint64
	FlushedLSN   uint64
	CheckpointLSN uint64
	Snapshots    map[string]SnapshotState
	BaseSnapshot *SnapshotState
}

func NewStorage() *Storage {
	return &Storage{
		Extent:    map[uint64]uint64{},
		Snapshots: map[string]SnapshotState{},
	}
}

func (s *Storage) AppendWrite(w Write) {
	// Insert in LSN order (handles out-of-order delivery from jitter).
	inserted := false
	for i, existing := range s.WAL {
		if w.LSN == existing.LSN {
			return // duplicate, skip
		}
		if w.LSN < existing.LSN {
			s.WAL = append(s.WAL[:i], append([]Write{w}, s.WAL[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		s.WAL = append(s.WAL, w)
	}
	s.Extent[w.Block] = w.Value
	if w.LSN > s.ReceivedLSN {
		s.ReceivedLSN = w.LSN
	}
}

func (s *Storage) AdvanceFlush(lsn uint64) {
	if lsn > s.ReceivedLSN {
		lsn = s.ReceivedLSN
	}
	if lsn > s.FlushedLSN {
		s.FlushedLSN = lsn
	}
}

func (s *Storage) AdvanceCheckpoint(lsn uint64) {
	if lsn > s.FlushedLSN {
		lsn = s.FlushedLSN
	}
	if lsn > s.CheckpointLSN {
		s.CheckpointLSN = lsn
	}
}

func (s *Storage) StateAt(lsn uint64) map[uint64]uint64 {
	state := map[uint64]uint64{}
	if s.BaseSnapshot != nil {
		if s.BaseSnapshot.LSN > lsn {
			return cloneMap(s.BaseSnapshot.State)
		}
		state = cloneMap(s.BaseSnapshot.State)
	}
	for _, w := range s.WAL {
		if w.LSN > lsn {
			break
		}
		if s.BaseSnapshot != nil && w.LSN <= s.BaseSnapshot.LSN {
			continue
		}
		state[w.Block] = w.Value
	}
	return state
}

func (s *Storage) TakeSnapshot(id string, lsn uint64) SnapshotState {
	snap := SnapshotState{
		ID:    id,
		LSN:   lsn,
		State: cloneMap(s.StateAt(lsn)),
	}
	s.Snapshots[id] = snap
	return snap
}

func (s *Storage) LoadSnapshot(snap SnapshotState) {
	s.Extent = cloneMap(snap.State)
	s.FlushedLSN = snap.LSN
	s.ReceivedLSN = snap.LSN
	s.CheckpointLSN = snap.LSN
	s.BaseSnapshot = &SnapshotState{
		ID:    snap.ID,
		LSN:   snap.LSN,
		State: cloneMap(snap.State),
	}
	s.WAL = nil
}

func (s *Storage) ReplaceWAL(writes []Write) {
	s.WAL = append([]Write(nil), writes...)
	sort.Slice(s.WAL, func(i, j int) bool { return s.WAL[i].LSN < s.WAL[j].LSN })
	s.Extent = s.StateAt(s.ReceivedLSN)
}

func writesInRange(writes []Write, startExclusive, endInclusive uint64) []Write {
	out := make([]Write, 0)
	for _, w := range writes {
		if w.LSN <= startExclusive {
			continue
		}
		if w.LSN > endInclusive {
			break
		}
		out = append(out, w)
	}
	return out
}
