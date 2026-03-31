package distsim

import "sort"

type SnapshotState struct {
	ID    string
	LSN   uint64
	State map[uint64]uint64
}

// Storage models the per-node storage state with explicit crash-consistency
// boundaries. Phase 4.5: split into 5 distinct LSN boundaries.
//
// State progression:
//   Write arrives → ReceivedLSN (not yet durable)
//   WAL fsync     → WALDurableLSN (survives crash)
//   Flusher       → ExtentAppliedLSN (materialized to live extent, volatile)
//   Checkpoint    → CheckpointLSN (durable base image)
//
// After crash + restart:
//   RecoverableState = CheckpointExtent + WAL[CheckpointLSN+1 .. WALDurableLSN]
type Storage struct {
	WAL              []Write
	LiveExtent       map[uint64]uint64 // runtime view (volatile — lost on crash)
	CheckpointExtent map[uint64]uint64 // crash-safe base image (survives crash)
	ReceivedLSN      uint64            // highest LSN received (may not be durable)
	WALDurableLSN    uint64            // highest LSN guaranteed to survive crash (= FlushedLSN)
	ExtentAppliedLSN uint64            // highest LSN materialized into LiveExtent
	CheckpointLSN    uint64            // highest LSN in the durable base image
	Snapshots        map[string]SnapshotState
	BaseSnapshot     *SnapshotState

	// Backward compat alias.
	FlushedLSN uint64 // = WALDurableLSN
}

func NewStorage() *Storage {
	return &Storage{
		LiveExtent:       map[uint64]uint64{},
		CheckpointExtent: map[uint64]uint64{},
		Snapshots:        map[string]SnapshotState{},
	}
}

// AppendWrite adds a WAL entry. Does NOT update LiveExtent — that's the flusher's job.
// Does NOT advance WALDurableLSN — that requires explicit AdvanceFlush (WAL fsync).
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
	if w.LSN > s.ReceivedLSN {
		s.ReceivedLSN = w.LSN
	}
}

// AdvanceFlush simulates WAL fdatasync completing. Entries up to lsn are now
// durable and will survive crash. This is the authoritative progress for sync_all.
func (s *Storage) AdvanceFlush(lsn uint64) {
	if lsn > s.ReceivedLSN {
		lsn = s.ReceivedLSN
	}
	if lsn > s.WALDurableLSN {
		s.WALDurableLSN = lsn
		s.FlushedLSN = lsn // backward compat alias
	}
}

// ApplyToExtent simulates the flusher materializing WAL entries into the live extent.
// Entries from (ExtentAppliedLSN, targetLSN] are applied. This is a volatile operation —
// LiveExtent is lost on crash.
func (s *Storage) ApplyToExtent(targetLSN uint64) {
	if targetLSN > s.WALDurableLSN {
		targetLSN = s.WALDurableLSN // can't materialize un-durable entries
	}
	for _, w := range s.WAL {
		if w.LSN <= s.ExtentAppliedLSN {
			continue
		}
		if w.LSN > targetLSN {
			break
		}
		s.LiveExtent[w.Block] = w.Value
	}
	if targetLSN > s.ExtentAppliedLSN {
		s.ExtentAppliedLSN = targetLSN
	}
}

// AdvanceCheckpoint creates a crash-safe base image at exactly the given LSN.
// The checkpoint image contains state ONLY through lsn — not the full LiveExtent.
// This is critical: LiveExtent may contain applied entries beyond lsn that are
// NOT part of the checkpoint and must NOT survive a crash.
func (s *Storage) AdvanceCheckpoint(lsn uint64) {
	if lsn > s.ExtentAppliedLSN {
		lsn = s.ExtentAppliedLSN
	}
	if lsn > s.CheckpointLSN {
		s.CheckpointLSN = lsn
		// Build checkpoint image from base + WAL replay through exactly lsn.
		// Do NOT clone LiveExtent — it may contain entries beyond checkpoint.
		s.CheckpointExtent = s.StateAt(lsn)
		// Set BaseSnapshot so StateAt() can use it after WAL GC.
		s.BaseSnapshot = &SnapshotState{
			ID:    "checkpoint",
			LSN:   lsn,
			State: cloneMap(s.CheckpointExtent),
		}
	}
}

// Crash simulates a node crash: LiveExtent is lost, only CheckpointExtent
// and durable WAL entries survive.
func (s *Storage) Crash() {
	s.LiveExtent = nil
	s.ExtentAppliedLSN = 0
	// ReceivedLSN drops to WALDurableLSN (un-fsynced entries lost)
	s.ReceivedLSN = s.WALDurableLSN
	// Remove non-durable WAL entries
	durable := make([]Write, 0, len(s.WAL))
	for _, w := range s.WAL {
		if w.LSN <= s.WALDurableLSN {
			durable = append(durable, w)
		}
	}
	s.WAL = durable
}

// Restart recovers state from CheckpointExtent + durable WAL replay.
// Sets BaseSnapshot from checkpoint so StateAt() works after WAL GC.
// Returns the RecoverableLSN (highest LSN in the recovered view).
func (s *Storage) Restart() uint64 {
	// Start from checkpoint base image.
	s.LiveExtent = cloneMap(s.CheckpointExtent)
	// Set BaseSnapshot so StateAt() can reconstruct from checkpoint after WAL GC.
	s.BaseSnapshot = &SnapshotState{
		ID:    "checkpoint",
		LSN:   s.CheckpointLSN,
		State: cloneMap(s.CheckpointExtent),
	}
	// Replay durable WAL entries past checkpoint.
	for _, w := range s.WAL {
		if w.LSN <= s.CheckpointLSN {
			continue
		}
		if w.LSN > s.WALDurableLSN {
			break
		}
		s.LiveExtent[w.Block] = w.Value
	}
	s.ExtentAppliedLSN = s.WALDurableLSN
	return s.WALDurableLSN
}

// RecoverableLSN returns the highest LSN that would be recoverable after
// crash + restart. This is a replayability proof, not just a watermark:
//   - CheckpointExtent covers [0, CheckpointLSN]
//   - WAL entries (CheckpointLSN, WALDurableLSN] must exist contiguously
//   - If any gap exists in the WAL between CheckpointLSN and WALDurableLSN,
//     recovery would be incomplete
//
// Returns the highest contiguously recoverable LSN from checkpoint + WAL.
func (s *Storage) RecoverableLSN() uint64 {
	// Start from checkpoint — everything through CheckpointLSN is safe.
	recoverable := s.CheckpointLSN

	// Walk durable WAL entries past checkpoint and verify contiguity.
	for _, w := range s.WAL {
		if w.LSN <= s.CheckpointLSN {
			continue // already covered by checkpoint
		}
		if w.LSN > s.WALDurableLSN {
			break // not durable
		}
		if w.LSN == recoverable+1 {
			recoverable = w.LSN // contiguous — extend
		} else {
			break // gap — stop here
		}
	}
	return recoverable
}

// StateAt computes the block state by replaying WAL entries up to the given LSN.
// Used for correctness assertions against the reference model.
//
// Phase 4.5: for lsn < CheckpointLSN (after WAL GC), the WAL entries needed
// to reconstruct historical state may no longer exist. In that case, we return
// the checkpoint state (best available), but callers should use
// CanReconstructAt(lsn) to check if the result is authoritative.
func (s *Storage) StateAt(lsn uint64) map[uint64]uint64 {
	state := map[uint64]uint64{}
	usedSnapshot := false
	if s.BaseSnapshot != nil {
		if s.BaseSnapshot.LSN > lsn {
			// Snapshot is NEWER than requested — cannot use it.
			// Fall through to WAL-only replay.
		} else {
			state = cloneMap(s.BaseSnapshot.State)
			usedSnapshot = true
		}
	}
	for _, w := range s.WAL {
		if w.LSN > lsn {
			break
		}
		if usedSnapshot && w.LSN <= s.BaseSnapshot.LSN {
			continue
		}
		state[w.Block] = w.Value
	}
	return state
}

// CanReconstructAt returns true if the storage has enough information to
// accurately reconstruct state at the given LSN. False means the WAL entries
// needed for historical reconstruction have been GC'd and StateAt(lsn) may
// return an approximation (checkpoint state) rather than exact history.
//
// A7 (Historical Data Correctness): this should be checked before trusting
// StateAt() results for old LSNs. Current extent cannot fake old history.
func (s *Storage) CanReconstructAt(lsn uint64) bool {
	if lsn == 0 {
		return true // empty state is always reconstructable
	}

	// To reconstruct state at exactly lsn, we need a contiguous chain of
	// evidence from LSN 0 (or a snapshot taken AT lsn) through lsn.
	//
	// A checkpoint at LSN C contains state through C. If lsn < C, the
	// checkpoint has MORE data than existed at lsn — it cannot reconstruct
	// the exact historical state at lsn. We would need WAL entries [1, lsn]
	// to rebuild from scratch, which are gone after GC.
	//
	// A checkpoint at LSN C where C == lsn is exact.
	// A checkpoint at LSN C where C > lsn cannot help with exact lsn state.

	// Check if any snapshot was taken exactly at this LSN.
	for _, snap := range s.Snapshots {
		if snap.LSN == lsn {
			return true
		}
	}

	// Find the best base: a snapshot/checkpoint at or before lsn.
	baseLSN := uint64(0)
	if s.BaseSnapshot != nil && s.BaseSnapshot.LSN <= lsn {
		baseLSN = s.BaseSnapshot.LSN
	}

	// If baseLSN > 0, we have a snapshot that provides state through baseLSN.
	// We need contiguous WAL from baseLSN+1 through lsn.
	// If baseLSN == 0, we need contiguous WAL from 1 through lsn.

	expected := baseLSN + 1
	for _, w := range s.WAL {
		if w.LSN <= baseLSN {
			continue
		}
		if w.LSN > lsn {
			break
		}
		if w.LSN != expected {
			return false // gap — history is incomplete
		}
		expected = w.LSN + 1
	}
	return expected > lsn
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
	s.LiveExtent = cloneMap(snap.State)
	s.CheckpointExtent = cloneMap(snap.State)
	s.WALDurableLSN = snap.LSN
	s.FlushedLSN = snap.LSN
	s.ReceivedLSN = snap.LSN
	s.CheckpointLSN = snap.LSN
	s.ExtentAppliedLSN = snap.LSN
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
	// Recompute LiveExtent from base + WAL
	s.LiveExtent = s.StateAt(s.ReceivedLSN)
}

// Extent returns the current live extent for backward compatibility.
// Callers should migrate to LiveExtent.
func (s *Storage) Extent() map[uint64]uint64 {
	return s.LiveExtent
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
