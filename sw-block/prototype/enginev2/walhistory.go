package enginev2

import "fmt"

// WALEntry represents a single write in the WAL history.
type WALEntry struct {
	LSN   uint64
	Epoch uint64
	Block uint64
	Value uint64
}

// WALHistory is a minimal retained-prefix model for proving recoverability.
// It tracks which LSN ranges are available for catch-up and which have been
// recycled (requiring rebuild). This is the data model behind
// ClassifyRecoveryOutcome — it makes "why recovery is allowed" executable.
type WALHistory struct {
	entries      []WALEntry
	headLSN      uint64 // highest LSN written
	tailLSN      uint64 // oldest retained LSN (exclusive: entries with LSN > tailLSN are kept)
	committedLSN uint64 // lineage-safe boundary
}

// NewWALHistory creates an empty WAL history.
func NewWALHistory() *WALHistory {
	return &WALHistory{}
}

// Append adds a WAL entry. LSN must be strictly greater than headLSN.
func (w *WALHistory) Append(entry WALEntry) error {
	if entry.LSN <= w.headLSN {
		return fmt.Errorf("WAL: append LSN %d <= head %d", entry.LSN, w.headLSN)
	}
	w.entries = append(w.entries, entry)
	w.headLSN = entry.LSN
	return nil
}

// Commit advances the committed boundary. Must be <= headLSN.
func (w *WALHistory) Commit(lsn uint64) error {
	if lsn > w.headLSN {
		return fmt.Errorf("WAL: commit LSN %d > head %d", lsn, w.headLSN)
	}
	if lsn > w.committedLSN {
		w.committedLSN = lsn
	}
	return nil
}

// AdvanceTail recycles entries at or below lsn. After this, entries with
// LSN <= lsn are no longer available for catch-up recovery.
func (w *WALHistory) AdvanceTail(lsn uint64) {
	if lsn <= w.tailLSN {
		return
	}
	w.tailLSN = lsn
	// Remove recycled entries from storage.
	kept := w.entries[:0]
	for _, e := range w.entries {
		if e.LSN > lsn {
			kept = append(kept, e)
		}
	}
	w.entries = kept
}

// Truncate removes entries with LSN > afterLSN. Used to clean divergent
// tail on a replica that is ahead of the committed boundary.
func (w *WALHistory) Truncate(afterLSN uint64) {
	kept := w.entries[:0]
	for _, e := range w.entries {
		if e.LSN <= afterLSN {
			kept = append(kept, e)
		}
	}
	w.entries = kept
	if afterLSN < w.headLSN {
		w.headLSN = afterLSN
	}
}

// EntriesInRange returns entries with startExclusive < LSN <= endInclusive.
// Returns nil if any entry in the range has been recycled.
func (w *WALHistory) EntriesInRange(startExclusive, endInclusive uint64) ([]WALEntry, error) {
	if startExclusive < w.tailLSN {
		return nil, fmt.Errorf("WAL: range start %d < tail %d (recycled)", startExclusive, w.tailLSN)
	}
	var result []WALEntry
	for _, e := range w.entries {
		if e.LSN <= startExclusive {
			continue
		}
		if e.LSN > endInclusive {
			break
		}
		result = append(result, e)
	}
	return result, nil
}

// IsRecoverable checks whether all entries from startExclusive+1 to
// endInclusive are retained in the WAL. This is the executable proof
// of "why catch-up is allowed."
func (w *WALHistory) IsRecoverable(startExclusive, endInclusive uint64) bool {
	return startExclusive >= w.tailLSN
}

// MakeHandshakeResult generates a HandshakeResult from the WAL state
// and a replica's reported flushed LSN. This connects the data model
// to the outcome classification.
func (w *WALHistory) MakeHandshakeResult(replicaFlushedLSN uint64) HandshakeResult {
	retentionStart := w.tailLSN + 1 // first available LSN
	if w.tailLSN == 0 {
		retentionStart = 0 // all history retained
	}
	return HandshakeResult{
		ReplicaFlushedLSN: replicaFlushedLSN,
		CommittedLSN:      w.committedLSN,
		RetentionStartLSN: retentionStart,
	}
}

// HeadLSN returns the highest LSN written.
func (w *WALHistory) HeadLSN() uint64 { return w.headLSN }

// TailLSN returns the oldest retained LSN boundary.
func (w *WALHistory) TailLSN() uint64 { return w.tailLSN }

// CommittedLSN returns the lineage-safe committed boundary.
func (w *WALHistory) CommittedLSN() uint64 { return w.committedLSN }

// Len returns the number of retained entries.
func (w *WALHistory) Len() int { return len(w.entries) }

// StateAt replays entries up to lsn and returns block→value state.
// Used to verify historical data correctness after recovery.
func (w *WALHistory) StateAt(lsn uint64) map[uint64]uint64 {
	state := map[uint64]uint64{}
	for _, e := range w.entries {
		if e.LSN > lsn {
			break
		}
		state[e.Block] = e.Value
	}
	return state
}
