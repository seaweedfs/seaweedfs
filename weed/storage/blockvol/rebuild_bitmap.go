package blockvol

// RebuildBitmap is a session-scoped dense bitset tracking which LBAs have
// been covered by applied WAL entries during a rebuild session. It is the
// overwrite protection for the two-line rebuild model:
//
//   - Line 1 (base lane): snapshot/extent blocks copied to replica
//   - Line 2 (WAL lane): live WAL entries applied to replica local WAL
//
// When a base chunk targets an LBA:
//   - bitmap clear → write base data (no conflict)
//   - bitmap set → skip (WAL-applied data is newer, WAL always wins)
//
// The bit is set when a WAL entry is APPLIED to the replica's local WAL
// (replayable after crash), NOT when it is merely received on the network.
//
// This bitmap is session-local volatile state. After crash, the rebuild
// session must restart from scratch with a fresh bitmap. Durable WAL
// entries survive crash and protect correctness via WAL replay.
//
// Implementation is a dense bitset modeled on SnapshotBitmap. No internal
// locking — callers must serialize access if needed.
type RebuildBitmap struct {
	data       []byte
	totalLBAs  uint64 // total number of LBAs (= volumeSize / blockSize)
	blockSize  uint32
	appliedCount uint64 // number of LBAs marked as WAL-applied
}

// NewRebuildBitmap creates a zero-initialized rebuild bitmap.
// totalLBAs = volumeSize / blockSize.
func NewRebuildBitmap(totalLBAs uint64, blockSize uint32) *RebuildBitmap {
	byteLen := (totalLBAs + 7) / 8
	return &RebuildBitmap{
		data:      make([]byte, byteLen),
		totalLBAs: totalLBAs,
		blockSize: blockSize,
	}
}

// MarkApplied sets the bit for the given LBA, indicating that a WAL entry
// covering this LBA has been applied to the replica's local WAL.
func (b *RebuildBitmap) MarkApplied(lba uint64) {
	if lba >= b.totalLBAs {
		return
	}
	if !b.IsApplied(lba) {
		b.appliedCount++
	}
	b.data[lba/8] |= 1 << (lba % 8)
}

// IsApplied returns true if the LBA has been covered by an applied WAL entry.
// When true, base lane data for this LBA must be skipped.
func (b *RebuildBitmap) IsApplied(lba uint64) bool {
	if lba >= b.totalLBAs {
		return false
	}
	return b.data[lba/8]&(1<<(lba%8)) != 0
}

// ShouldApplyBase returns true if the base lane may write data at this LBA.
// This is the conflict resolution rule: WAL-applied wins over base.
func (b *RebuildBitmap) ShouldApplyBase(lba uint64) bool {
	return !b.IsApplied(lba)
}

// AppliedCount returns the number of LBAs marked as WAL-applied.
func (b *RebuildBitmap) AppliedCount() uint64 {
	return b.appliedCount
}

// TotalLBAs returns the total number of trackable LBAs.
func (b *RebuildBitmap) TotalLBAs() uint64 {
	return b.totalLBAs
}

// Clear resets the bitmap to all-zero (no LBAs applied).
func (b *RebuildBitmap) Clear() {
	for i := range b.data {
		b.data[i] = 0
	}
	b.appliedCount = 0
}
