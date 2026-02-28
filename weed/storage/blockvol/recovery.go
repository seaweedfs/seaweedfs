package blockvol

import (
	"fmt"
	"os"
)

// RecoveryResult contains the outcome of WAL recovery.
type RecoveryResult struct {
	EntriesReplayed int    // number of entries replayed into dirty map
	HighestLSN      uint64 // highest LSN seen during recovery
	TornEntries     int    // entries discarded due to CRC failure
}

// RecoverWAL scans the WAL region from tail to head, replaying valid entries
// into the dirty map. Entries with LSN <= checkpointLSN are skipped (already
// in extent). Scanning stops at the first CRC failure (torn write).
//
// The WAL is a circular buffer. If head >= tail, scan [tail, head).
// If head < tail (wrapped), scan [tail, walSize) then [0, head).
func RecoverWAL(fd *os.File, sb *Superblock, dirtyMap *DirtyMap) (RecoveryResult, error) {
	result := RecoveryResult{}

	logicalHead := sb.WALHead
	logicalTail := sb.WALTail
	walOffset := sb.WALOffset
	walSize := sb.WALSize
	checkpointLSN := sb.WALCheckpointLSN

	if logicalHead == logicalTail {
		// WAL is empty (or fully flushed).
		return result, nil
	}

	// Convert logical positions to physical.
	physHead := logicalHead % walSize
	physTail := logicalTail % walSize

	// Build the list of byte ranges to scan.
	type scanRange struct {
		start, end uint64 // physical positions within WAL
	}

	var ranges []scanRange
	if physHead > physTail {
		// No wrap: scan [tail, head).
		ranges = append(ranges, scanRange{physTail, physHead})
	} else if physHead == physTail {
		// Head and tail at same physical position but different logical positions
		// means the WAL is completely full. Scan the entire region.
		ranges = append(ranges, scanRange{physTail, walSize})
		if physHead > 0 {
			ranges = append(ranges, scanRange{0, physHead})
		}
	} else {
		// Wrapped: scan [tail, walSize) then [0, head).
		ranges = append(ranges, scanRange{physTail, walSize})
		if physHead > 0 {
			ranges = append(ranges, scanRange{0, physHead})
		}
	}

	for _, r := range ranges {
		pos := r.start
		for pos < r.end {
			remaining := r.end - pos

			// Need at least a header to proceed.
			if remaining < uint64(walEntryHeaderSize) {
				break
			}

			// Read header.
			headerBuf := make([]byte, walEntryHeaderSize)
			absOff := int64(walOffset + pos)
			if _, err := fd.ReadAt(headerBuf, absOff); err != nil {
				return result, fmt.Errorf("recovery: read header at WAL+%d: %w", pos, err)
			}

			// Parse entry type and length field.
			entryType := headerBuf[16]
			lengthField := parseLength(headerBuf)

			// For padding entries, skip forward.
			if entryType == EntryTypePadding {
				entrySize := uint64(walEntryHeaderSize) + uint64(lengthField)
				pos += entrySize
				continue
			}

			// Calculate on-disk entry size. WRITE and PADDING carry data payload;
			// TRIM and BARRIER do not (Length is metadata, not data size).
			var payloadLen uint64
			if entryType == EntryTypeWrite {
				payloadLen = uint64(lengthField)
			}
			entrySize := uint64(walEntryHeaderSize) + payloadLen
			if entrySize > remaining {
				// Torn write: entry extends past available data.
				result.TornEntries++
				break
			}

			// Read full entry.
			fullBuf := make([]byte, entrySize)
			if _, err := fd.ReadAt(fullBuf, absOff); err != nil {
				return result, fmt.Errorf("recovery: read entry at WAL+%d: %w", pos, err)
			}

			// Decode and validate CRC.
			entry, err := DecodeWALEntry(fullBuf)
			if err != nil {
				// CRC failure or corrupt entry — stop here (torn write).
				result.TornEntries++
				break
			}

			// Skip entries already flushed to extent.
			if entry.LSN <= checkpointLSN {
				pos += entrySize
				continue
			}

			// Replay entry.
			switch entry.Type {
			case EntryTypeWrite:
				blocks := entry.Length / sb.BlockSize
				for i := uint32(0); i < blocks; i++ {
					dirtyMap.Put(entry.LBA+uint64(i), pos, entry.LSN, sb.BlockSize)
				}
				result.EntriesReplayed++

			case EntryTypeTrim:
				// TRIM carries Length (bytes) covering multiple blocks.
				blocks := entry.Length / sb.BlockSize
				if blocks == 0 {
					blocks = 1 // legacy single-block trim
				}
				for i := uint32(0); i < blocks; i++ {
					dirtyMap.Put(entry.LBA+uint64(i), pos, entry.LSN, sb.BlockSize)
				}
				result.EntriesReplayed++

			case EntryTypeBarrier:
				// Barriers don't modify data, just skip.
			}

			if entry.LSN > result.HighestLSN {
				result.HighestLSN = entry.LSN
			}

			pos += entrySize
		}
	}

	return result, nil
}
