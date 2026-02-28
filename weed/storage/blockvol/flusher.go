package blockvol

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"
)

// Flusher copies WAL entries to the extent region and frees WAL space.
// It runs as a background goroutine and can also be triggered manually.
type Flusher struct {
	fd          *os.File
	super       *Superblock
	wal         *WALWriter
	dirtyMap    *DirtyMap
	walOffset   uint64 // absolute file offset of WAL region
	walSize     uint64
	blockSize   uint32
	extentStart uint64 // absolute file offset of extent region

	mu             sync.Mutex
	checkpointLSN  uint64 // last flushed LSN
	checkpointTail uint64 // WAL physical tail after last flush

	interval time.Duration
	notifyCh chan struct{}
	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// FlusherConfig configures the flusher.
type FlusherConfig struct {
	FD          *os.File
	Super       *Superblock
	WAL         *WALWriter
	DirtyMap    *DirtyMap
	Interval    time.Duration // default 100ms
}

// NewFlusher creates a flusher. Call Run() in a goroutine.
func NewFlusher(cfg FlusherConfig) *Flusher {
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	return &Flusher{
		fd:             cfg.FD,
		super:          cfg.Super,
		wal:            cfg.WAL,
		dirtyMap:       cfg.DirtyMap,
		walOffset:      cfg.Super.WALOffset,
		walSize:        cfg.Super.WALSize,
		blockSize:      cfg.Super.BlockSize,
		extentStart:    cfg.Super.WALOffset + cfg.Super.WALSize,
		checkpointLSN:  cfg.Super.WALCheckpointLSN,
		checkpointTail: 0,
		interval:       cfg.Interval,
		notifyCh:       make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}
}

// Run is the flusher main loop. Call in a goroutine.
func (f *Flusher) Run() {
	defer close(f.done)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
			f.FlushOnce()
		case <-f.notifyCh:
			f.FlushOnce()
		}
	}
}

// Notify wakes up the flusher for an immediate flush cycle.
func (f *Flusher) Notify() {
	select {
	case f.notifyCh <- struct{}{}:
	default:
	}
}

// Stop shuts down the flusher. Safe to call multiple times.
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopCh)
	})
	<-f.done
}

// FlushOnce performs a single flush cycle: scan dirty map, copy data to
// extent region, fsync, update checkpoint, advance WAL tail.
func (f *Flusher) FlushOnce() error {
	// Snapshot dirty entries. We use a full scan (Range over all possible LBAs
	// is impractical), so we collect from the dirty map directly.
	type flushEntry struct {
		lba       uint64
		walOff    uint64
		lsn       uint64
		length    uint32
	}

	entries := f.dirtyMap.Snapshot()
	if len(entries) == 0 {
		return nil
	}

	// Find the max LSN and max WAL offset to know where to advance tail.
	var maxLSN uint64
	var maxWALEnd uint64

	for _, e := range entries {
		// Read the WAL entry and copy data to extent region.
		headerBuf := make([]byte, walEntryHeaderSize)
		absWALOff := int64(f.walOffset + e.WalOffset)
		if _, err := f.fd.ReadAt(headerBuf, absWALOff); err != nil {
			return fmt.Errorf("flusher: read WAL header at %d: %w", absWALOff, err)
		}

		// Parse entry type and length.
		entryType := headerBuf[16] // Type at LSN(8)+Epoch(8)=16
		dataLen := parseLength(headerBuf)

		if entryType == EntryTypeWrite && dataLen > 0 {
			// Read full entry.
			entryLen := walEntryHeaderSize + int(dataLen)
			fullBuf := make([]byte, entryLen)
			if _, err := f.fd.ReadAt(fullBuf, absWALOff); err != nil {
				return fmt.Errorf("flusher: read WAL entry at %d: %w", absWALOff, err)
			}

			entry, err := DecodeWALEntry(fullBuf)
			if err != nil {
				return fmt.Errorf("flusher: decode WAL entry: %w", err)
			}

			// Write data to extent region.
			blocks := entry.Length / f.blockSize
			for i := uint32(0); i < blocks; i++ {
				blockLBA := entry.LBA + uint64(i)
				extentOff := int64(f.extentStart + blockLBA*uint64(f.blockSize))
				blockData := entry.Data[i*f.blockSize : (i+1)*f.blockSize]
				if _, err := f.fd.WriteAt(blockData, extentOff); err != nil {
					return fmt.Errorf("flusher: write extent at LBA %d: %w", blockLBA, err)
				}
			}

			// Track WAL end position for tail advance.
			walEnd := e.WalOffset + uint64(entryLen)
			if walEnd > maxWALEnd {
				maxWALEnd = walEnd
			}
		} else if entryType == EntryTypeTrim {
			// TRIM entries: zero the extent region for this LBA.
			// Each dirty map entry represents one trimmed block.
			zeroBlock := make([]byte, f.blockSize)
			extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
			if _, err := f.fd.WriteAt(zeroBlock, extentOff); err != nil {
				return fmt.Errorf("flusher: zero extent at LBA %d: %w", e.Lba, err)
			}

			// TRIM entry has no data payload, just a header.
			walEnd := e.WalOffset + uint64(walEntryHeaderSize)
			if walEnd > maxWALEnd {
				maxWALEnd = walEnd
			}
		}

		if e.Lsn > maxLSN {
			maxLSN = e.Lsn
		}
	}

	// Fsync extent writes.
	if err := f.fd.Sync(); err != nil {
		return fmt.Errorf("flusher: fsync extent: %w", err)
	}

	// Remove flushed entries from dirty map.
	f.mu.Lock()
	for _, e := range entries {
		// Only remove if the dirty map entry still has the same LSN
		// (a newer write may have updated it).
		_, currentLSN, _, ok := f.dirtyMap.Get(e.Lba)
		if ok && currentLSN == e.Lsn {
			f.dirtyMap.Delete(e.Lba)
		}
	}
	f.checkpointLSN = maxLSN
	f.checkpointTail = maxWALEnd
	f.mu.Unlock()

	// Advance WAL tail to free space.
	if maxWALEnd > 0 {
		f.wal.AdvanceTail(maxWALEnd)
	}

	// Update superblock checkpoint.
	f.updateSuperblockCheckpoint(maxLSN, f.wal.Tail())

	return nil
}

// updateSuperblockCheckpoint writes the updated checkpoint to disk.
func (f *Flusher) updateSuperblockCheckpoint(checkpointLSN uint64, walTail uint64) error {
	f.super.WALCheckpointLSN = checkpointLSN
	f.super.WALHead = f.wal.LogicalHead()
	f.super.WALTail = f.wal.LogicalTail()

	if _, err := f.fd.Seek(0, 0); err != nil {
		return fmt.Errorf("flusher: seek to superblock: %w", err)
	}
	if _, err := f.super.WriteTo(f.fd); err != nil {
		return fmt.Errorf("flusher: write superblock: %w", err)
	}
	return f.fd.Sync()
}

// CheckpointLSN returns the last flushed LSN.
func (f *Flusher) CheckpointLSN() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpointLSN
}

// parseLength extracts the Length field from a WAL entry header buffer.
func parseLength(headerBuf []byte) uint32 {
	// Length at LSN(8)+Epoch(8)+Type(1)+Flags(1)+LBA(8) = 26
	return binary.LittleEndian.Uint32(headerBuf[26:])
}
