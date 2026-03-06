package blockvol

import (
	"encoding/binary"
	"fmt"
	"log"
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

	// flushMu serializes FlushOnce calls and is acquired by CreateSnapshot
	// to pause the flusher while the snapshot is being set up.
	// Lock order: flushMu -> snapMu (flushMu acquired first).
	flushMu sync.Mutex

	// snapMu protects the snapshots slice. Acquired under flushMu in
	// FlushOnce (RLock) and under flushMu in PauseAndFlush callers.
	snapMu    sync.RWMutex
	snapshots []*activeSnapshot

	logger  *log.Logger
	lastErr bool // true if last FlushOnce returned error
	metrics *EngineMetrics

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
	Interval    time.Duration    // default 100ms
	Logger      *log.Logger      // optional; defaults to log.Default()
	Metrics     *EngineMetrics   // optional; if nil, no metrics recorded
}

// NewFlusher creates a flusher. Call Run() in a goroutine.
func NewFlusher(cfg FlusherConfig) *Flusher {
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
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
		logger:         cfg.Logger,
		metrics:        cfg.Metrics,
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
			if err := f.FlushOnce(); err != nil {
				if !f.lastErr {
					f.logger.Printf("flusher error: %v", err)
				}
				f.lastErr = true
				if f.metrics != nil {
					f.metrics.RecordFlusherError()
				}
			} else {
				f.lastErr = false
			}
		case <-f.notifyCh:
			if err := f.FlushOnce(); err != nil {
				if !f.lastErr {
					f.logger.Printf("flusher error: %v", err)
				}
				f.lastErr = true
				if f.metrics != nil {
					f.metrics.RecordFlusherError()
				}
			} else {
				f.lastErr = false
			}
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

// NotifyUrgent wakes the flusher for an urgent flush (WAL pressure).
// Phase 3 MVP: delegates to Notify(). Future: may use a priority channel.
func (f *Flusher) NotifyUrgent() {
	f.Notify()
}

// Stop shuts down the flusher. Safe to call multiple times.
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() {
		close(f.stopCh)
	})
	<-f.done
}

// AddSnapshot adds a snapshot to the flusher's active list.
func (f *Flusher) AddSnapshot(snap *activeSnapshot) {
	f.snapMu.Lock()
	f.snapshots = append(f.snapshots, snap)
	f.snapMu.Unlock()
}

// RemoveSnapshot removes a snapshot from the flusher's active list by ID.
func (f *Flusher) RemoveSnapshot(id uint32) {
	f.snapMu.Lock()
	for i, s := range f.snapshots {
		if s.id == id {
			f.snapshots = append(f.snapshots[:i], f.snapshots[i+1:]...)
			break
		}
	}
	f.snapMu.Unlock()
}

// HasActiveSnapshots returns true if there are active snapshots needing CoW.
func (f *Flusher) HasActiveSnapshots() bool {
	f.snapMu.RLock()
	n := len(f.snapshots)
	f.snapMu.RUnlock()
	return n > 0
}

// PauseAndFlush acquires flushMu (pausing the flusher), then runs FlushOnce.
// The caller must call Resume() when done.
func (f *Flusher) PauseAndFlush() error {
	f.flushMu.Lock()
	return f.flushOnceLocked()
}

// Resume releases flushMu, allowing the flusher to resume.
func (f *Flusher) Resume() {
	f.flushMu.Unlock()
}

// FlushOnce performs a single flush cycle: scan dirty map, CoW for active
// snapshots, copy data to extent region, fsync, update checkpoint, advance WAL tail.
func (f *Flusher) FlushOnce() error {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()
	return f.flushOnceLocked()
}

// flushOnceLocked is the inner FlushOnce. Caller must hold flushMu.
func (f *Flusher) flushOnceLocked() error {
	flushStart := time.Now()
	entries := f.dirtyMap.Snapshot()
	if len(entries) == 0 {
		return nil
	}

	// --- Phase 1: CoW for active snapshots ---
	f.snapMu.RLock()
	snaps := make([]*activeSnapshot, len(f.snapshots))
	copy(snaps, f.snapshots)
	f.snapMu.RUnlock()

	if len(snaps) > 0 {
		cowDirty := false
		for _, e := range entries {
			for _, snap := range snaps {
				if !snap.bitmap.Get(e.Lba) {
					// Read old data from extent (pre-modification state).
					oldData := make([]byte, f.blockSize)
					extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
					if _, err := f.fd.ReadAt(oldData, extentOff); err != nil {
						return fmt.Errorf("flusher: CoW read extent LBA %d: %w", e.Lba, err)
					}
					// Write old data to delta file.
					deltaOff := int64(snap.dataOffset + e.Lba*uint64(f.blockSize))
					if _, err := snap.fd.WriteAt(oldData, deltaOff); err != nil {
						return fmt.Errorf("flusher: CoW write delta LBA %d snap %d: %w", e.Lba, snap.id, err)
					}
					snap.bitmap.Set(e.Lba)
					snap.dirty = true
					cowDirty = true
				}
			}
		}

		if cowDirty {
			// Crash safety: delta data -> fsync -> bitmap persist -> fsync -> extent write.
			for _, snap := range snaps {
				if snap.dirty {
					if err := snap.fd.Sync(); err != nil {
						return fmt.Errorf("flusher: fsync delta snap %d: %w", snap.id, err)
					}
					if err := snap.bitmap.WriteTo(snap.fd, SnapHeaderSize); err != nil {
						return fmt.Errorf("flusher: persist bitmap snap %d: %w", snap.id, err)
					}
					if err := snap.fd.Sync(); err != nil {
						return fmt.Errorf("flusher: fsync bitmap snap %d: %w", snap.id, err)
					}
					snap.dirty = false
				}
			}
		}
	}

	// --- Phase 2: Extent writes (existing, unchanged) ---
	var maxLSN uint64
	var maxWALEnd uint64

	for _, e := range entries {
		// Read the WAL entry and copy data to extent region.
		headerBuf := make([]byte, walEntryHeaderSize)
		absWALOff := int64(f.walOffset + e.WalOffset)
		if _, err := f.fd.ReadAt(headerBuf, absWALOff); err != nil {
			return fmt.Errorf("flusher: read WAL header at %d: %w", absWALOff, err)
		}

		// WAL reuse guard: validate LSN before trusting the entry.
		entryLSN := binary.LittleEndian.Uint64(headerBuf[0:8])
		if entryLSN != e.Lsn {
			continue // stale --WAL slot reused, skip this entry
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
				continue // corrupt or partially overwritten --skip
			}

			if e.Lba < entry.LBA {
				continue // LBA mismatch --stale entry
			}
			blockIdx := e.Lba - entry.LBA
			dataStart := blockIdx * uint64(f.blockSize)
			if dataStart+uint64(f.blockSize) <= uint64(len(entry.Data)) {
				extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
				blockData := entry.Data[dataStart : dataStart+uint64(f.blockSize)]
				if _, err := f.fd.WriteAt(blockData, extentOff); err != nil {
					return fmt.Errorf("flusher: write extent at LBA %d: %w", e.Lba, err)
				}
			}

			walEnd := e.WalOffset + uint64(entryLen)
			if walEnd > maxWALEnd {
				maxWALEnd = walEnd
			}
		} else if entryType == EntryTypeTrim {
			zeroBlock := make([]byte, f.blockSize)
			extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
			if _, err := f.fd.WriteAt(zeroBlock, extentOff); err != nil {
				return fmt.Errorf("flusher: zero extent at LBA %d: %w", e.Lba, err)
			}

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

	// Record metrics.
	if f.metrics != nil {
		bytesWritten := uint64(len(entries)) * uint64(f.blockSize)
		f.metrics.RecordFlusherFlush(bytesWritten, time.Since(flushStart))
	}

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

// SetFD replaces the file descriptor used for extent writes. Test-only.
func (f *Flusher) SetFD(fd *os.File) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fd = fd
}

// parseLength extracts the Length field from a WAL entry header buffer.
func parseLength(headerBuf []byte) uint32 {
	// Length at LSN(8)+Epoch(8)+Type(1)+Flags(1)+LBA(8) = 26
	return binary.LittleEndian.Uint32(headerBuf[26:])
}
