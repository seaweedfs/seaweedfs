package blockvol

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/batchio"
)

// Flusher copies WAL entries to the extent region and frees WAL space.
// It runs as a background goroutine and can also be triggered manually.
type Flusher struct {
	fd          *os.File
	super       *Superblock
	superMu     *sync.Mutex // serializes superblock writes (shared with group commit)
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

	bio     batchio.BatchIO // batch I/O backend (default: standard sequential)
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
	SuperMu     *sync.Mutex      // serializes superblock writes (shared with group commit)
	WAL         *WALWriter
	DirtyMap    *DirtyMap
	Interval    time.Duration    // default 100ms
	Logger      *log.Logger      // optional; defaults to log.Default()
	Metrics     *EngineMetrics   // optional; if nil, no metrics recorded
	BatchIO     batchio.BatchIO  // optional; defaults to batchio.NewStandard()
}

// NewFlusher creates a flusher. Call Run() in a goroutine.
func NewFlusher(cfg FlusherConfig) *Flusher {
	if cfg.Interval == 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.BatchIO == nil {
		cfg.BatchIO = batchio.NewStandard()
	}
	return &Flusher{
		fd:             cfg.FD,
		super:          cfg.Super,
		superMu:        cfg.SuperMu,
		wal:            cfg.WAL,
		dirtyMap:       cfg.DirtyMap,
		walOffset:      cfg.Super.WALOffset,
		walSize:        cfg.Super.WALSize,
		blockSize:      cfg.Super.BlockSize,
		extentStart:    cfg.Super.WALOffset + cfg.Super.WALSize,
		bio:            cfg.BatchIO,
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

	// --- Phase 2: Extent writes via BatchIO ---
	var maxLSN uint64
	var maxWALEnd uint64

	// Step 2a: Batch-read WAL headers.
	headerOps := make([]batchio.Op, len(entries))
	for i, e := range entries {
		headerOps[i] = batchio.Op{
			Buf:    make([]byte, walEntryHeaderSize),
			Offset: int64(f.walOffset + e.WalOffset),
		}
	}
	if err := f.bio.PreadBatch(f.fd, headerOps); err != nil {
		return fmt.Errorf("flusher: batch read WAL headers: %w", err)
	}

	// Step 2b: Identify entries needing full WAL read, batch-read them.
	type pendingEntry struct {
		idx       int    // index into entries
		entryType uint8
		entryLen  int
	}
	var pending []pendingEntry

	for i, e := range entries {
		hdr := headerOps[i].Buf
		entryLSN := binary.LittleEndian.Uint64(hdr[0:8])
		if entryLSN != e.Lsn {
			continue // stale — WAL slot reused
		}

		entryType := hdr[16]
		if entryType == EntryTypeWrite {
			dataLen := parseLength(hdr)
			if dataLen > 0 {
				pending = append(pending, pendingEntry{
					idx:       i,
					entryType: entryType,
					entryLen:  walEntryHeaderSize + int(dataLen),
				})
			}
		} else if entryType == EntryTypeTrim {
			pending = append(pending, pendingEntry{
				idx:       i,
				entryType: entryType,
				entryLen:  walEntryHeaderSize,
			})
		}

		if e.Lsn > maxLSN {
			maxLSN = e.Lsn
		}
	}

	// Batch-read full WAL entries for write ops.
	var walReadOps []batchio.Op
	for _, p := range pending {
		if p.entryType == EntryTypeWrite {
			walReadOps = append(walReadOps, batchio.Op{
				Buf:    make([]byte, p.entryLen),
				Offset: int64(f.walOffset + entries[p.idx].WalOffset),
			})
		}
	}
	if len(walReadOps) > 0 {
		if err := f.bio.PreadBatch(f.fd, walReadOps); err != nil {
			return fmt.Errorf("flusher: batch read WAL entries: %w", err)
		}
	}

	// Step 2c: Decode entries and build extent write ops.
	var extentWriteOps []batchio.Op
	walReadI := 0

	for _, p := range pending {
		e := entries[p.idx]
		if p.entryType == EntryTypeWrite {
			fullBuf := walReadOps[walReadI].Buf
			walReadI++

			entry, err := DecodeWALEntry(fullBuf)
			if err != nil {
				continue
			}
			if e.Lba < entry.LBA {
				continue
			}
			blockIdx := e.Lba - entry.LBA
			dataStart := blockIdx * uint64(f.blockSize)
			if dataStart+uint64(f.blockSize) <= uint64(len(entry.Data)) {
				extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
				blockData := entry.Data[dataStart : dataStart+uint64(f.blockSize)]
				extentWriteOps = append(extentWriteOps, batchio.Op{
					Buf:    blockData,
					Offset: extentOff,
				})
			}

			walEnd := e.WalOffset + uint64(p.entryLen)
			if walEnd > maxWALEnd {
				maxWALEnd = walEnd
			}
		} else if p.entryType == EntryTypeTrim {
			zeroBlock := make([]byte, f.blockSize)
			extentOff := int64(f.extentStart + e.Lba*uint64(f.blockSize))
			extentWriteOps = append(extentWriteOps, batchio.Op{
				Buf:    zeroBlock,
				Offset: extentOff,
			})

			walEnd := e.WalOffset + uint64(walEntryHeaderSize)
			if walEnd > maxWALEnd {
				maxWALEnd = walEnd
			}
		}
	}

	// Step 2d: Batch-write extents + fsync.
	if len(extentWriteOps) > 0 {
		if err := f.bio.PwriteBatch(f.fd, extentWriteOps); err != nil {
			return fmt.Errorf("flusher: batch write extents: %w", err)
		}
	}
	if err := f.bio.Fsync(f.fd); err != nil {
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
	log.Printf("flusher: checkpoint LSN=%d entries=%d WALTail=%d WALHead=%d",
		maxLSN, len(entries), f.wal.LogicalTail(), f.wal.LogicalHead())
	f.updateSuperblockCheckpoint(maxLSN, f.wal.Tail())

	// Record metrics.
	if f.metrics != nil {
		bytesWritten := uint64(len(entries)) * uint64(f.blockSize)
		f.metrics.RecordFlusherFlush(bytesWritten, time.Since(flushStart))
	}

	return nil
}

// updateSuperblockCheckpoint writes the updated checkpoint to disk.
// Acquires superMu to serialize against syncWithWALProgress (group commit).
func (f *Flusher) updateSuperblockCheckpoint(checkpointLSN uint64, walTail uint64) error {
	f.superMu.Lock()
	defer f.superMu.Unlock()

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

// CloseBatchIO releases the batch I/O backend resources (e.g. io_uring ring).
// Must be called after Stop() and the final FlushOnce().
func (f *Flusher) CloseBatchIO() error {
	if f.bio != nil {
		return f.bio.Close()
	}
	return nil
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
