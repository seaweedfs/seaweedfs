// Package blockvol implements a block volume storage engine with WAL,
// dirty map, group commit, and crash recovery. It operates on a single
// file and has no dependency on SeaweedFS internals.
package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/batchio"
)

// CreateOptions configures a new block volume.
type CreateOptions struct {
	VolumeSize     uint64         // required, logical size in bytes
	ExtentSize     uint32         // default 64KB
	BlockSize      uint32         // default 4KB
	WALSize        uint64         // default 64MB
	Replication    string         // default "000"
	DurabilityMode DurabilityMode // CP8-3-1: default best_effort (0)
	StorageProfile StorageProfile // CP11A-1: default single (0)
}

// ErrVolumeClosed is returned when an operation is attempted on a closed BlockVol.
var ErrVolumeClosed = errors.New("blockvol: volume closed")

// BlockVol is the core block volume engine.
// walRetentionTimeout is the maximum time a recoverable replica can hold
// WAL entries. After this, the replica is escalated to NeedsRebuild and
// its WAL hold is released. CP13-6: timeout-only budget (max-bytes deferred).
const walRetentionTimeout = 5 * time.Minute

type BlockVol struct {
	mu             sync.RWMutex
	ioMu           sync.RWMutex // guards local data mutation (WAL/dirtyMap/extent); Lock for restore/import/expand
	superMu        sync.Mutex   // serializes superblock mutation + persist (group commit vs flusher)
	shipMu         sync.Mutex   // serializes ShipAll calls to guarantee LSN-order delivery
	fd             *os.File
	path           string
	super          Superblock
	config         BlockVolConfig
	wal            *WALWriter
	dirtyMap       *DirtyMap
	groupCommit    *GroupCommitter
	flusher        *Flusher
	nextLSN        atomic.Uint64
	healthy        atomic.Bool
	closed         atomic.Bool
	opsOutstanding atomic.Int64 // in-flight Read/Write/Trim/SyncCache ops
	opsDrained     chan struct{}

	// Replication fields (Phase 4A CP2, generalized to N replicas in CP8-2).
	shipperGroup *ShipperGroup
	replRecv     *ReplicaReceiver

	// Fencing fields (Phase 4A).
	epoch        atomic.Uint64 // current persisted epoch
	masterEpoch  atomic.Uint64 // expected epoch from master
	lease        Lease
	role         atomic.Uint32
	roleCallback RoleChangeCallback

	// Promotion/rebuild fields (Phase 4A CP3).
	rebuildServer *RebuildServer
	assignMu      sync.Mutex    // serializes HandleAssignment calls
	drainTimeout  time.Duration // default 10s, for demote drain

	// Health score and scrub (CP8-2).
	healthScore *HealthScore
	scrubber    *Scrubber

	// Write admission control (BUG-CP103-2).
	walAdmission *WALAdmission

	// Observability (CP8-4).
	Metrics *EngineMetrics

	// Shipper state change callback — triggers immediate heartbeat.
	onShipperStateChange func(from, to ReplicaState)

	// Snapshot fields (Phase 5 CP5-2).
	snapMu    sync.RWMutex
	snapshots map[uint32]*activeSnapshot
}

// CreateBlockVol creates a new block volume file at path.
func CreateBlockVol(path string, opts CreateOptions, cfgs ...BlockVolConfig) (*BlockVol, error) {
	var cfg BlockVolConfig
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if opts.VolumeSize == 0 {
		return nil, ErrInvalidVolumeSize
	}
	if opts.StorageProfile == ProfileStriped {
		return nil, ErrStripedNotImplemented
	}
	if opts.StorageProfile > ProfileStriped {
		return nil, fmt.Errorf("%w: %d", ErrInvalidStorageProfile, opts.StorageProfile)
	}

	sb, err := NewSuperblock(opts.VolumeSize, opts)
	if err != nil {
		return nil, fmt.Errorf("blockvol: create superblock: %w", err)
	}
	sb.CreatedAt = uint64(time.Now().Unix())

	// Extent region starts after superblock + WAL.
	extentStart := sb.WALOffset + sb.WALSize
	totalFileSize := extentStart + opts.VolumeSize

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("blockvol: create file: %w", err)
	}

	if err := fd.Truncate(int64(totalFileSize)); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: truncate: %w", err)
	}

	if _, err := sb.WriteTo(fd); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: write superblock: %w", err)
	}

	if err := fd.Sync(); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: sync: %w", err)
	}

	dm := NewDirtyMap(cfg.DirtyMapShards)
	wal := NewWALWriter(fd, sb.WALOffset, sb.WALSize, 0, 0)
	v := &BlockVol{
		fd:          fd,
		path:        path,
		super:       sb,
		config:      cfg,
		wal:         wal,
		dirtyMap:    dm,
		healthScore: NewHealthScore(),
		Metrics:     NewEngineMetrics(),
		opsDrained:  make(chan struct{}, 1),
		snapshots:   make(map[uint32]*activeSnapshot),
	}
	v.nextLSN.Store(1)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:       v.fd.Sync,
		MaxDelay:       cfg.GroupCommitMaxDelay,
		MaxBatch:       cfg.GroupCommitMaxBatch,
		LowWatermark:   cfg.GroupCommitLowWatermark,
		OnDegraded:     func() { v.healthy.Store(false) },
		PostSyncCheck:  v.writeGate,
		Metrics:        v.Metrics,
	})
	go v.groupCommit.Run()
	bio, _, err := newBatchIO(cfg.IOBackend, log.Default())
	if err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: %w", err)
	}
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      wal,
		DirtyMap: dm,
		Interval: cfg.FlushInterval,
		Metrics:  v.Metrics,
		BatchIO:  bio,
		// CP13-6: replica-aware WAL retention.
		RetentionFloorFn: func() (uint64, bool) {
			if v.shipperGroup == nil {
				return 0, false
			}
			return v.shipperGroup.MinRecoverableFlushedLSN()
		},
		EvaluateRetentionBudgetsFn: func() {
			if v.shipperGroup != nil {
				v.shipperGroup.EvaluateRetentionBudgets(walRetentionTimeout)
			}
		},
	})
	go v.flusher.Run()
	v.walAdmission = NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: cfg.WALMaxConcurrentWrites,
		SoftWatermark: cfg.WALSoftWatermark,
		HardWatermark: cfg.WALHardWatermark,
		WALUsedFn:     wal.UsedFraction,
		NotifyFn:      v.flusher.NotifyUrgent,
		ClosedFn:      v.closed.Load,
		Metrics:       v.Metrics,
	})
	return v, nil
}

// OpenBlockVol opens an existing block volume file and runs crash recovery.
func OpenBlockVol(path string, cfgs ...BlockVolConfig) (*BlockVol, error) {
	var cfg BlockVolConfig
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("blockvol: open file: %w", err)
	}

	sb, err := ReadSuperblock(fd)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("blockvol: read superblock: %w", err)
	}
	if err := sb.Validate(); err != nil {
		fd.Close()
		return nil, fmt.Errorf("blockvol: validate superblock: %w", err)
	}

	// CP11A-2: Clear stale prepared expand state on recovery.
	if sb.PreparedSize != 0 {
		log.Printf("blockvol: clearing stale PreparedSize=%d ExpandEpoch=%d on open (crash during prepare phase)", sb.PreparedSize, sb.ExpandEpoch)
		sb.PreparedSize = 0
		sb.ExpandEpoch = 0
		if _, err := fd.Seek(0, 0); err != nil {
			fd.Close()
			return nil, fmt.Errorf("blockvol: seek for prepared clear: %w", err)
		}
		if _, err := sb.WriteTo(fd); err != nil {
			fd.Close()
			return nil, fmt.Errorf("blockvol: write superblock for prepared clear: %w", err)
		}
		if err := fd.Sync(); err != nil {
			fd.Close()
			return nil, fmt.Errorf("blockvol: sync for prepared clear: %w", err)
		}
	}

	dirtyMap := NewDirtyMap(cfg.DirtyMapShards)

	// Run WAL recovery: replay entries from tail to head.
	result, err := RecoverWAL(fd, &sb, dirtyMap)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("blockvol: recovery: %w", err)
	}

	nextLSN := sb.WALCheckpointLSN + 1
	if result.HighestLSN >= nextLSN {
		nextLSN = result.HighestLSN + 1
	}

	wal := NewWALWriter(fd, sb.WALOffset, sb.WALSize, sb.WALHead, sb.WALTail)
	v := &BlockVol{
		fd:          fd,
		path:        path,
		super:       sb,
		config:      cfg,
		wal:         wal,
		dirtyMap:    dirtyMap,
		healthScore: NewHealthScore(),
		Metrics:     NewEngineMetrics(),
		opsDrained:  make(chan struct{}, 1),
	}
	v.nextLSN.Store(nextLSN)
	v.epoch.Store(sb.Epoch)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:       v.fd.Sync,
		MaxDelay:       cfg.GroupCommitMaxDelay,
		MaxBatch:       cfg.GroupCommitMaxBatch,
		LowWatermark:   cfg.GroupCommitLowWatermark,
		OnDegraded:     func() { v.healthy.Store(false) },
		PostSyncCheck:  v.writeGate,
		Metrics:        v.Metrics,
	})
	go v.groupCommit.Run()
	bio, _, err := newBatchIO(cfg.IOBackend, log.Default())
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("blockvol: %w", err)
	}
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      wal,
		DirtyMap: dirtyMap,
		Interval: cfg.FlushInterval,
		Metrics:  v.Metrics,
		BatchIO:  bio,
		RetentionFloorFn: func() (uint64, bool) {
			if v.shipperGroup == nil {
				return 0, false
			}
			return v.shipperGroup.MinRecoverableFlushedLSN()
		},
		EvaluateRetentionBudgetsFn: func() {
			if v.shipperGroup != nil {
				v.shipperGroup.EvaluateRetentionBudgets(walRetentionTimeout)
			}
		},
	})
	go v.flusher.Run()

	// Discover and reopen existing snapshots.
	v.snapshots = make(map[uint32]*activeSnapshot)
	snapFiles, _ := filepath.Glob(path + ".snap.*")
	for _, sf := range snapFiles {
		snap, err := openDeltaFile(sf)
		if err != nil {
			log.Printf("blockvol: skipping snapshot file %s: %v", sf, err)
			continue
		}
		if snap.header.ParentUUID != v.super.UUID {
			log.Printf("blockvol: skipping snapshot %d: parent UUID mismatch", snap.id)
			snap.Close()
			continue
		}
		v.snapshots[snap.id] = snap
		v.flusher.AddSnapshot(snap)
	}
	if len(v.snapshots) > 0 {
		log.Printf("blockvol: recovered %d snapshot(s)", len(v.snapshots))
	}

	v.walAdmission = NewWALAdmission(WALAdmissionConfig{
		MaxConcurrent: cfg.WALMaxConcurrentWrites,
		SoftWatermark: cfg.WALSoftWatermark,
		HardWatermark: cfg.WALHardWatermark,
		WALUsedFn:     wal.UsedFraction,
		NotifyFn:      v.flusher.NotifyUrgent,
		ClosedFn:      v.closed.Load,
		Metrics:       v.Metrics,
	})

	return v, nil
}

// syncWithWALProgress persists the WAL head pointer in the superblock.
// Called by the flusher during checkpoint (not on every group commit).
//
// The extended WAL recovery scan (RecoverWAL) makes this advisory:
// recovery scans past the recorded WALHead using CRC validation, so
// entries written after the last superblock persist are not lost.
// Persisting WALHead reduces recovery scan range but is not required
// for correctness.
func (v *BlockVol) syncWithWALProgress() error {
	v.superMu.Lock()
	defer v.superMu.Unlock()

	// Update superblock WALHead from the live WAL writer.
	// WALTail and CheckpointLSN are updated by the flusher, not here.
	v.super.WALHead = v.wal.LogicalHead()

	// Rewrite the full superblock at offset 0.
	var buf bytes.Buffer
	if _, err := v.super.WriteTo(&buf); err != nil {
		return fmt.Errorf("blockvol: serialize superblock for WAL progress: %w", err)
	}
	if _, err := v.fd.WriteAt(buf.Bytes(), 0); err != nil {
		return fmt.Errorf("blockvol: persist WAL progress: %w", err)
	}

	// Single fsync covers both WAL data and superblock.
	return v.fd.Sync()
}

// beginOp increments the in-flight ops counter. Returns ErrVolumeClosed if
// the volume is already closed, so callers must not proceed.
func (v *BlockVol) beginOp() error {
	v.opsOutstanding.Add(1)
	if v.closed.Load() {
		v.endOp()
		return ErrVolumeClosed
	}
	return nil
}

// endOp decrements the in-flight ops counter and signals the drain channel
// if this was the last op and the volume is closing.
func (v *BlockVol) endOp() {
	if v.opsOutstanding.Add(-1) == 0 && v.closed.Load() {
		select {
		case v.opsDrained <- struct{}{}:
		default:
		}
	}
}

// appendWithRetry appends a WAL entry, retrying on WAL-full by triggering
// the flusher until the timeout expires.
func (v *BlockVol) appendWithRetry(entry *WALEntry) (uint64, error) {
	walOff, err := v.wal.Append(entry)
	if errors.Is(err, ErrWALFull) {
		deadline := time.After(v.config.WALFullTimeout)
		for errors.Is(err, ErrWALFull) {
			if v.closed.Load() {
				return 0, ErrVolumeClosed
			}
			v.flusher.NotifyUrgent()
			select {
			case <-deadline:
				return 0, fmt.Errorf("blockvol: WAL full timeout: %w", ErrWALFull)
			case <-time.After(1 * time.Millisecond):
			}
			walOff, err = v.wal.Append(entry)
		}
	}
	if err != nil {
		return 0, fmt.Errorf("blockvol: WAL append: %w", err)
	}
	// Re-check closed after retry: Close() may have freed WAL space
	// while we were retrying, allowing Append to succeed.
	if v.closed.Load() {
		return 0, ErrVolumeClosed
	}

	// Ship to replicas if configured (fire-and-forget).
	// LSN-order delivery is guaranteed by shipMu held from LSN allocation
	// through shipping in WriteLBA/Trim.
	if v.shipperGroup != nil {
		v.shipperGroup.ShipAll(entry)
	}

	// Notify scrubber of written LBAs for CRC invalidation.
	if v.scrubber != nil && entry.Type == EntryTypeWrite {
		v.scrubber.NotifyWrite(entry.LBA, entry.Length/v.super.BlockSize)
	}

	return walOff, nil
}

// WriteLBA writes data at the given logical block address.
// Data length must be a multiple of BlockSize.
func (v *BlockVol) WriteLBA(lba uint64, data []byte) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()
	if err := v.writeGate(); err != nil {
		return err
	}
	if err := ValidateWrite(lba, uint32(len(data)), v.super.VolumeSize, v.super.BlockSize); err != nil {
		return err
	}

	// Admission control: throttle/block based on WAL pressure watermarks.
	if v.walAdmission != nil {
		if err := v.walAdmission.Acquire(v.config.WALFullTimeout); err != nil {
			return fmt.Errorf("blockvol: write admission: %w", err)
		}
		defer v.walAdmission.Release()
	}

	// shipMu serializes LSN allocation + WAL append + Ship to guarantee
	// LSN-order delivery to replicas under concurrent writes.
	v.shipMu.Lock()

	lsn := v.nextLSN.Add(1) - 1
	entry := &WALEntry{
		LSN:    lsn,
		Epoch:  v.epoch.Load(),
		Type:   EntryTypeWrite,
		LBA:    lba,
		Length: uint32(len(data)),
		Data:   data,
	}

	walOff, err := v.appendWithRetry(entry)
	if err != nil {
		v.shipMu.Unlock()
		return err
	}

	v.shipMu.Unlock()

	// Check WAL pressure and notify flusher if threshold exceeded.
	if v.wal.UsedFraction() >= v.config.WALPressureThreshold {
		v.flusher.NotifyUrgent()
	}

	// Update dirty map: one entry per block written.
	blocks := uint32(len(data)) / v.super.BlockSize
	for i := uint32(0); i < blocks; i++ {
		blockOff := walOff // all blocks in this entry share the same WAL offset
		v.dirtyMap.Put(lba+uint64(i), blockOff, lsn, v.super.BlockSize)
	}

	return nil
}

// ReadLBA reads data at the given logical block address.
// length is in bytes and must be a multiple of BlockSize.
func (v *BlockVol) ReadLBA(lba uint64, length uint32) ([]byte, error) {
	if err := v.beginOp(); err != nil {
		return nil, err
	}
	defer v.endOp()
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()
	if err := ValidateWrite(lba, length, v.super.VolumeSize, v.super.BlockSize); err != nil {
		return nil, err
	}

	blocks := length / v.super.BlockSize
	result := make([]byte, length)

	for i := uint32(0); i < blocks; i++ {
		blockLBA := lba + uint64(i)
		blockData, err := v.readOneBlock(blockLBA)
		if err != nil {
			return nil, fmt.Errorf("blockvol: ReadLBA block %d: %w", blockLBA, err)
		}
		copy(result[i*v.super.BlockSize:], blockData)
	}

	return result, nil
}

// readOneBlock reads a single block, checking dirty map first, then extent.
func (v *BlockVol) readOneBlock(lba uint64) ([]byte, error) {
	for {
		walOff, lsn, _, ok := v.dirtyMap.Get(lba)
		if !ok {
			return v.readBlockFromExtent(lba)
		}
		data, stale, err := v.readBlockFromWAL(walOff, lba, lsn)
		if err != nil {
			return nil, err
		}
		if stale {
			// WAL slot was reused. Extent may not have the latest: a newer write
			// could have updated dirtyMap after we got our old entry. Re-check
			// dirtyMap before trusting extent; if a newer entry exists, retry.
			_, currentLSN, _, stillOk := v.dirtyMap.Get(lba)
			if stillOk && currentLSN != lsn {
				continue
			}
			return v.readBlockFromExtent(lba)
		}
		// Verify no newer write overwrote this LBA while we were reading.
		// A concurrent WriteLBA could have Put(lba, walOff_new, lsn_new) after
		// our Get; we would have read old data at walOff. Re-check and retry.
		_, currentLSN, _, stillOk := v.dirtyMap.Get(lba)
		if stillOk && currentLSN == lsn {
			return data, nil
		}
		// LSN changed (newer write) or flusher removed: retry with fresh dirtyMap state.
		continue
	}
}

// maxWALEntryDataLen caps the data length we trust from a WAL entry header.
// Anything larger than the WAL region itself is corrupt.
const maxWALEntryDataLen = 256 * 1024 * 1024 // 256MB absolute ceiling

// readBlockFromWAL reads a block's data from its WAL entry.
// Returns (data, stale, err). If stale is true, the WAL slot was reused
// by a newer entry (flusher reclaimed it) and the caller should fall back
// to the extent region.
func (v *BlockVol) readBlockFromWAL(walOff uint64, lba uint64, expectedLSN uint64) ([]byte, bool, error) {
	// Read the WAL entry header to get the full entry size.
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(v.super.WALOffset + walOff)
	if _, err := v.fd.ReadAt(headerBuf, absOff); err != nil {
		return nil, false, fmt.Errorf("readBlockFromWAL: read header at %d: %w", absOff, err)
	}

	// WAL reuse guard: validate LSN before trusting the entry.
	// If the flusher reclaimed this slot and a new write reused it,
	// the LSN will differ --fall back to extent read.
	entryLSN := binary.LittleEndian.Uint64(headerBuf[0:8])
	if entryLSN != expectedLSN {
		return nil, true, nil // stale --WAL slot reused
	}

	// Check entry type first --TRIM has no data payload, so Length is
	// metadata (trim extent), not a data size to allocate.
	entryType := headerBuf[16] // Type is at offset LSN(8) + Epoch(8) = 16
	if entryType == EntryTypeTrim {
		// TRIM entry: return zeros regardless of Length field.
		return make([]byte, v.super.BlockSize), false, nil
	}
	if entryType != EntryTypeWrite {
		// Unexpected type at a supposedly valid offset --treat as stale.
		return nil, true, nil
	}

	// Parse and validate the data Length field before allocating (WRITE only).
	dataLen := v.parseDataLength(headerBuf)
	if uint64(dataLen) > v.super.WALSize || uint64(dataLen) > maxWALEntryDataLen {
		// LSN matched but length is corrupt --real data integrity error.
		return nil, false, fmt.Errorf("readBlockFromWAL: corrupt entry length %d exceeds WAL size %d", dataLen, v.super.WALSize)
	}

	entryLen := walEntryHeaderSize + int(dataLen)
	fullBuf := make([]byte, entryLen)
	if _, err := v.fd.ReadAt(fullBuf, absOff); err != nil {
		return nil, false, fmt.Errorf("readBlockFromWAL: read entry at %d: %w", absOff, err)
	}

	entry, err := DecodeWALEntry(fullBuf)
	if err != nil {
		// LSN matched but CRC failed --real corruption.
		return nil, false, fmt.Errorf("readBlockFromWAL: decode: %w", err)
	}

	// Final guard: verify the entry actually covers this LBA.
	if lba < entry.LBA {
		return nil, true, nil // stale --different entry at same offset
	}
	blockOffset := (lba - entry.LBA) * uint64(v.super.BlockSize)
	if blockOffset+uint64(v.super.BlockSize) > uint64(len(entry.Data)) {
		return nil, true, nil // stale --LBA out of range
	}

	block := make([]byte, v.super.BlockSize)
	copy(block, entry.Data[blockOffset:blockOffset+uint64(v.super.BlockSize)])
	return block, false, nil
}

// parseDataLength extracts the Length field from a WAL entry header buffer.
func (v *BlockVol) parseDataLength(headerBuf []byte) uint32 {
	// Length is at offset: LSN(8) + Epoch(8) + Type(1) + Flags(1) + LBA(8) = 26
	return binary.LittleEndian.Uint32(headerBuf[26:])
}

// readBlockFromExtent reads a block directly from the extent region.
func (v *BlockVol) readBlockFromExtent(lba uint64) ([]byte, error) {
	extentStart := v.super.WALOffset + v.super.WALSize
	byteOffset := extentStart + lba*uint64(v.super.BlockSize)

	block := make([]byte, v.super.BlockSize)
	if _, err := v.fd.ReadAt(block, int64(byteOffset)); err != nil {
		return nil, fmt.Errorf("readBlockFromExtent: pread at %d: %w", byteOffset, err)
	}
	return block, nil
}

// Trim marks blocks as deallocated. Subsequent reads return zeros.
// The trim is recorded in the WAL with a Length field so the flusher
// can zero the extent region and recovery can replay the trim.
func (v *BlockVol) Trim(lba uint64, length uint32) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()
	if err := v.writeGate(); err != nil {
		return err
	}
	if err := ValidateWrite(lba, length, v.super.VolumeSize, v.super.BlockSize); err != nil {
		return err
	}

	// Admission control: throttle/block based on WAL pressure watermarks.
	if v.walAdmission != nil {
		if err := v.walAdmission.Acquire(v.config.WALFullTimeout); err != nil {
			return fmt.Errorf("blockvol: trim admission: %w", err)
		}
		defer v.walAdmission.Release()
	}

	// shipMu serializes LSN allocation + WAL append + Ship (same as WriteLBA).
	v.shipMu.Lock()

	lsn := v.nextLSN.Add(1) - 1
	entry := &WALEntry{
		LSN:    lsn,
		Epoch:  v.epoch.Load(),
		Type:   EntryTypeTrim,
		LBA:    lba,
		Length: length,
	}

	walOff, err := v.appendWithRetry(entry)
	if err != nil {
		v.shipMu.Unlock()
		return err
	}

	v.shipMu.Unlock()

	// Update dirty map: mark each trimmed block so the flusher sees it.
	// readOneBlock checks entry type and returns zeros for TRIM entries.
	blocks := length / v.super.BlockSize
	for i := uint32(0); i < blocks; i++ {
		v.dirtyMap.Put(lba+uint64(i), walOff, lsn, v.super.BlockSize)
	}

	return nil
}

// Path returns the file path of the block volume.
func (v *BlockVol) Path() string {
	return v.path
}

// Info returns volume metadata.
func (v *BlockVol) Info() VolumeInfo {
	return VolumeInfo{
		VolumeSize: v.super.VolumeSize,
		BlockSize:  v.super.BlockSize,
		ExtentSize: v.super.ExtentSize,
		WALSize:    v.super.WALSize,
		UUID:       v.super.UUID,
		Healthy:    v.healthy.Load(),
	}
}

// VolumeInfo contains read-only volume metadata.
type VolumeInfo struct {
	VolumeSize uint64
	BlockSize  uint32
	ExtentSize uint32
	WALSize    uint64
	UUID       [16]byte
	Healthy    bool
}

// DurabilityMode returns the volume's durability mode from the superblock.
func (v *BlockVol) DurabilityMode() DurabilityMode {
	return DurabilityMode(v.super.DurabilityMode)
}

// Profile returns the volume's storage profile from the superblock.
func (v *BlockVol) Profile() StorageProfile {
	return StorageProfile(v.super.StorageProfile)
}

// WALUsedFraction returns the fraction of WAL space currently in use (0.0 to 1.0).
func (v *BlockVol) WALUsedFraction() float64 {
	if v.wal == nil {
		return 0
	}
	return v.wal.UsedFraction()
}

// DirtyMapLen returns the number of entries in the dirty map.
func (v *BlockVol) DirtyMapLen() int {
	if v.dirtyMap == nil {
		return 0
	}
	return v.dirtyMap.Len()
}

// SyncCache ensures all previously written WAL entries are durable on disk.
// It submits a sync request to the group committer, which batches fsyncs.
func (v *BlockVol) SyncCache() error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	v.ioMu.RLock()
	defer v.ioMu.RUnlock()
	return v.groupCommit.Submit()
}

// ReplicaAddr holds the data and control addresses for one replica.
type ReplicaAddr struct {
	DataAddr string
	CtrlAddr string
	ServerID string // V2: stable server identity from registry (not address-derived)
}

// WALAccess provides the shipper with the minimal WAL interface needed
// for reconnect handshake and catch-up. Avoids exposing raw WAL internals.
type WALAccess interface {
	// RetainedRange returns the current WAL retained LSN range.
	// walRetainStart is the lowest LSN still in the WAL.
	// primaryHeadLSN is the highest LSN written.
	RetainedRange() (walRetainStart, primaryHeadLSN uint64)
	// StreamEntries streams WAL entries from fromLSN to the current head.
	// Calls fn for each entry. Returns ErrWALRecycled if fromLSN is no longer retained.
	StreamEntries(fromLSN uint64, fn func(*WALEntry) error) error
}

// walAccess implements WALAccess for BlockVol.
type walAccess struct {
	vol *BlockVol
}

func (a *walAccess) RetainedRange() (uint64, uint64) {
	checkpointLSN := uint64(0)
	if a.vol.flusher != nil {
		checkpointLSN = a.vol.flusher.CheckpointLSN()
	}
	headLSN := a.vol.nextLSN.Load()
	if headLSN > 0 {
		headLSN--
	}
	// WAL retain start: entries before checkpoint are eligible for reclaim.
	// The flusher may have already reclaimed them. Use checkpoint+1 as retain start.
	retainStart := checkpointLSN + 1
	return retainStart, headLSN
}

func (a *walAccess) StreamEntries(fromLSN uint64, fn func(*WALEntry) error) error {
	checkpointLSN := uint64(0)
	if a.vol.flusher != nil {
		checkpointLSN = a.vol.flusher.CheckpointLSN()
	}
	return a.vol.wal.ScanFrom(a.vol.fd, a.vol.super.WALOffset, checkpointLSN, fromLSN, fn)
}

// SetOnShipperStateChange registers a callback for shipper state transitions.
// Called by the volume server to trigger immediate heartbeat on degradation/recovery.
func (v *BlockVol) SetOnShipperStateChange(fn func(from, to ReplicaState)) {
	v.onShipperStateChange = fn
}

// GetShipperGroup returns the shipper group for debug/observability.
// Returns nil if no replication is configured.
func (v *BlockVol) GetShipperGroup() *ShipperGroup {
	return v.shipperGroup
}

// SetReplicaAddr configures a single replica endpoint. Backward-compatible wrapper
// around SetReplicaAddrs for RF=2 callers.
func (v *BlockVol) SetReplicaAddr(dataAddr, ctrlAddr string) {
	v.SetReplicaAddrs([]ReplicaAddr{{DataAddr: dataAddr, CtrlAddr: ctrlAddr}})
}

// SetReplicaAddrs configures N replica endpoints and creates a ShipperGroup
// with distributed group commit. Creates fresh shippers (old group is GC'd).
func (v *BlockVol) SetReplicaAddrs(addrs []ReplicaAddr) {
	wa := &walAccess{vol: v}
	shippers := make([]*WALShipper, len(addrs))
	for i, a := range addrs {
		shippers[i] = NewWALShipper(a.DataAddr, a.CtrlAddr, func() uint64 {
			return v.epoch.Load()
		}, wa, v.Metrics)
	}
	v.shipperGroup = NewShipperGroup(shippers)

	// Wire state change callback so shipper degradation triggers immediate heartbeat.
	if v.onShipperStateChange != nil {
		v.shipperGroup.SetOnStateChange(v.onShipperStateChange)
	}

	// Replace the group committer's sync function with a distributed version.
	v.groupCommit.Stop()
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:      MakeDistributedSync(v.fd.Sync, v.shipperGroup, v),
		MaxDelay:      v.config.GroupCommitMaxDelay,
		MaxBatch:      v.config.GroupCommitMaxBatch,
		LowWatermark:  v.config.GroupCommitLowWatermark,
		OnDegraded:    func() { v.healthy.Store(false) },
		PostSyncCheck: v.writeGate,
	})
	go v.groupCommit.Run()
}

// ReplicaShipperStates returns per-replica status from the primary's shipper group.
// Used by heartbeat to report which replicas need rebuild. Returns nil if no shippers.
func (v *BlockVol) ReplicaShipperStates() []ReplicaShipperStatus {
	if v.shipperGroup == nil {
		return nil
	}
	return v.shipperGroup.ShipperStates()
}

// V2StatusSnapshot holds the storage state fields needed by the V2 engine bridge.
type V2StatusSnapshot struct {
	WALHeadLSN        uint64
	WALTailLSN        uint64
	CommittedLSN      uint64
	CheckpointLSN     uint64
	CheckpointTrusted bool
}

// StatusSnapshot returns a snapshot of blockvol state for V2 engine consumption.
// Each field reads from the authoritative source:
//
//   WALHeadLSN        ← nextLSN - 1 (last written LSN)
//   WALTailLSN        ← super.WALCheckpointLSN (LSN boundary, not byte offset)
//   CommittedLSN      ← nextLSN - 1 (for sync_all: every write is barrier-confirmed)
//   CheckpointLSN     ← super.WALCheckpointLSN (durable base image)
//   CheckpointTrusted ← super.Validate() == nil (superblock integrity)
func (v *BlockVol) StatusSnapshot() V2StatusSnapshot {
	headLSN := v.nextLSN.Load()
	if headLSN > 0 {
		headLSN--
	}

	// WALTailLSN: the oldest retained LSN boundary for recovery classification.
	// Entries with LSN > WALTailLSN are guaranteed in the WAL.
	walTailLSN := v.super.WALCheckpointLSN

	// CommittedLSN: for sync_all mode, every write is barrier-confirmed
	// before returning. So WALHeadLSN (nextLSN-1) IS the committed boundary.
	// This separates CommittedLSN from CheckpointLSN — entries between
	// checkpoint and head are committed but not yet flushed to extent.
	committedLSN := headLSN

	return V2StatusSnapshot{
		WALHeadLSN:        headLSN,
		WALTailLSN:        walTailLSN,
		CommittedLSN:      committedLSN,
		CheckpointLSN:     v.super.WALCheckpointLSN,
		CheckpointTrusted: v.super.Validate() == nil,
	}
}

// SetV2RetentionFloor registers an additional retention floor function from the
// V2 bridge pinner. The flusher will check this floor before advancing the WAL
// tail, preventing reclaim past any held position.
func (v *BlockVol) SetV2RetentionFloor(fn func() (uint64, bool)) {
	if v.flusher != nil {
		// Chain with existing retention floor (from shipper group).
		existing := v.flusher.RetentionFloorFn()
		v.flusher.SetRetentionFloorFn(func() (uint64, bool) {
			var min uint64
			found := false
			if existing != nil {
				if lsn, ok := existing(); ok {
					min = lsn
					found = true
				}
			}
			if lsn, ok := fn(); ok {
				if !found || lsn < min {
					min = lsn
					found = true
				}
			}
			return min, found
		})
	}
}

// ScanWALEntries reads WAL entries from fromLSN using the real ScanFrom mechanism.
// This is the entry point for the V2 bridge executor's catch-up path.
//
// Uses super.WALCheckpointLSN as the recycled boundary (not flusher.CheckpointLSN).
// The superblock checkpoint is the durable boundary persisted to disk.
// The flusher's live checkpointLSN may have advanced further in memory
// but entries between super.WALCheckpointLSN and headLSN are still in the WAL.
func (v *BlockVol) ScanWALEntries(fromLSN uint64, fn func(*WALEntry) error) error {
	if v.wal == nil {
		return fmt.Errorf("WAL not initialized")
	}
	// Use the durable superblock checkpoint as the recycled boundary.
	// Entries with LSN > super.WALCheckpointLSN are guaranteed in the WAL.
	return v.wal.ScanFrom(v.fd, v.super.WALOffset, v.super.WALCheckpointLSN, fromLSN, fn)
}

// ForceFlush triggers a synchronous flush cycle. This advances the checkpoint
// and WAL tail. For test use — in production, the flusher runs automatically.
func (v *BlockVol) ForceFlush() error {
	if v.flusher == nil {
		return fmt.Errorf("flusher not initialized")
	}
	return v.flusher.FlushOnce()
}

// ReplicaReceiverAddrInfo holds canonical addresses from the replica receiver.
type ReplicaReceiverAddrInfo struct {
	DataAddr string
	CtrlAddr string
}

// ReplicaReceiverAddr returns the canonical addresses of the replica receiver,
// or nil if no receiver is running. Used by the VS to store canonical addresses
// in heartbeat state instead of raw assignment addresses.
func (v *BlockVol) ReplicaReceiverAddr() *ReplicaReceiverAddrInfo {
	if v.replRecv == nil {
		return nil
	}
	return &ReplicaReceiverAddrInfo{
		DataAddr: v.replRecv.DataAddr(),
		CtrlAddr: v.replRecv.CtrlAddr(),
	}
}

// StartReplicaReceiver starts listening for replicated WAL entries from a primary.
func (v *BlockVol) StartReplicaReceiver(dataAddr, ctrlAddr string) error {
	recv, err := NewReplicaReceiver(v, dataAddr, ctrlAddr)
	if err != nil {
		return err
	}
	v.replRecv = recv
	recv.Serve()
	return nil
}

// HandleAssignment processes a role/epoch/lease assignment from master.
func (v *BlockVol) HandleAssignment(epoch uint64, role Role, leaseTTL time.Duration) error {
	return HandleAssignment(v, epoch, role, leaseTTL)
}

// StartRebuildServer creates and starts a rebuild server on the given address.
func (v *BlockVol) StartRebuildServer(addr string) error {
	srv, err := NewRebuildServer(v, addr)
	if err != nil {
		return err
	}
	v.rebuildServer = srv
	srv.Serve()
	return nil
}

// StopRebuildServer stops the rebuild server if running.
func (v *BlockVol) StopRebuildServer() {
	if v.rebuildServer != nil {
		v.rebuildServer.Stop()
		v.rebuildServer = nil
	}
}

// degradeReplica logs a warning about a replica barrier failure.
// Individual shippers are already degraded by Barrier itself.
func (v *BlockVol) degradeReplica(err error) {
	log.Printf("blockvol: replica degraded: %v", err)
}

// BlockVolumeStatus contains block volume state for heartbeat reporting.
type BlockVolumeStatus struct {
	Epoch           uint64
	WALHeadLSN      uint64
	Role            Role
	CheckpointLSN   uint64
	HasLease        bool
	HealthScore     float64
	ReplicaDegraded bool
}

// Status returns the current block volume status for heartbeat reporting.
func (v *BlockVol) Status() BlockVolumeStatus {
	var cpLSN uint64
	if v.flusher != nil {
		cpLSN = v.flusher.CheckpointLSN()
	}
	var hs float64 = 1.0
	if v.healthScore != nil {
		hs = v.healthScore.Score()
	}
	var degraded bool
	if v.shipperGroup != nil {
		degraded = v.shipperGroup.AnyDegraded()
	}
	return BlockVolumeStatus{
		Epoch:           v.epoch.Load(),
		WALHeadLSN:      v.nextLSN.Load() - 1,
		Role:            v.Role(),
		CheckpointLSN:   cpLSN,
		HasLease:        v.lease.IsValid(),
		HealthScore:     hs,
		ReplicaDegraded: degraded,
	}
}


// WALStatus is a point-in-time snapshot of WAL pressure and admission metrics.
type WALStatus struct {
	UsedFraction        float64 // current WAL usage 0.0–1.0
	PressureState       string  // "normal", "soft", "hard"
	SoftWatermark       float64 // configured soft threshold
	HardWatermark       float64 // configured hard threshold
	SoftAdmitTotal      uint64  // soft watermark throttle events
	HardAdmitTotal      uint64  // hard watermark block events
	TimeoutTotal        uint64  // ErrWALFull timeouts
	AdmitWaitTotalSec   float64 // cumulative wait time in Acquire (seconds)
	SoftPressureWaitSec float64 // cumulative writer wait in soft zone (seconds)
	HardPressureWaitSec float64 // cumulative writer wait in hard zone (seconds)
}

// WALStatus returns a point-in-time snapshot of WAL pressure state and admission metrics.
func (v *BlockVol) WALStatus() WALStatus {
	ws := WALStatus{
		UsedFraction:  v.WALUsedFraction(),
		PressureState: "normal",
		SoftWatermark: 0.7,
		HardWatermark: 0.9,
	}
	if v.walAdmission != nil {
		ws.PressureState = v.walAdmission.PressureState()
		ws.SoftWatermark = v.walAdmission.SoftMark()
		ws.HardWatermark = v.walAdmission.HardMark()
		ws.SoftPressureWaitSec = float64(v.walAdmission.SoftPressureWaitNs()) / 1e9
		ws.HardPressureWaitSec = float64(v.walAdmission.HardPressureWaitNs()) / 1e9
	}
	if v.Metrics != nil {
		ws.SoftAdmitTotal = v.Metrics.WALAdmitSoftTotal.Load()
		ws.HardAdmitTotal = v.Metrics.WALAdmitHardTotal.Load()
		ws.TimeoutTotal = v.Metrics.WALAdmitTimeoutTotal.Load()
		_, sumNs := v.Metrics.WALAdmitWaitSnapshot()
		ws.AdmitWaitTotalSec = float64(sumNs) / 1e9
	}
	return ws
}

// WALPressureState returns the current WAL pressure state ("normal", "soft", "hard").
func (v *BlockVol) WALPressureState() string {
	if v.walAdmission == nil {
		return "normal"
	}
	return v.walAdmission.PressureState()
}

// WALSoftPressureWaitNs returns cumulative nanoseconds writers spent waiting in the soft zone.
func (v *BlockVol) WALSoftPressureWaitNs() int64 {
	if v.walAdmission == nil {
		return 0
	}
	return v.walAdmission.SoftPressureWaitNs()
}

// WALHardPressureWaitNs returns cumulative nanoseconds writers spent waiting in the hard zone.
func (v *BlockVol) WALHardPressureWaitNs() int64 {
	if v.walAdmission == nil {
		return 0
	}
	return v.walAdmission.HardPressureWaitNs()
}

// CheckpointLSN returns the last LSN flushed to the extent region.
func (v *BlockVol) CheckpointLSN() uint64 {
	if v.flusher != nil {
		return v.flusher.CheckpointLSN()
	}
	return 0
}

// HealthScore returns the current health score (0.0-1.0).
func (v *BlockVol) HealthScore() float64 {
	if v.healthScore == nil {
		return 1.0
	}
	return v.healthScore.Score()
}

// HealthStats returns detailed health statistics.
func (v *BlockVol) HealthStats() HealthStats {
	if v.healthScore == nil {
		return HealthStats{Score: 1.0}
	}
	return v.healthScore.Stats()
}

// StartScrub starts background scrubbing with the given interval.
func (v *BlockVol) StartScrub(interval time.Duration) {
	if v.scrubber != nil {
		return
	}
	v.scrubber = NewScrubber(v, interval)
	v.scrubber.Start()
}

// StopScrub stops the background scrubber.
func (v *BlockVol) StopScrub() {
	if v.scrubber != nil {
		v.scrubber.Stop()
		v.scrubber = nil
	}
}

// TriggerScrub triggers an immediate scrub pass.
func (v *BlockVol) TriggerScrub() {
	if v.scrubber != nil {
		v.scrubber.TriggerNow()
	}
}

// ScrubStats returns the current scrub statistics.
func (v *BlockVol) ScrubStats() ScrubStats {
	if v.scrubber == nil {
		return ScrubStats{}
	}
	return v.scrubber.Stats()
}

// CreateSnapshot creates a point-in-time snapshot. The snapshot captures the
// exact volume state after all pending writes have been flushed to the extent.
// Only allowed on Primary or None (standalone) roles.
func (v *BlockVol) CreateSnapshot(id uint32) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()

	// Role check: only Primary or None (standalone).
	r := Role(v.role.Load())
	if r != RoleNone && r != RolePrimary {
		return ErrSnapshotRoleReject
	}

	// Hold snapMu across duplicate check + insert to prevent two concurrent
	// CreateSnapshot(id) from both passing the check. (Fix #3: TOCTOU race.)
	v.snapMu.Lock()
	if _, exists := v.snapshots[id]; exists {
		v.snapMu.Unlock()
		return ErrSnapshotExists
	}

	// SyncCache: ensure all prior WAL writes are durable.
	if err := v.groupCommit.Submit(); err != nil {
		v.snapMu.Unlock()
		return fmt.Errorf("blockvol: snapshot sync: %w", err)
	}

	// Pause flusher and flush everything to extent.
	if err := v.flusher.PauseAndFlush(); err != nil {
		v.flusher.Resume()
		v.snapMu.Unlock()
		return fmt.Errorf("blockvol: snapshot flush: %w", err)
	}
	// flusher is now paused (flushMu held) -- extent is consistent.

	baseLSN := v.flusher.CheckpointLSN()

	// Create delta file.
	deltaPath := deltaFilePath(v.path, id)
	snap, err := createDeltaFile(deltaPath, id, v, baseLSN)
	if err != nil {
		v.flusher.Resume()
		v.snapMu.Unlock()
		return err
	}

	// Register snapshot in flusher and volume (still under snapMu + flushMu).
	v.flusher.AddSnapshot(snap)
	v.snapshots[id] = snap
	v.snapMu.Unlock()
	v.flusher.Resume()

	// Update superblock snapshot count.
	v.snapMu.RLock()
	v.super.SnapshotCount = uint32(len(v.snapshots))
	v.snapMu.RUnlock()
	return v.persistSuperblock()
}

// ReadSnapshot reads data from a snapshot at the given LBA and length (bytes).
func (v *BlockVol) ReadSnapshot(id uint32, lba uint64, length uint32) ([]byte, error) {
	if err := v.beginOp(); err != nil {
		return nil, err
	}
	defer v.endOp()

	if err := ValidateWrite(lba, length, v.super.VolumeSize, v.super.BlockSize); err != nil {
		return nil, err
	}

	v.snapMu.RLock()
	snap, ok := v.snapshots[id]
	v.snapMu.RUnlock()
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	blocks := length / v.super.BlockSize
	result := make([]byte, length)

	for i := uint32(0); i < blocks; i++ {
		blockLBA := lba + uint64(i)
		off := i * v.super.BlockSize
		if snap.bitmap.Get(blockLBA) {
			// CoW'd block: read from delta file.
			deltaOff := int64(snap.dataOffset + blockLBA*uint64(v.super.BlockSize))
			if _, err := snap.fd.ReadAt(result[off:off+v.super.BlockSize], deltaOff); err != nil {
				return nil, fmt.Errorf("blockvol: read snapshot delta LBA %d: %w", blockLBA, err)
			}
		} else {
			// Not CoW'd: read from extent (still has snapshot-time data).
			extentStart := v.super.WALOffset + v.super.WALSize
			extentOff := int64(extentStart + blockLBA*uint64(v.super.BlockSize))
			if _, err := v.fd.ReadAt(result[off:off+v.super.BlockSize], extentOff); err != nil {
				return nil, fmt.Errorf("blockvol: read snapshot extent LBA %d: %w", blockLBA, err)
			}
		}
	}

	return result, nil
}

// DeleteSnapshot removes a snapshot and deletes its delta file.
func (v *BlockVol) DeleteSnapshot(id uint32) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()

	v.snapMu.Lock()
	snap, ok := v.snapshots[id]
	if !ok {
		v.snapMu.Unlock()
		return ErrSnapshotNotFound
	}
	delete(v.snapshots, id)
	remaining := uint32(len(v.snapshots))
	v.snapMu.Unlock()

	// Pause flusher so no in-flight CoW cycle can use snap.fd after close.
	// (Fix #1: use-after-close race.)
	v.flusher.PauseAndFlush()
	v.flusher.RemoveSnapshot(id)
	v.flusher.Resume()

	// Close and remove delta file. Safe now -- flusher cannot reference snap.
	deltaPath := deltaFilePath(v.path, id)
	snap.Close()
	os.Remove(deltaPath)

	// Update superblock.
	v.super.SnapshotCount = remaining
	return v.persistSuperblock()
}

// RestoreSnapshot reverts the live volume to the state captured by the snapshot.
// This is destructive: all writes after the snapshot are lost. All snapshots are
// removed after restore. The volume must have no active I/O (call after Close
// coordination or on a standalone volume).
func (v *BlockVol) RestoreSnapshot(id uint32) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()

	// Exclusive lock: drains all in-flight I/O before modifying extent/WAL/dirtyMap.
	v.ioMu.Lock()
	defer v.ioMu.Unlock()

	v.snapMu.RLock()
	snap, ok := v.snapshots[id]
	v.snapMu.RUnlock()
	if !ok {
		return ErrSnapshotNotFound
	}

	// Pause flusher. Check error -- if flush fails, don't proceed.
	if err := v.flusher.PauseAndFlush(); err != nil {
		v.flusher.Resume()
		return fmt.Errorf("blockvol: restore flush: %w", err)
	}
	defer v.flusher.Resume()

	// Copy CoW'd blocks from delta back to extent.
	extentStart := v.super.WALOffset + v.super.WALSize
	totalBlocks := v.super.VolumeSize / uint64(v.super.BlockSize)
	buf := make([]byte, v.super.BlockSize)
	for lba := uint64(0); lba < totalBlocks; lba++ {
		if snap.bitmap.Get(lba) {
			deltaOff := int64(snap.dataOffset + lba*uint64(v.super.BlockSize))
			if _, err := snap.fd.ReadAt(buf, deltaOff); err != nil {
				return fmt.Errorf("blockvol: restore read delta LBA %d: %w", lba, err)
			}
			extentOff := int64(extentStart + lba*uint64(v.super.BlockSize))
			if _, err := v.fd.WriteAt(buf, extentOff); err != nil {
				return fmt.Errorf("blockvol: restore write extent LBA %d: %w", lba, err)
			}
		}
	}

	// Fsync extent.
	if err := v.fd.Sync(); err != nil {
		return fmt.Errorf("blockvol: restore fsync: %w", err)
	}

	// Clear dirty map.
	v.dirtyMap.Clear()

	// Reset WAL.
	v.wal.Reset()
	v.super.WALHead = 0
	v.super.WALTail = 0
	v.super.WALCheckpointLSN = snap.header.BaseLSN
	v.nextLSN.Store(snap.header.BaseLSN + 1)

	// Remove ALL snapshots. Flusher is paused, so no CoW cycle can race.
	v.snapMu.Lock()
	for sid, s := range v.snapshots {
		v.flusher.RemoveSnapshot(sid)
		s.Close()
		os.Remove(deltaFilePath(v.path, sid))
	}
	v.snapshots = make(map[uint32]*activeSnapshot)
	v.snapMu.Unlock()

	// Update superblock.
	v.super.SnapshotCount = 0
	return v.persistSuperblock()
}

// ListSnapshots returns metadata for all active snapshots.
func (v *BlockVol) ListSnapshots() []SnapshotInfo {
	v.snapMu.RLock()
	defer v.snapMu.RUnlock()
	infos := make([]SnapshotInfo, 0, len(v.snapshots))
	for _, snap := range v.snapshots {
		infos = append(infos, SnapshotInfo{
			ID:        snap.id,
			BaseLSN:   snap.header.BaseLSN,
			CreatedAt: time.Unix(int64(snap.header.CreatedAt), 0),
			CoWBlocks: snap.bitmap.CountSet(),
		})
	}
	return infos
}

var (
	ErrShrinkNotSupported     = errors.New("blockvol: shrink not supported")
	ErrSnapshotsPreventResize = errors.New("blockvol: cannot resize with active snapshots")
	ErrExpandAlreadyInFlight  = errors.New("blockvol: expand already in flight")
	ErrExpandEpochMismatch    = errors.New("blockvol: expand epoch mismatch")
	ErrNoExpandInFlight       = errors.New("blockvol: no expand in flight")
	ErrSameSize               = errors.New("blockvol: new size equals current size")
)

// growFile extends the backing file to accommodate newSize bytes of extent data.
// Validates size, alignment, and snapshot constraints. Pauses/resumes flusher.
// Does NOT update VolumeSize in the superblock.
func (v *BlockVol) growFile(newSize uint64) error {
	if newSize <= v.super.VolumeSize {
		if newSize == v.super.VolumeSize {
			return nil // no-op, caller should check
		}
		return ErrShrinkNotSupported
	}
	if newSize%uint64(v.super.BlockSize) != 0 {
		return ErrAlignment
	}

	// Hold snapMu across entire operation to prevent concurrent CreateSnapshot.
	v.snapMu.RLock()
	defer v.snapMu.RUnlock()
	if len(v.snapshots) > 0 {
		return ErrSnapshotsPreventResize
	}

	// Pause flusher (no concurrent extent writes during file extension).
	if err := v.flusher.PauseAndFlush(); err != nil {
		v.flusher.Resume()
		return fmt.Errorf("blockvol: expand flush: %w", err)
	}
	defer v.flusher.Resume()

	// Extend file.
	extentStart := v.super.WALOffset + v.super.WALSize
	newFileSize := int64(extentStart + newSize)
	if err := v.fd.Truncate(newFileSize); err != nil {
		return fmt.Errorf("blockvol: expand truncate: %w", err)
	}
	return nil
}

// Expand grows the volume to newSize bytes (standalone direct-commit).
// newSize must be larger than the current size and aligned to BlockSize.
// Fails if snapshots are active. No PreparedSize involved.
func (v *BlockVol) Expand(newSize uint64) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	// Exclusive ioMu: drain I/O before file growth + VolumeSize mutation.
	v.ioMu.Lock()
	defer v.ioMu.Unlock()
	if err := v.writeGate(); err != nil {
		return err
	}

	if newSize == v.super.VolumeSize {
		return nil // no-op
	}

	if err := v.growFile(newSize); err != nil {
		return err
	}

	// Update superblock: direct-commit.
	v.super.VolumeSize = newSize
	v.super.PreparedSize = 0  // defensive clear
	v.super.ExpandEpoch = 0
	return v.persistSuperblock()
}

// PrepareExpand grows the file and records the pending expand in the superblock
// without updating VolumeSize. Writes beyond the old VolumeSize are rejected
// by ValidateWrite until CommitExpand is called.
func (v *BlockVol) PrepareExpand(newSize, expandEpoch uint64) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()

	// Exclusive ioMu: drain I/O before file growth.
	v.ioMu.Lock()
	defer v.ioMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.super.PreparedSize != 0 {
		return ErrExpandAlreadyInFlight
	}

	if newSize <= v.super.VolumeSize {
		if newSize == v.super.VolumeSize {
			return ErrSameSize
		}
		return ErrShrinkNotSupported
	}

	if err := v.growFile(newSize); err != nil {
		return err
	}

	v.super.PreparedSize = newSize
	v.super.ExpandEpoch = expandEpoch
	return v.persistSuperblock()
}

// CommitExpand activates the prepared expand: VolumeSize = PreparedSize.
// Returns ErrNoExpandInFlight if no prepare was done, or ErrExpandEpochMismatch
// if the epoch doesn't match.
func (v *BlockVol) CommitExpand(expandEpoch uint64) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	v.ioMu.Lock()
	defer v.ioMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.super.PreparedSize == 0 {
		return ErrNoExpandInFlight
	}
	if v.super.ExpandEpoch != expandEpoch {
		return ErrExpandEpochMismatch
	}

	v.super.VolumeSize = v.super.PreparedSize
	v.super.PreparedSize = 0
	v.super.ExpandEpoch = 0
	return v.persistSuperblock()
}

// CancelExpand clears the prepared expand state without activating it.
// The file stays physically grown (sparse, harmless).
// If expandEpoch is 0, force-cancels regardless of current epoch.
func (v *BlockVol) CancelExpand(expandEpoch uint64) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()

	v.ioMu.Lock()
	defer v.ioMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if expandEpoch != 0 && v.super.ExpandEpoch != expandEpoch {
		return ErrExpandEpochMismatch
	}

	v.super.PreparedSize = 0
	v.super.ExpandEpoch = 0
	return v.persistSuperblock()
}

// ExpandState returns the current prepared expand state.
func (v *BlockVol) ExpandState() (preparedSize, expandEpoch uint64) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.super.PreparedSize, v.super.ExpandEpoch
}

// persistSuperblock writes the superblock to disk and fsyncs.
func (v *BlockVol) persistSuperblock() error {
	v.superMu.Lock()
	defer v.superMu.Unlock()
	if _, err := v.fd.Seek(0, 0); err != nil {
		return fmt.Errorf("blockvol: persist superblock seek: %w", err)
	}
	if _, err := v.super.WriteTo(v.fd); err != nil {
		return fmt.Errorf("blockvol: persist superblock write: %w", err)
	}
	if err := v.fd.Sync(); err != nil {
		return fmt.Errorf("blockvol: persist superblock sync: %w", err)
	}
	return nil
}

// Close shuts down the block volume and closes the file.
// Shutdown order: shippers -> replica receiver -> rebuild server -> drain ops -> group committer -> flusher -> final flush -> close fd.
func (v *BlockVol) Close() error {
	v.closed.Store(true)

	// Stop scrubber first (no more extent reads).
	v.StopScrub()

	// Stop shippers: no more Ship calls.
	if v.shipperGroup != nil {
		v.shipperGroup.StopAll()
	}
	// Stop replica receiver: no more barrier waits.
	if v.replRecv != nil {
		v.replRecv.Stop()
	}
	// Stop rebuild server.
	v.StopRebuildServer()

	// Drain in-flight ops: beginOp checks closed and returns ErrVolumeClosed,
	// so no new ops can start. Wait for existing ones to finish (max 5s).
	if v.opsOutstanding.Load() > 0 {
		select {
		case <-v.opsDrained:
		case <-time.After(5 * time.Second):
			// Proceed with shutdown even if ops are stuck.
		}
	}

	if v.groupCommit != nil {
		v.groupCommit.Stop()
	}
	var flushErr error
	if v.flusher != nil {
		v.flusher.Stop()                 // stop background goroutine first (no concurrent flush)
		flushErr = v.flusher.FlushOnce() // then do final flush safely
		v.flusher.CloseBatchIO()         // release io_uring ring / kernel resources
	}

	// Close snapshot delta files.
	v.snapMu.Lock()
	for _, snap := range v.snapshots {
		snap.Close()
	}
	v.snapshots = nil
	v.snapMu.Unlock()

	closeErr := v.fd.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

// newBatchIO creates a BatchIO backend based on the IOBackend config mode.
//
// Returns (backend, selectedName, error):
//   - "standard": always succeeds
//   - "auto": tries io_uring, falls back to standard with warning
//   - "io_uring": requires io_uring, returns error if unavailable
func newBatchIO(mode IOBackendMode, logger *log.Logger) (batchio.BatchIO, string, error) {
	impl := batchio.IOUringImpl // compiled-in implementation ("iceber", "giouring", "raw", or "")
	if impl == "" {
		impl = "none"
	}

	switch mode {
	case IOBackendIOUring:
		bio, err := batchio.NewIOUring(256)
		if err != nil {
			return nil, "", fmt.Errorf("io_uring requested but unavailable (compiled=%s): %w", impl, err)
		}
		logger.Printf("io backend: requested=io_uring implementation=%s selected=io_uring", impl)
		return bio, "io_uring", nil

	case IOBackendAuto:
		bio, err := batchio.NewIOUring(256)
		if err != nil {
			logger.Printf("io backend: requested=auto implementation=%s selected=standard reason=%v", impl, err)
			return batchio.NewStandard(), "standard", nil
		}
		logger.Printf("io backend: requested=auto implementation=%s selected=io_uring", impl)
		return bio, "io_uring", nil

	default: // IOBackendStandard or empty
		logger.Printf("io backend: requested=standard implementation=%s selected=standard", impl)
		return batchio.NewStandard(), "standard", nil
	}
}
