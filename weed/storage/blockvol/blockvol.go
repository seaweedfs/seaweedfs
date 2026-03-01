// Package blockvol implements a block volume storage engine with WAL,
// dirty map, group commit, and crash recovery. It operates on a single
// file and has no dependency on SeaweedFS internals.
package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// CreateOptions configures a new block volume.
type CreateOptions struct {
	VolumeSize  uint64 // required, logical size in bytes
	ExtentSize  uint32 // default 64KB
	BlockSize   uint32 // default 4KB
	WALSize     uint64 // default 64MB
	Replication string // default "000"
}

// ErrVolumeClosed is returned when an operation is attempted on a closed BlockVol.
var ErrVolumeClosed = errors.New("blockvol: volume closed")

// BlockVol is the core block volume engine.
type BlockVol struct {
	mu             sync.RWMutex
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
		fd:         fd,
		path:       path,
		super:      sb,
		config:     cfg,
		wal:        wal,
		dirtyMap:   dm,
		opsDrained: make(chan struct{}, 1),
	}
	v.nextLSN.Store(1)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:     fd.Sync,
		MaxDelay:     cfg.GroupCommitMaxDelay,
		MaxBatch:     cfg.GroupCommitMaxBatch,
		LowWatermark: cfg.GroupCommitLowWatermark,
		OnDegraded:   func() { v.healthy.Store(false) },
	})
	go v.groupCommit.Run()
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		WAL:      wal,
		DirtyMap: dm,
		Interval: cfg.FlushInterval,
	})
	go v.flusher.Run()
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
		fd:         fd,
		path:       path,
		super:      sb,
		config:     cfg,
		wal:        wal,
		dirtyMap:   dirtyMap,
		opsDrained: make(chan struct{}, 1),
	}
	v.nextLSN.Store(nextLSN)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:     fd.Sync,
		MaxDelay:     cfg.GroupCommitMaxDelay,
		MaxBatch:     cfg.GroupCommitMaxBatch,
		LowWatermark: cfg.GroupCommitLowWatermark,
		OnDegraded:   func() { v.healthy.Store(false) },
	})
	go v.groupCommit.Run()
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		WAL:      wal,
		DirtyMap: dirtyMap,
		Interval: cfg.FlushInterval,
	})
	go v.flusher.Run()
	return v, nil
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
	return walOff, nil
}

// WriteLBA writes data at the given logical block address.
// Data length must be a multiple of BlockSize.
func (v *BlockVol) WriteLBA(lba uint64, data []byte) error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	if err := ValidateWrite(lba, uint32(len(data)), v.super.VolumeSize, v.super.BlockSize); err != nil {
		return err
	}

	lsn := v.nextLSN.Add(1) - 1
	entry := &WALEntry{
		LSN:    lsn,
		Epoch:  0, // Phase 1: no fencing
		Type:   EntryTypeWrite,
		LBA:    lba,
		Length: uint32(len(data)),
		Data:   data,
	}

	walOff, err := v.appendWithRetry(entry)
	if err != nil {
		return err
	}

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
	walOff, lsn, _, ok := v.dirtyMap.Get(lba)
	if ok {
		data, stale, err := v.readBlockFromWAL(walOff, lba, lsn)
		if err != nil {
			return nil, err
		}
		if !stale {
			return data, nil
		}
		// WAL slot was reused (flusher reclaimed it between our
		// dirty map read and WAL read). The data is already flushed
		// to the extent region, so fall through to extent read.
	}
	return v.readBlockFromExtent(lba)
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
	// the LSN will differ — fall back to extent read.
	entryLSN := binary.LittleEndian.Uint64(headerBuf[0:8])
	if entryLSN != expectedLSN {
		return nil, true, nil // stale — WAL slot reused
	}

	// Check entry type first — TRIM has no data payload, so Length is
	// metadata (trim extent), not a data size to allocate.
	entryType := headerBuf[16] // Type is at offset LSN(8) + Epoch(8) = 16
	if entryType == EntryTypeTrim {
		// TRIM entry: return zeros regardless of Length field.
		return make([]byte, v.super.BlockSize), false, nil
	}
	if entryType != EntryTypeWrite {
		// Unexpected type at a supposedly valid offset — treat as stale.
		return nil, true, nil
	}

	// Parse and validate the data Length field before allocating (WRITE only).
	dataLen := v.parseDataLength(headerBuf)
	if uint64(dataLen) > v.super.WALSize || uint64(dataLen) > maxWALEntryDataLen {
		// LSN matched but length is corrupt — real data integrity error.
		return nil, false, fmt.Errorf("readBlockFromWAL: corrupt entry length %d exceeds WAL size %d", dataLen, v.super.WALSize)
	}

	entryLen := walEntryHeaderSize + int(dataLen)
	fullBuf := make([]byte, entryLen)
	if _, err := v.fd.ReadAt(fullBuf, absOff); err != nil {
		return nil, false, fmt.Errorf("readBlockFromWAL: read entry at %d: %w", absOff, err)
	}

	entry, err := DecodeWALEntry(fullBuf)
	if err != nil {
		// LSN matched but CRC failed — real corruption.
		return nil, false, fmt.Errorf("readBlockFromWAL: decode: %w", err)
	}

	// Final guard: verify the entry actually covers this LBA.
	if lba < entry.LBA {
		return nil, true, nil // stale — different entry at same offset
	}
	blockOffset := (lba - entry.LBA) * uint64(v.super.BlockSize)
	if blockOffset+uint64(v.super.BlockSize) > uint64(len(entry.Data)) {
		return nil, true, nil // stale — LBA out of range
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
	if err := ValidateWrite(lba, length, v.super.VolumeSize, v.super.BlockSize); err != nil {
		return err
	}

	lsn := v.nextLSN.Add(1) - 1
	entry := &WALEntry{
		LSN:    lsn,
		Epoch:  0,
		Type:   EntryTypeTrim,
		LBA:    lba,
		Length: length,
	}

	walOff, err := v.appendWithRetry(entry)
	if err != nil {
		return err
	}

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
		Healthy:    v.healthy.Load(),
	}
}

// VolumeInfo contains read-only volume metadata.
type VolumeInfo struct {
	VolumeSize uint64
	BlockSize  uint32
	ExtentSize uint32
	WALSize    uint64
	Healthy    bool
}

// SyncCache ensures all previously written WAL entries are durable on disk.
// It submits a sync request to the group committer, which batches fsyncs.
func (v *BlockVol) SyncCache() error {
	if err := v.beginOp(); err != nil {
		return err
	}
	defer v.endOp()
	return v.groupCommit.Submit()
}

// Close shuts down the block volume and closes the file.
// Shutdown order: drain in-flight ops → group committer → flusher → final flush → close fd.
func (v *BlockVol) Close() error {
	v.closed.Store(true)

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
		v.flusher.Stop()         // stop background goroutine first (no concurrent flush)
		flushErr = v.flusher.FlushOnce() // then do final flush safely
	}
	closeErr := v.fd.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
