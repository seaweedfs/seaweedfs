// Package blockvol implements a block volume storage engine with WAL,
// dirty map, group commit, and crash recovery. It operates on a single
// file and has no dependency on SeaweedFS internals.
package blockvol

import (
	"encoding/binary"
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

// BlockVol is the core block volume engine.
type BlockVol struct {
	mu          sync.RWMutex
	fd          *os.File
	path        string
	super       Superblock
	wal         *WALWriter
	dirtyMap    *DirtyMap
	groupCommit *GroupCommitter
	flusher     *Flusher
	nextLSN     atomic.Uint64
	healthy     atomic.Bool
}

// CreateBlockVol creates a new block volume file at path.
func CreateBlockVol(path string, opts CreateOptions) (*BlockVol, error) {
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

	dm := NewDirtyMap()
	wal := NewWALWriter(fd, sb.WALOffset, sb.WALSize, 0, 0)
	v := &BlockVol{
		fd:       fd,
		path:     path,
		super:    sb,
		wal:      wal,
		dirtyMap: dm,
	}
	v.nextLSN.Store(1)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:   fd.Sync,
		OnDegraded: func() { v.healthy.Store(false) },
	})
	go v.groupCommit.Run()
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		WAL:      wal,
		DirtyMap: dm,
	})
	go v.flusher.Run()
	return v, nil
}

// OpenBlockVol opens an existing block volume file and runs crash recovery.
func OpenBlockVol(path string) (*BlockVol, error) {
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

	dirtyMap := NewDirtyMap()

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
		fd:       fd,
		path:     path,
		super:    sb,
		wal:      wal,
		dirtyMap: dirtyMap,
	}
	v.nextLSN.Store(nextLSN)
	v.healthy.Store(true)
	v.groupCommit = NewGroupCommitter(GroupCommitterConfig{
		SyncFunc:   fd.Sync,
		OnDegraded: func() { v.healthy.Store(false) },
	})
	go v.groupCommit.Run()
	v.flusher = NewFlusher(FlusherConfig{
		FD:       fd,
		Super:    &v.super,
		WAL:      wal,
		DirtyMap: dirtyMap,
	})
	go v.flusher.Run()
	return v, nil
}

// WriteLBA writes data at the given logical block address.
// Data length must be a multiple of BlockSize.
func (v *BlockVol) WriteLBA(lba uint64, data []byte) error {
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

	walOff, err := v.wal.Append(entry)
	if err != nil {
		return fmt.Errorf("blockvol: WriteLBA: %w", err)
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
	walOff, _, _, ok := v.dirtyMap.Get(lba)
	if ok {
		return v.readBlockFromWAL(walOff, lba)
	}
	return v.readBlockFromExtent(lba)
}

// maxWALEntryDataLen caps the data length we trust from a WAL entry header.
// Anything larger than the WAL region itself is corrupt.
const maxWALEntryDataLen = 256 * 1024 * 1024 // 256MB absolute ceiling

// readBlockFromWAL reads a block's data from its WAL entry.
func (v *BlockVol) readBlockFromWAL(walOff uint64, lba uint64) ([]byte, error) {
	// Read the WAL entry header to get the full entry size.
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(v.super.WALOffset + walOff)
	if _, err := v.fd.ReadAt(headerBuf, absOff); err != nil {
		return nil, fmt.Errorf("readBlockFromWAL: read header at %d: %w", absOff, err)
	}

	// Check entry type first — TRIM has no data payload, so Length is
	// metadata (trim extent), not a data size to allocate.
	entryType := headerBuf[16] // Type is at offset LSN(8) + Epoch(8) = 16
	if entryType == EntryTypeTrim {
		// TRIM entry: return zeros regardless of Length field.
		return make([]byte, v.super.BlockSize), nil
	}
	if entryType != EntryTypeWrite {
		return nil, fmt.Errorf("readBlockFromWAL: expected WRITE or TRIM entry, got type 0x%02x", entryType)
	}

	// Parse and validate the data Length field before allocating (WRITE only).
	dataLen := v.parseDataLength(headerBuf)
	if uint64(dataLen) > v.super.WALSize || uint64(dataLen) > maxWALEntryDataLen {
		return nil, fmt.Errorf("readBlockFromWAL: corrupt entry length %d exceeds WAL size %d", dataLen, v.super.WALSize)
	}

	entryLen := walEntryHeaderSize + int(dataLen)
	fullBuf := make([]byte, entryLen)
	if _, err := v.fd.ReadAt(fullBuf, absOff); err != nil {
		return nil, fmt.Errorf("readBlockFromWAL: read entry at %d: %w", absOff, err)
	}

	entry, err := DecodeWALEntry(fullBuf)
	if err != nil {
		return nil, fmt.Errorf("readBlockFromWAL: decode: %w", err)
	}

	// Find the block within the entry's data.
	blockOffset := (lba - entry.LBA) * uint64(v.super.BlockSize)
	if blockOffset+uint64(v.super.BlockSize) > uint64(len(entry.Data)) {
		return nil, fmt.Errorf("readBlockFromWAL: block offset %d out of range for entry data len %d", blockOffset, len(entry.Data))
	}

	block := make([]byte, v.super.BlockSize)
	copy(block, entry.Data[blockOffset:blockOffset+uint64(v.super.BlockSize)])
	return block, nil
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

	walOff, err := v.wal.Append(entry)
	if err != nil {
		return fmt.Errorf("blockvol: Trim: %w", err)
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
	return v.groupCommit.Submit()
}

// Close shuts down the block volume and closes the file.
// Shutdown order: group committer → stop flusher goroutine → final flush → close fd.
func (v *BlockVol) Close() error {
	if v.groupCommit != nil {
		v.groupCommit.Stop()
	}
	var flushErr error
	if v.flusher != nil {
		v.flusher.Stop()      // stop background goroutine first (no concurrent flush)
		flushErr = v.flusher.FlushOnce() // then do final flush safely
	}
	closeErr := v.fd.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}
