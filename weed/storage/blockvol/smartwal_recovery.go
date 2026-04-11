package blockvol

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

// SmartWALVolume is the minimal prototype volume that uses SmartWAL.
// It combines an extent file (data) with a SmartWAL ring buffer (metadata).
// Together they form a logical LSN-ordered storage.
//
// This is a self-contained prototype — it does not modify or replace
// the existing BlockVol implementation.
type SmartWALVolume struct {
	extentFD  *os.File
	wal       *SmartWALBuffer
	blockSize uint64
	numBlocks uint64
	epoch     uint64
	nextLSN   atomic.Uint64

	// dirtyMap tracks which LBAs have been written since last clean state.
	// Maps LBA → highest LSN that wrote to it. Used for replication delta.
	dirtyMu  sync.Mutex
	dirtyMap map[uint32]uint64
}

// SmartWALVolumeConfig configures a SmartWAL prototype volume.
type SmartWALVolumeConfig struct {
	ExtentPath string // path to extent data file
	WALPath    string // path to WAL ring buffer file
	BlockSize  uint64 // block size in bytes (must be 4096)
	NumBlocks  uint64 // number of blocks in the extent
	WALSlots   uint64 // number of WAL slots (default: 65536 = 2MB WAL)
	Epoch      uint64 // fencing epoch
}

// CreateSmartWALVolume creates a new SmartWAL volume with zeroed extent
// and empty WAL.
func CreateSmartWALVolume(cfg SmartWALVolumeConfig) (*SmartWALVolume, error) {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 4096
	}
	if cfg.WALSlots == 0 {
		cfg.WALSlots = 65536 // 2MB WAL = 65536 × 32 bytes
	}

	// Create extent file
	extentFD, err := os.OpenFile(cfg.ExtentPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: create extent %s: %w", cfg.ExtentPath, err)
	}
	extentSize := int64(cfg.NumBlocks * cfg.BlockSize)
	if err := extentFD.Truncate(extentSize); err != nil {
		extentFD.Close()
		return nil, fmt.Errorf("smartwal: truncate extent to %d: %w", extentSize, err)
	}

	// Create WAL ring buffer
	wal, err := NewSmartWALBuffer(cfg.WALPath, cfg.WALSlots)
	if err != nil {
		extentFD.Close()
		return nil, err
	}

	v := &SmartWALVolume{
		extentFD:  extentFD,
		wal:       wal,
		blockSize: cfg.BlockSize,
		numBlocks: cfg.NumBlocks,
		epoch:     cfg.Epoch,
		dirtyMap:  make(map[uint32]uint64),
	}
	v.nextLSN.Store(1)
	return v, nil
}

// OpenSmartWALVolume opens an existing SmartWAL volume and runs recovery.
func OpenSmartWALVolume(cfg SmartWALVolumeConfig) (*SmartWALVolume, error) {
	if cfg.BlockSize == 0 {
		cfg.BlockSize = 4096
	}
	extentFD, err := os.OpenFile(cfg.ExtentPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: open extent %s: %w", cfg.ExtentPath, err)
	}
	wal, err := OpenSmartWALBuffer(cfg.WALPath)
	if err != nil {
		extentFD.Close()
		return nil, err
	}

	v := &SmartWALVolume{
		extentFD:  extentFD,
		wal:       wal,
		blockSize: cfg.BlockSize,
		numBlocks: cfg.NumBlocks,
		epoch:     cfg.Epoch,
		dirtyMap:  make(map[uint32]uint64),
	}
	v.nextLSN.Store(1)

	if err := v.Recover(); err != nil {
		v.Close()
		return nil, err
	}
	return v, nil
}

// WriteLBA writes a block to the extent and appends a metadata WAL record.
// NOT durable until SyncCache is called.
func (v *SmartWALVolume) WriteLBA(lba uint32, data []byte) error {
	if uint64(lba) >= v.numBlocks {
		return fmt.Errorf("smartwal: LBA %d out of range (max %d)", lba, v.numBlocks-1)
	}
	if uint64(len(data)) != v.blockSize {
		return fmt.Errorf("smartwal: data size %d != block size %d", len(data), v.blockSize)
	}

	// Step 1: write data to extent
	offset := int64(lba) * int64(v.blockSize)
	if _, err := v.extentFD.WriteAt(data, offset); err != nil {
		return fmt.Errorf("smartwal: write extent LBA %d: %w", lba, err)
	}

	// Step 2: compute CRC of what we wrote
	dataCRC := crc32.ChecksumIEEE(data)

	// Step 3: append metadata record to WAL
	lsn := v.nextLSN.Add(1) - 1
	rec := SmartWALRecord{
		LSN:       lsn,
		Epoch:     v.epoch,
		LBA:       lba,
		Flags:     SmartFlagWrite,
		DataCRC32: dataCRC,
	}
	if err := v.wal.AppendRecord(rec); err != nil {
		return err
	}

	// Step 4: mark dirty
	v.dirtyMu.Lock()
	v.dirtyMap[lba] = lsn
	v.dirtyMu.Unlock()
	return nil
}

// ReadLBA reads a block from the extent.
func (v *SmartWALVolume) ReadLBA(lba uint32) ([]byte, error) {
	if uint64(lba) >= v.numBlocks {
		return nil, fmt.Errorf("smartwal: LBA %d out of range", lba)
	}
	data := make([]byte, v.blockSize)
	offset := int64(lba) * int64(v.blockSize)
	if _, err := v.extentFD.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("smartwal: read LBA %d: %w", lba, err)
	}
	return data, nil
}

// TrimLBA zeros a block and appends a trim WAL record.
func (v *SmartWALVolume) TrimLBA(lba uint32) error {
	if uint64(lba) >= v.numBlocks {
		return fmt.Errorf("smartwal: LBA %d out of range", lba)
	}
	zeros := make([]byte, v.blockSize)
	offset := int64(lba) * int64(v.blockSize)
	if _, err := v.extentFD.WriteAt(zeros, offset); err != nil {
		return fmt.Errorf("smartwal: trim LBA %d: %w", lba, err)
	}
	lsn := v.nextLSN.Add(1) - 1
	rec := SmartWALRecord{
		LSN:       lsn,
		Epoch:     v.epoch,
		LBA:       lba,
		Flags:     SmartFlagTrim,
		DataCRC32: crc32.ChecksumIEEE(zeros),
	}
	if err := v.wal.AppendRecord(rec); err != nil {
		return err
	}
	v.dirtyMu.Lock()
	v.dirtyMap[lba] = lsn
	v.dirtyMu.Unlock()
	return nil
}

// SyncCache flushes both extent and WAL to disk. This is the durability
// fence. After SyncCache returns, all preceding writes are durable.
//
// Ordering: extent sync BEFORE WAL sync. This ensures that when a WAL
// record is durable, the extent data it references is also durable.
func (v *SmartWALVolume) SyncCache() error {
	// Step 1: flush extent data to disk
	if err := v.extentFD.Sync(); err != nil {
		return fmt.Errorf("smartwal: sync extent: %w", err)
	}
	// Step 2: flush WAL metadata to disk
	// After this, WAL records reference durable extent data.
	if err := v.wal.Sync(); err != nil {
		return fmt.Errorf("smartwal: sync WAL: %w", err)
	}
	return nil
}

// Recover replays WAL records and verifies extent data integrity.
// Records with CRC mismatches are skipped (torn/unflushed writes).
// Sets nextLSN to one past the last valid recovered record.
func (v *SmartWALVolume) Recover() error {
	records, lastValidLSN, err := v.wal.ScanValidRecords()
	if err != nil {
		return fmt.Errorf("smartwal: recovery scan: %w", err)
	}
	if len(records) == 0 {
		log.Printf("smartwal: recovery: no valid records found")
		return nil
	}

	// Build the last-writer-wins map: for each LBA, keep only the
	// record with the highest LSN.
	lastWrite := make(map[uint32]SmartWALRecord)
	for _, rec := range records {
		if existing, ok := lastWrite[rec.LBA]; !ok || rec.LSN > existing.LSN {
			lastWrite[rec.LBA] = rec
		}
	}

	// Verify each LBA's final record against extent data.
	var recovered, damaged int
	for _, rec := range lastWrite {
		if rec.Flags == SmartFlagTrim {
			recovered++
			continue
		}

		data := make([]byte, v.blockSize)
		offset := int64(rec.LBA) * int64(v.blockSize)
		if _, err := v.extentFD.ReadAt(data, offset); err != nil {
			return fmt.Errorf("smartwal: recovery read LBA %d: %w", rec.LBA, err)
		}
		actualCRC := crc32.ChecksumIEEE(data)
		if actualCRC != rec.DataCRC32 {
			log.Printf("smartwal: recovery CRC mismatch LSN=%d LBA=%d (expected=%08x actual=%08x) — skipping",
				rec.LSN, rec.LBA, rec.DataCRC32, actualCRC)
			damaged++
			continue
		}
		recovered++
		v.dirtyMap[rec.LBA] = rec.LSN
	}

	v.nextLSN.Store(lastValidLSN + 1)
	log.Printf("smartwal: recovery: %d LBAs verified, %d damaged, nextLSN=%d",
		recovered, damaged, lastValidLSN+1)
	return nil
}

// NextLSN returns the next LSN that will be assigned.
func (v *SmartWALVolume) NextLSN() uint64 {
	return v.nextLSN.Load()
}

// Close closes the extent and WAL files.
func (v *SmartWALVolume) Close() error {
	var firstErr error
	if v.wal != nil {
		if err := v.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if v.extentFD != nil {
		if err := v.extentFD.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
