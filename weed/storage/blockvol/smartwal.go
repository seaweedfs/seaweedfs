package blockvol

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// SmartWALBuffer is a fixed-size slot-based ring buffer for metadata-only
// WAL records. Each slot is exactly SmartWALRecordSize (32) bytes.
//
// The ring buffer supports concurrent appends (via head CAS) and
// sequential sync (fdatasync flushes all pending records).
//
// Capacity planning: a 64MB buffer holds 2M records (vs ~16K for
// current 4KB-inline WAL). WAL pressure is effectively eliminated.
type SmartWALBuffer struct {
	fd       *os.File
	capacity uint64 // number of slots
	size     uint64 // capacity * SmartWALRecordSize

	head   atomic.Uint64 // next write slot (monotonic, wraps via mod)
	synced atomic.Uint64 // last synced head position

	mu sync.Mutex // protects fd writes (pwrite is not atomic across goroutines)
}

// NewSmartWALBuffer creates a new ring buffer backed by the given file.
// The file is truncated/extended to the specified number of slots and
// zero-filled. Zero bytes decode as invalid records (no magic byte).
func NewSmartWALBuffer(path string, slots uint64) (*SmartWALBuffer, error) {
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: open %s: %w", path, err)
	}
	size := slots * SmartWALRecordSize
	if err := fd.Truncate(int64(size)); err != nil {
		fd.Close()
		return nil, fmt.Errorf("smartwal: truncate %s to %d: %w", path, size, err)
	}
	return &SmartWALBuffer{
		fd:       fd,
		capacity: slots,
		size:     size,
	}, nil
}

// OpenSmartWALBuffer opens an existing ring buffer file for recovery.
// Does not truncate or zero-fill.
func OpenSmartWALBuffer(path string) (*SmartWALBuffer, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("smartwal: open %s: %w", path, err)
	}
	info, err := fd.Stat()
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("smartwal: stat %s: %w", path, err)
	}
	size := uint64(info.Size())
	if size < SmartWALRecordSize || size%SmartWALRecordSize != 0 {
		fd.Close()
		return nil, fmt.Errorf("smartwal: invalid size %d (must be multiple of %d)", size, SmartWALRecordSize)
	}
	return &SmartWALBuffer{
		fd:       fd,
		capacity: size / SmartWALRecordSize,
		size:     size,
	}, nil
}

// AppendRecord writes a record to the next slot in the ring buffer.
// The write is a pwrite to the correct file offset — no seeking.
// NOT durable until Sync() is called.
func (w *SmartWALBuffer) AppendRecord(rec SmartWALRecord) error {
	slot := w.head.Add(1) - 1
	offset := (slot % w.capacity) * SmartWALRecordSize
	encoded := EncodeSmartWALRecord(rec)

	w.mu.Lock()
	_, err := w.fd.WriteAt(encoded[:], int64(offset))
	w.mu.Unlock()
	if err != nil {
		return fmt.Errorf("smartwal: write slot %d: %w", slot, err)
	}
	return nil
}

// Sync flushes all pending records to disk. After Sync returns, all
// records appended before this call are durable.
func (w *SmartWALBuffer) Sync() error {
	if err := w.fd.Sync(); err != nil {
		return fmt.Errorf("smartwal: sync: %w", err)
	}
	w.synced.Store(w.head.Load())
	return nil
}

// SyncedPosition returns the head position at the last successful Sync.
func (w *SmartWALBuffer) SyncedPosition() uint64 {
	return w.synced.Load()
}

// HeadPosition returns the current head (next write slot).
func (w *SmartWALBuffer) HeadPosition() uint64 {
	return w.head.Load()
}

// Capacity returns the number of slots in the ring buffer.
func (w *SmartWALBuffer) Capacity() uint64 {
	return w.capacity
}

// ScanValidRecords reads all slots in the ring buffer and returns valid
// records in slot order. Stops at the first invalid/zeroed slot after
// finding at least one valid record in a forward scan.
//
// For recovery: the caller should use the returned records to verify
// extent data via CRC.
//
// Returns (records, lastValidLSN, error).
func (w *SmartWALBuffer) ScanValidRecords() ([]SmartWALRecord, uint64, error) {
	buf := make([]byte, w.size)
	if _, err := w.fd.ReadAt(buf, 0); err != nil {
		return nil, 0, fmt.Errorf("smartwal: read all: %w", err)
	}

	var valid []smartSlotRecord
	for i := uint64(0); i < w.capacity; i++ {
		offset := i * SmartWALRecordSize
		rec, ok := DecodeSmartWALRecord(buf[offset : offset+SmartWALRecordSize])
		if !ok {
			continue
		}
		valid = append(valid, smartSlotRecord{slot: i, rec: rec}) //nolint:govet
	}

	if len(valid) == 0 {
		return nil, 0, nil
	}

	// Sort by LSN to get correct ordering (ring may have wrapped).
	// Since LSN is monotonic, sorting by LSN gives temporal order.
	sortByLSN(valid)

	records := make([]SmartWALRecord, len(valid))
	var maxLSN uint64
	for i, sr := range valid {
		records[i] = sr.rec
		if sr.rec.LSN > maxLSN {
			maxLSN = sr.rec.LSN
		}
	}

	// Set head to after the last valid record for post-recovery appends.
	w.head.Store(maxLSN)
	w.synced.Store(maxLSN)

	return records, maxLSN, nil
}

type smartSlotRecord struct {
	slot uint64
	rec  SmartWALRecord
}

// sortByLSN sorts slot records by LSN (ascending).
func sortByLSN(records []smartSlotRecord) {
	// Simple insertion sort — recovery is not performance-critical.
	for i := 1; i < len(records); i++ {
		for j := i; j > 0 && records[j].rec.LSN < records[j-1].rec.LSN; j-- {
			records[j], records[j-1] = records[j-1], records[j]
		}
	}
}

// Close closes the underlying file.
func (w *SmartWALBuffer) Close() error {
	if w.fd == nil {
		return nil
	}
	return w.fd.Close()
}
