package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

var (
	ErrWALFull = errors.New("blockvol: WAL region full")
)

// WALWriter appends entries to the circular WAL region of a blockvol file.
//
// It uses logical (monotonically increasing) head and tail counters to track
// used space. Physical position = logical % walSize. This eliminates the
// classic circular buffer ambiguity where head==tail could mean empty or full.
// Used space = logicalHead - logicalTail. Free space = walSize - used.
type WALWriter struct {
	mu          sync.Mutex
	fd          *os.File
	walOffset   uint64 // absolute file offset where WAL region starts
	walSize     uint64 // size of the WAL region in bytes
	logicalHead uint64 // monotonically increasing write position
	logicalTail uint64 // monotonically increasing flush position
}

// NewWALWriter creates a WAL writer for the given file.
// head and tail are physical positions relative to WAL region start.
// For a fresh WAL, both are 0.
func NewWALWriter(fd *os.File, walOffset, walSize, head, tail uint64) *WALWriter {
	return &WALWriter{
		fd:          fd,
		walOffset:   walOffset,
		walSize:     walSize,
		logicalHead: head, // on fresh WAL, physical == logical (both start at 0)
		logicalTail: tail,
	}
}

// physicalPos converts a logical position to a physical WAL offset.
func (w *WALWriter) physicalPos(logical uint64) uint64 {
	return logical % w.walSize
}

// used returns the number of bytes occupied in the WAL.
func (w *WALWriter) used() uint64 {
	return w.logicalHead - w.logicalTail
}

// Append writes a serialized WAL entry to the circular WAL region.
// Returns the physical WAL-relative offset where the entry was written.
// If the entry doesn't fit in the remaining space before the region end,
// a padding entry is written and the real entry starts at physical offset 0.
func (w *WALWriter) Append(entry *WALEntry) (walRelOffset uint64, err error) {
	buf, err := entry.Encode()
	if err != nil {
		return 0, fmt.Errorf("WALWriter.Append: encode: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	entryLen := uint64(len(buf))
	if entryLen > w.walSize {
		return 0, fmt.Errorf("%w: entry size %d exceeds WAL size %d", ErrWALFull, entryLen, w.walSize)
	}

	physHead := w.physicalPos(w.logicalHead)
	remaining := w.walSize - physHead

	if remaining < entryLen {
		// Not enough room at end of region -- write padding and wrap.
		// Padding consumes 'remaining' bytes logically.
		if w.used()+remaining+entryLen > w.walSize {
			return 0, ErrWALFull
		}
		if err := w.writePadding(remaining, physHead); err != nil {
			return 0, fmt.Errorf("WALWriter.Append: padding: %w", err)
		}
		w.logicalHead += remaining
		physHead = 0
	}

	// Check if there's enough free space for the entry.
	if w.used()+entryLen > w.walSize {
		return 0, ErrWALFull
	}

	absOffset := int64(w.walOffset + physHead)
	if _, err := w.fd.WriteAt(buf, absOffset); err != nil {
		return 0, fmt.Errorf("WALWriter.Append: pwrite at offset %d: %w", absOffset, err)
	}

	writeOffset := physHead
	w.logicalHead += entryLen
	return writeOffset, nil
}

// writePadding writes a padding entry at the given physical position.
func (w *WALWriter) writePadding(size uint64, physPos uint64) error {
	if size < walEntryHeaderSize {
		// Too small for a proper entry header -- zero it out.
		buf := make([]byte, size)
		absOffset := int64(w.walOffset + physPos)
		_, err := w.fd.WriteAt(buf, absOffset)
		return err
	}

	buf := make([]byte, size)
	le := binary.LittleEndian
	off := 0
	le.PutUint64(buf[off:], 0) // LSN=0
	off += 8
	le.PutUint64(buf[off:], 0) // Epoch=0
	off += 8
	buf[off] = EntryTypePadding
	off++
	buf[off] = 0 // Flags
	off++
	le.PutUint64(buf[off:], 0) // LBA=0
	off += 8
	paddingDataLen := uint32(size) - uint32(walEntryHeaderSize)
	le.PutUint32(buf[off:], paddingDataLen)
	off += 4
	dataEnd := off + int(paddingDataLen)

	crc := crc32.ChecksumIEEE(buf[:dataEnd])
	le.PutUint32(buf[dataEnd:], crc)
	le.PutUint32(buf[dataEnd+4:], uint32(size))

	absOffset := int64(w.walOffset + physPos)
	_, err := w.fd.WriteAt(buf, absOffset)
	return err
}

// AdvanceTail moves the tail forward, freeing WAL space.
// Called by the flusher after entries have been written to the extent region.
// newTail is a physical position; it is converted to a logical advance.
func (w *WALWriter) AdvanceTail(newTail uint64) {
	w.mu.Lock()
	physTail := w.physicalPos(w.logicalTail)
	var advance uint64
	if newTail >= physTail {
		advance = newTail - physTail
	} else {
		// Tail wrapped around.
		advance = w.walSize - physTail + newTail
	}
	w.logicalTail += advance
	w.mu.Unlock()
}

// Head returns the current physical head position (relative to WAL start).
func (w *WALWriter) Head() uint64 {
	w.mu.Lock()
	h := w.physicalPos(w.logicalHead)
	w.mu.Unlock()
	return h
}

// Tail returns the current physical tail position (relative to WAL start).
func (w *WALWriter) Tail() uint64 {
	w.mu.Lock()
	t := w.physicalPos(w.logicalTail)
	w.mu.Unlock()
	return t
}

// LogicalHead returns the logical (monotonically increasing) head position.
func (w *WALWriter) LogicalHead() uint64 {
	w.mu.Lock()
	h := w.logicalHead
	w.mu.Unlock()
	return h
}

// LogicalTail returns the logical (monotonically increasing) tail position.
func (w *WALWriter) LogicalTail() uint64 {
	w.mu.Lock()
	t := w.logicalTail
	w.mu.Unlock()
	return t
}

// UsedFraction returns the fraction of WAL space currently in use (0.0 to 1.0).
func (w *WALWriter) UsedFraction() float64 {
	w.mu.Lock()
	u := w.used()
	s := w.walSize
	w.mu.Unlock()
	if s == 0 {
		return 0
	}
	return float64(u) / float64(s)
}

// Sync fsyncs the underlying file descriptor.
func (w *WALWriter) Sync() error {
	return w.fd.Sync()
}
