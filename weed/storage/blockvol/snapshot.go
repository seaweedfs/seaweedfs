package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Snapshot delta file constants.
const (
	SnapMagic      = "SWBS"
	SnapVersion    = 1
	SnapHeaderSize = 4096
)

var (
	ErrSnapshotNotFound   = errors.New("blockvol: snapshot not found")
	ErrSnapshotExists     = errors.New("blockvol: snapshot already exists")
	ErrSnapshotBadMagic   = errors.New("blockvol: snapshot bad magic")
	ErrSnapshotBadVersion = errors.New("blockvol: snapshot unsupported version")
	ErrSnapshotBadParent  = errors.New("blockvol: snapshot parent UUID mismatch")
	ErrSnapshotRoleReject = errors.New("blockvol: snapshots only allowed on primary or standalone")
)

// SnapshotBitmap is a dense bitset tracking which blocks have been CoW'd.
// No internal locking -- callers (flusher, CreateSnapshot) serialize access.
type SnapshotBitmap struct {
	data []byte
	bits uint64 // total number of bits (= VolumeSize/BlockSize)
}

// NewSnapshotBitmap creates a zero-initialized bitmap for totalBlocks blocks.
func NewSnapshotBitmap(totalBlocks uint64) *SnapshotBitmap {
	byteLen := (totalBlocks + 7) / 8
	return &SnapshotBitmap{
		data: make([]byte, byteLen),
		bits: totalBlocks,
	}
}

// Get returns true if the bit at position lba is set.
func (b *SnapshotBitmap) Get(lba uint64) bool {
	if lba >= b.bits {
		return false
	}
	return b.data[lba/8]&(1<<(lba%8)) != 0
}

// Set sets the bit at position lba.
func (b *SnapshotBitmap) Set(lba uint64) {
	if lba >= b.bits {
		return
	}
	b.data[lba/8] |= 1 << (lba % 8)
}

// ByteSize returns the number of bytes in the bitmap.
func (b *SnapshotBitmap) ByteSize() int {
	return len(b.data)
}

// WriteTo writes the bitmap data to w at the given offset.
func (b *SnapshotBitmap) WriteTo(w io.WriterAt, offset int64) error {
	_, err := w.WriteAt(b.data, offset)
	return err
}

// ReadFrom reads bitmap data from r at the given offset.
func (b *SnapshotBitmap) ReadFrom(r io.ReaderAt, offset int64) error {
	_, err := r.ReadAt(b.data, offset)
	return err
}

// CountSet returns the number of set bits (CoW'd blocks).
func (b *SnapshotBitmap) CountSet() uint64 {
	var count uint64
	for _, v := range b.data {
		count += uint64(popcount8(v))
	}
	return count
}

// popcount8 returns the number of set bits in a byte.
func popcount8(x byte) int {
	x = x - ((x >> 1) & 0x55)
	x = (x & 0x33) + ((x >> 2) & 0x33)
	return int((x + (x >> 4)) & 0x0F)
}

// SnapshotHeader is the on-disk header for a snapshot delta file.
type SnapshotHeader struct {
	Magic      [4]byte
	Version    uint16
	SnapshotID uint32
	BaseLSN    uint64
	VolumeSize uint64
	BlockSize  uint32
	BitmapSize uint64
	DataOffset uint64 // = SnapHeaderSize + BitmapSize, aligned to BlockSize
	CreatedAt  uint64
	ParentUUID [16]byte
}

// WriteTo serializes the header as a SnapHeaderSize-byte block to w.
func (h *SnapshotHeader) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, SnapHeaderSize)
	endian := binary.LittleEndian
	off := 0
	off += copy(buf[off:], h.Magic[:])
	endian.PutUint16(buf[off:], h.Version)
	off += 2
	endian.PutUint32(buf[off:], h.SnapshotID)
	off += 4
	endian.PutUint64(buf[off:], h.BaseLSN)
	off += 8
	endian.PutUint64(buf[off:], h.VolumeSize)
	off += 8
	endian.PutUint32(buf[off:], h.BlockSize)
	off += 4
	endian.PutUint64(buf[off:], h.BitmapSize)
	off += 8
	endian.PutUint64(buf[off:], h.DataOffset)
	off += 8
	endian.PutUint64(buf[off:], h.CreatedAt)
	off += 8
	copy(buf[off:], h.ParentUUID[:])
	n, err := w.Write(buf)
	return int64(n), err
}

// ReadSnapshotHeader reads a SnapshotHeader from r.
func ReadSnapshotHeader(r io.Reader) (*SnapshotHeader, error) {
	buf := make([]byte, SnapHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("blockvol: read snapshot header: %w", err)
	}

	endian := binary.LittleEndian
	h := &SnapshotHeader{}
	off := 0
	copy(h.Magic[:], buf[off:off+4])
	off += 4
	if string(h.Magic[:]) != SnapMagic {
		return nil, ErrSnapshotBadMagic
	}
	h.Version = endian.Uint16(buf[off:])
	off += 2
	if h.Version != SnapVersion {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrSnapshotBadVersion, h.Version, SnapVersion)
	}
	h.SnapshotID = endian.Uint32(buf[off:])
	off += 4
	h.BaseLSN = endian.Uint64(buf[off:])
	off += 8
	h.VolumeSize = endian.Uint64(buf[off:])
	off += 8
	h.BlockSize = endian.Uint32(buf[off:])
	off += 4
	h.BitmapSize = endian.Uint64(buf[off:])
	off += 8
	h.DataOffset = endian.Uint64(buf[off:])
	off += 8
	h.CreatedAt = endian.Uint64(buf[off:])
	off += 8
	copy(h.ParentUUID[:], buf[off:off+16])
	return h, nil
}

// activeSnapshot represents an open snapshot delta file with its in-memory bitmap.
type activeSnapshot struct {
	id         uint32
	fd         *os.File
	header     SnapshotHeader
	bitmap     *SnapshotBitmap
	dataOffset uint64
	dirty      bool // bitmap changed since last persist
}

// Close closes the delta file.
func (s *activeSnapshot) Close() error {
	return s.fd.Close()
}

// deltaFilePath returns the path for a snapshot delta file.
func deltaFilePath(volPath string, id uint32) string {
	return fmt.Sprintf("%s.snap.%d", volPath, id)
}

// createDeltaFile creates a new snapshot delta file and returns an activeSnapshot.
func createDeltaFile(path string, id uint32, vol *BlockVol, baseLSN uint64) (*activeSnapshot, error) {
	totalBlocks := vol.super.VolumeSize / uint64(vol.super.BlockSize)
	bitmap := NewSnapshotBitmap(totalBlocks)
	bitmapSize := uint64(bitmap.ByteSize())

	// Align DataOffset to BlockSize.
	dataOffset := uint64(SnapHeaderSize) + bitmapSize
	rem := dataOffset % uint64(vol.super.BlockSize)
	if rem != 0 {
		dataOffset += uint64(vol.super.BlockSize) - rem
	}

	hdr := SnapshotHeader{
		Version:    SnapVersion,
		SnapshotID: id,
		BaseLSN:    baseLSN,
		VolumeSize: vol.super.VolumeSize,
		BlockSize:  vol.super.BlockSize,
		BitmapSize: bitmapSize,
		DataOffset: dataOffset,
		CreatedAt:  uint64(time.Now().Unix()),
		ParentUUID: vol.super.UUID,
	}
	copy(hdr.Magic[:], SnapMagic)

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("blockvol: create delta file: %w", err)
	}

	// Truncate to full size (sparse file -- only header+bitmap consume disk).
	totalSize := int64(dataOffset + vol.super.VolumeSize)
	if err := fd.Truncate(totalSize); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: truncate delta: %w", err)
	}

	// Write header.
	if _, err := hdr.WriteTo(fd); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: write snapshot header: %w", err)
	}

	// Write zero bitmap.
	if err := bitmap.WriteTo(fd, SnapHeaderSize); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: write snapshot bitmap: %w", err)
	}

	// Fsync delta file.
	if err := fd.Sync(); err != nil {
		fd.Close()
		os.Remove(path)
		return nil, fmt.Errorf("blockvol: sync delta: %w", err)
	}

	return &activeSnapshot{
		id:         id,
		fd:         fd,
		header:     hdr,
		bitmap:     bitmap,
		dataOffset: dataOffset,
	}, nil
}

// openDeltaFile opens an existing snapshot delta file, reads its header and bitmap.
func openDeltaFile(path string) (*activeSnapshot, error) {
	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("blockvol: open delta file: %w", err)
	}

	hdr, err := ReadSnapshotHeader(fd)
	if err != nil {
		fd.Close()
		return nil, err
	}

	totalBlocks := hdr.VolumeSize / uint64(hdr.BlockSize)
	bitmap := NewSnapshotBitmap(totalBlocks)
	if err := bitmap.ReadFrom(fd, SnapHeaderSize); err != nil {
		fd.Close()
		return nil, fmt.Errorf("blockvol: read snapshot bitmap: %w", err)
	}

	return &activeSnapshot{
		id:         hdr.SnapshotID,
		fd:         fd,
		header:     *hdr,
		bitmap:     bitmap,
		dataOffset: hdr.DataOffset,
	}, nil
}

// SnapshotInfo contains read-only snapshot metadata for listing.
type SnapshotInfo struct {
	ID        uint32
	BaseLSN   uint64
	CreatedAt time.Time
	CoWBlocks uint64 // number of CoW'd blocks (bitmap.CountSet())
}
