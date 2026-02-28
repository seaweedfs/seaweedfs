package blockvol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const (
	SuperblockSize = 4096
	MagicSWBK      = "SWBK"
	CurrentVersion = 1
)

var (
	ErrNotBlockVol        = errors.New("blockvol: not a blockvol file (bad magic)")
	ErrUnsupportedVersion = errors.New("blockvol: unsupported version")
	ErrInvalidVolumeSize  = errors.New("blockvol: volume size must be > 0")
)

// Superblock is the 4KB header at offset 0 of a blockvol file.
// It identifies the file format, stores volume geometry, and tracks WAL state.
type Superblock struct {
	Magic            [4]byte
	Version          uint16
	Flags            uint16
	UUID             [16]byte
	VolumeSize       uint64 // logical size in bytes
	ExtentSize       uint32 // default 64KB
	BlockSize        uint32 // default 4KB (min I/O unit)
	WALOffset        uint64 // byte offset where WAL region starts
	WALSize          uint64 // WAL region size in bytes
	WALHead          uint64 // logical WAL write position (monotonically increasing)
	WALTail          uint64 // logical WAL flush position (monotonically increasing)
	WALCheckpointLSN uint64 // last LSN flushed to extent region
	Replication      [4]byte
	CreatedAt        uint64 // unix timestamp
	SnapshotCount    uint32
}

// superblockOnDisk is the fixed-size on-disk layout (binary.Write/Read target).
// Must be <= SuperblockSize bytes. Remaining space is zero-padded.
type superblockOnDisk struct {
	Magic            [4]byte
	Version          uint16
	Flags            uint16
	UUID             [16]byte
	VolumeSize       uint64
	ExtentSize       uint32
	BlockSize        uint32
	WALOffset        uint64
	WALSize          uint64
	WALHead          uint64
	WALTail          uint64
	WALCheckpointLSN uint64
	Replication      [4]byte
	CreatedAt        uint64
	SnapshotCount    uint32
}

// NewSuperblock creates a superblock with defaults and a fresh UUID.
func NewSuperblock(volumeSize uint64, opts CreateOptions) (Superblock, error) {
	if volumeSize == 0 {
		return Superblock{}, ErrInvalidVolumeSize
	}

	extentSize := opts.ExtentSize
	if extentSize == 0 {
		extentSize = 64 * 1024 // 64KB
	}
	blockSize := opts.BlockSize
	if blockSize == 0 {
		blockSize = 4096 // 4KB
	}
	walSize := opts.WALSize
	if walSize == 0 {
		walSize = 64 * 1024 * 1024 // 64MB
	}

	var repl [4]byte
	if opts.Replication != "" {
		copy(repl[:], opts.Replication)
	} else {
		copy(repl[:], "000")
	}

	id := uuid.New()

	sb := Superblock{
		Version:    CurrentVersion,
		VolumeSize: volumeSize,
		ExtentSize: extentSize,
		BlockSize:  blockSize,
		WALOffset:  SuperblockSize,
		WALSize:    walSize,
	}
	copy(sb.Magic[:], MagicSWBK)
	sb.UUID = id
	sb.Replication = repl

	return sb, nil
}

// WriteTo serializes the superblock to w as a 4096-byte block.
func (sb *Superblock) WriteTo(w io.Writer) (int64, error) {
	buf := make([]byte, SuperblockSize)

	d := superblockOnDisk{
		Magic:            sb.Magic,
		Version:          sb.Version,
		Flags:            sb.Flags,
		UUID:             sb.UUID,
		VolumeSize:       sb.VolumeSize,
		ExtentSize:       sb.ExtentSize,
		BlockSize:        sb.BlockSize,
		WALOffset:        sb.WALOffset,
		WALSize:          sb.WALSize,
		WALHead:          sb.WALHead,
		WALTail:          sb.WALTail,
		WALCheckpointLSN: sb.WALCheckpointLSN,
		Replication:      sb.Replication,
		CreatedAt:        sb.CreatedAt,
		SnapshotCount:    sb.SnapshotCount,
	}

	// Encode into beginning of buf; rest stays zero (padding).
	endian := binary.LittleEndian
	off := 0
	off += copy(buf[off:], d.Magic[:])
	endian.PutUint16(buf[off:], d.Version)
	off += 2
	endian.PutUint16(buf[off:], d.Flags)
	off += 2
	off += copy(buf[off:], d.UUID[:])
	endian.PutUint64(buf[off:], d.VolumeSize)
	off += 8
	endian.PutUint32(buf[off:], d.ExtentSize)
	off += 4
	endian.PutUint32(buf[off:], d.BlockSize)
	off += 4
	endian.PutUint64(buf[off:], d.WALOffset)
	off += 8
	endian.PutUint64(buf[off:], d.WALSize)
	off += 8
	endian.PutUint64(buf[off:], d.WALHead)
	off += 8
	endian.PutUint64(buf[off:], d.WALTail)
	off += 8
	endian.PutUint64(buf[off:], d.WALCheckpointLSN)
	off += 8
	off += copy(buf[off:], d.Replication[:])
	endian.PutUint64(buf[off:], d.CreatedAt)
	off += 8
	endian.PutUint32(buf[off:], d.SnapshotCount)

	n, err := w.Write(buf)
	return int64(n), err
}

// ReadSuperblock reads and validates a superblock from r.
func ReadSuperblock(r io.Reader) (Superblock, error) {
	buf := make([]byte, SuperblockSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Superblock{}, fmt.Errorf("blockvol: read superblock: %w", err)
	}

	endian := binary.LittleEndian
	var sb Superblock
	off := 0
	copy(sb.Magic[:], buf[off:off+4])
	off += 4

	if string(sb.Magic[:]) != MagicSWBK {
		return Superblock{}, ErrNotBlockVol
	}

	sb.Version = endian.Uint16(buf[off:])
	off += 2
	if sb.Version != CurrentVersion {
		return Superblock{}, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, sb.Version, CurrentVersion)
	}

	sb.Flags = endian.Uint16(buf[off:])
	off += 2
	copy(sb.UUID[:], buf[off:off+16])
	off += 16
	sb.VolumeSize = endian.Uint64(buf[off:])
	off += 8

	if sb.VolumeSize == 0 {
		return Superblock{}, ErrInvalidVolumeSize
	}

	sb.ExtentSize = endian.Uint32(buf[off:])
	off += 4
	sb.BlockSize = endian.Uint32(buf[off:])
	off += 4
	sb.WALOffset = endian.Uint64(buf[off:])
	off += 8
	sb.WALSize = endian.Uint64(buf[off:])
	off += 8
	sb.WALHead = endian.Uint64(buf[off:])
	off += 8
	sb.WALTail = endian.Uint64(buf[off:])
	off += 8
	sb.WALCheckpointLSN = endian.Uint64(buf[off:])
	off += 8
	copy(sb.Replication[:], buf[off:off+4])
	off += 4
	sb.CreatedAt = endian.Uint64(buf[off:])
	off += 8
	sb.SnapshotCount = endian.Uint32(buf[off:])

	return sb, nil
}

var ErrInvalidSuperblock = errors.New("blockvol: invalid superblock")

// Validate checks that the superblock fields are internally consistent.
func (sb *Superblock) Validate() error {
	if string(sb.Magic[:]) != MagicSWBK {
		return ErrNotBlockVol
	}
	if sb.Version != CurrentVersion {
		return fmt.Errorf("%w: got %d", ErrUnsupportedVersion, sb.Version)
	}
	if sb.VolumeSize == 0 {
		return ErrInvalidVolumeSize
	}
	if sb.BlockSize == 0 {
		return fmt.Errorf("%w: BlockSize is 0", ErrInvalidSuperblock)
	}
	if sb.ExtentSize == 0 {
		return fmt.Errorf("%w: ExtentSize is 0", ErrInvalidSuperblock)
	}
	if sb.WALSize == 0 {
		return fmt.Errorf("%w: WALSize is 0", ErrInvalidSuperblock)
	}
	if sb.WALOffset != SuperblockSize {
		return fmt.Errorf("%w: WALOffset=%d, expected %d", ErrInvalidSuperblock, sb.WALOffset, SuperblockSize)
	}
	if sb.VolumeSize%uint64(sb.BlockSize) != 0 {
		return fmt.Errorf("%w: VolumeSize %d not aligned to BlockSize %d", ErrInvalidSuperblock, sb.VolumeSize, sb.BlockSize)
	}
	return nil
}
