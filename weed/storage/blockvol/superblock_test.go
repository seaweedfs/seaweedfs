package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestSuperblock(t *testing.T) {
	tests := []struct {
		name    string
		run     func(t *testing.T)
	}{
		{name: "superblock_roundtrip", run: testSuperblockRoundtrip},
		{name: "superblock_magic_check", run: testSuperblockMagicCheck},
		{name: "superblock_version_check", run: testSuperblockVersionCheck},
		{name: "superblock_zero_vol_size", run: testSuperblockZeroVolSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testSuperblockRoundtrip(t *testing.T) {
	sb, err := NewSuperblock(100*1024*1024*1024, CreateOptions{
		ExtentSize:  64 * 1024,
		BlockSize:   4096,
		WALSize:     64 * 1024 * 1024,
		Replication: "010",
	})
	if err != nil {
		t.Fatalf("NewSuperblock: %v", err)
	}
	sb.CreatedAt = 1700000000
	sb.SnapshotCount = 3
	sb.WALHead = 8192
	sb.WALCheckpointLSN = 42

	var buf bytes.Buffer
	n, err := sb.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != SuperblockSize {
		t.Fatalf("WriteTo wrote %d bytes, want %d", n, SuperblockSize)
	}

	got, err := ReadSuperblock(&buf)
	if err != nil {
		t.Fatalf("ReadSuperblock: %v", err)
	}

	if string(got.Magic[:]) != MagicSWBK {
		t.Errorf("Magic = %q, want %q", got.Magic, MagicSWBK)
	}
	if got.Version != CurrentVersion {
		t.Errorf("Version = %d, want %d", got.Version, CurrentVersion)
	}
	if got.UUID != sb.UUID {
		t.Errorf("UUID mismatch")
	}
	if got.VolumeSize != sb.VolumeSize {
		t.Errorf("VolumeSize = %d, want %d", got.VolumeSize, sb.VolumeSize)
	}
	if got.ExtentSize != sb.ExtentSize {
		t.Errorf("ExtentSize = %d, want %d", got.ExtentSize, sb.ExtentSize)
	}
	if got.BlockSize != sb.BlockSize {
		t.Errorf("BlockSize = %d, want %d", got.BlockSize, sb.BlockSize)
	}
	if got.WALOffset != sb.WALOffset {
		t.Errorf("WALOffset = %d, want %d", got.WALOffset, sb.WALOffset)
	}
	if got.WALSize != sb.WALSize {
		t.Errorf("WALSize = %d, want %d", got.WALSize, sb.WALSize)
	}
	if got.WALHead != sb.WALHead {
		t.Errorf("WALHead = %d, want %d", got.WALHead, sb.WALHead)
	}
	if got.WALCheckpointLSN != sb.WALCheckpointLSN {
		t.Errorf("WALCheckpointLSN = %d, want %d", got.WALCheckpointLSN, sb.WALCheckpointLSN)
	}
	if got.Replication != sb.Replication {
		t.Errorf("Replication = %q, want %q", got.Replication, sb.Replication)
	}
	if got.CreatedAt != sb.CreatedAt {
		t.Errorf("CreatedAt = %d, want %d", got.CreatedAt, sb.CreatedAt)
	}
	if got.SnapshotCount != sb.SnapshotCount {
		t.Errorf("SnapshotCount = %d, want %d", got.SnapshotCount, sb.SnapshotCount)
	}
}

func testSuperblockMagicCheck(t *testing.T) {
	// Write a valid superblock, then corrupt the magic bytes.
	sb, _ := NewSuperblock(1*1024*1024*1024, CreateOptions{})
	var buf bytes.Buffer
	sb.WriteTo(&buf)

	data := buf.Bytes()
	copy(data[0:4], "XXXX") // corrupt magic

	_, err := ReadSuperblock(bytes.NewReader(data))
	if !errors.Is(err, ErrNotBlockVol) {
		t.Errorf("expected ErrNotBlockVol, got %v", err)
	}
}

func testSuperblockVersionCheck(t *testing.T) {
	sb, _ := NewSuperblock(1*1024*1024*1024, CreateOptions{})
	var buf bytes.Buffer
	sb.WriteTo(&buf)

	data := buf.Bytes()
	// Version is at offset 4, uint16 little-endian.
	binary.LittleEndian.PutUint16(data[4:], 99)

	_, err := ReadSuperblock(bytes.NewReader(data))
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("expected ErrUnsupportedVersion, got %v", err)
	}
}

func testSuperblockZeroVolSize(t *testing.T) {
	_, err := NewSuperblock(0, CreateOptions{})
	if !errors.Is(err, ErrInvalidVolumeSize) {
		t.Errorf("NewSuperblock(0): expected ErrInvalidVolumeSize, got %v", err)
	}

	// Also test reading a superblock with zero volume size.
	sb, _ := NewSuperblock(1*1024*1024*1024, CreateOptions{})
	var buf bytes.Buffer
	sb.WriteTo(&buf)

	data := buf.Bytes()
	// VolumeSize is at offset 4+2+2+16 = 24, uint64 little-endian.
	binary.LittleEndian.PutUint64(data[24:], 0)

	_, err = ReadSuperblock(bytes.NewReader(data))
	if !errors.Is(err, ErrInvalidVolumeSize) {
		t.Errorf("ReadSuperblock(vol_size=0): expected ErrInvalidVolumeSize, got %v", err)
	}
}
