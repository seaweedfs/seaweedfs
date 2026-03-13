package blockvol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Snapshot artifact format constants.
const (
	SnapshotArtifactFormatV1 = 1
	ArtifactLayoutSingleFile = "single-file"
	ExportToolVersion        = "sw-block-cp11a4"
	exportChunkBlocks        = 256 // read 256 blocks per chunk (1MB at 4KB block size)
	exportTempSnapID         = uint32(0xFFFFFFFE)
)

// SnapshotArtifactManifest describes a snapshot export artifact.
type SnapshotArtifactManifest struct {
	FormatVersion     int    `json:"format_version"`
	SourceVolume      string `json:"source_volume"`
	SourceSizeBytes   uint64 `json:"source_size_bytes"`
	SourceBlockSize   uint32 `json:"source_block_size"`
	StorageProfile    string `json:"storage_profile"`
	CreatedAt         string `json:"created_at"`
	ArtifactLayout    string `json:"artifact_layout"`
	DataObjectKey     string `json:"data_object_key"`
	DataSizeBytes     uint64 `json:"data_size_bytes"`
	SHA256            string `json:"sha256"`
	Compression       string `json:"compression"`
	ExportToolVersion string `json:"export_tool_version"`
}

var (
	ErrUnsupportedArtifactVersion = errors.New("blockvol: unsupported artifact format version")
	ErrUnsupportedArtifactLayout  = errors.New("blockvol: unsupported artifact layout")
	ErrUnsupportedProfileExport   = errors.New("blockvol: only single profile supported for export/import")
	ErrManifestMissingField       = errors.New("blockvol: manifest missing required field")
	ErrChecksumMismatch           = errors.New("blockvol: data checksum mismatch")
	ErrImportSizeMismatch         = errors.New("blockvol: target volume size does not match manifest")
	ErrImportBlockSizeMismatch    = errors.New("blockvol: target block size does not match manifest")
	ErrImportTargetNotEmpty       = errors.New("blockvol: target volume is not empty (use AllowOverwrite)")
	ErrImportDataShort            = errors.New("blockvol: import data shorter than manifest declares")
)

// MarshalManifest encodes a manifest as indented JSON.
func MarshalManifest(m *SnapshotArtifactManifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// UnmarshalManifest decodes and validates a manifest from JSON.
func UnmarshalManifest(data []byte) (*SnapshotArtifactManifest, error) {
	var m SnapshotArtifactManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("blockvol: unmarshal manifest: %w", err)
	}
	if err := ValidateManifest(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ValidateManifest checks that all required fields are present and supported.
func ValidateManifest(m *SnapshotArtifactManifest) error {
	if m.FormatVersion != SnapshotArtifactFormatV1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedArtifactVersion, m.FormatVersion)
	}
	if m.ArtifactLayout != ArtifactLayoutSingleFile {
		return fmt.Errorf("%w: %s", ErrUnsupportedArtifactLayout, m.ArtifactLayout)
	}
	if m.StorageProfile != "single" {
		return fmt.Errorf("%w: %s", ErrUnsupportedProfileExport, m.StorageProfile)
	}
	if m.SourceSizeBytes == 0 {
		return fmt.Errorf("%w: source_size_bytes", ErrManifestMissingField)
	}
	if m.SourceBlockSize == 0 {
		return fmt.Errorf("%w: source_block_size", ErrManifestMissingField)
	}
	if m.SHA256 == "" {
		return fmt.Errorf("%w: sha256", ErrManifestMissingField)
	}
	if m.DataSizeBytes == 0 {
		return fmt.Errorf("%w: data_size_bytes", ErrManifestMissingField)
	}
	return nil
}

// ExportOptions configures a snapshot export.
type ExportOptions struct {
	DataObjectKey string // S3 object key for the data object (stored in manifest)
	SnapshotID    uint32 // if > 0, export from existing snapshot; if 0, create+delete temp
}

// ExportSnapshot exports a crash-consistent snapshot of the volume to w.
// If opts.SnapshotID is 0, a temporary snapshot is created and deleted after export.
// If opts.SnapshotID is > 0, the existing snapshot is used (not deleted).
// The full logical volume image is streamed to w with SHA-256 computed inline.
func (v *BlockVol) ExportSnapshot(ctx context.Context, w io.Writer, opts ExportOptions) (*SnapshotArtifactManifest, error) {
	if v.Profile() != ProfileSingle {
		return nil, ErrUnsupportedProfileExport
	}

	info := v.Info()
	snapID := opts.SnapshotID
	deleteSnap := false

	if snapID == 0 {
		snapID = exportTempSnapID
		if err := v.CreateSnapshot(snapID); err != nil {
			return nil, fmt.Errorf("blockvol: export create temp snapshot: %w", err)
		}
		deleteSnap = true
		defer func() {
			if deleteSnap {
				v.DeleteSnapshot(snapID)
			}
		}()
	} else {
		// Verify snapshot exists.
		v.snapMu.RLock()
		_, ok := v.snapshots[snapID]
		v.snapMu.RUnlock()
		if !ok {
			return nil, ErrSnapshotNotFound
		}
	}

	h := sha256.New()
	mw := io.MultiWriter(w, h)

	totalBlocks := info.VolumeSize / uint64(info.BlockSize)
	chunkBlocks := uint64(exportChunkBlocks)
	var totalWritten uint64

	for startBlock := uint64(0); startBlock < totalBlocks; startBlock += chunkBlocks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		remaining := totalBlocks - startBlock
		n := chunkBlocks
		if n > remaining {
			n = remaining
		}
		readBytes := uint32(n * uint64(info.BlockSize))

		data, err := v.ReadSnapshot(snapID, startBlock, readBytes)
		if err != nil {
			return nil, fmt.Errorf("blockvol: export read block %d: %w", startBlock, err)
		}

		if _, err := mw.Write(data); err != nil {
			return nil, fmt.Errorf("blockvol: export write: %w", err)
		}
		totalWritten += uint64(len(data))
	}

	manifest := &SnapshotArtifactManifest{
		FormatVersion:     SnapshotArtifactFormatV1,
		SourceVolume:      v.Path(),
		SourceSizeBytes:   info.VolumeSize,
		SourceBlockSize:   info.BlockSize,
		StorageProfile:    "single",
		CreatedAt:         time.Now().UTC().Format(time.RFC3339),
		ArtifactLayout:    ArtifactLayoutSingleFile,
		DataObjectKey:     opts.DataObjectKey,
		DataSizeBytes:     totalWritten,
		SHA256:            hex.EncodeToString(h.Sum(nil)),
		Compression:       "none",
		ExportToolVersion: ExportToolVersion,
	}

	return manifest, nil
}

// ImportOptions configures a snapshot import.
type ImportOptions struct {
	AllowOverwrite bool // if true, allow import into a non-empty volume
}

// ImportSnapshot imports a snapshot artifact into this volume.
// The volume must match the manifest's size and block size.
// Data is read from r and written directly to the extent region, bypassing the WAL.
// SHA-256 is verified against the manifest after the full read.
func (v *BlockVol) ImportSnapshot(ctx context.Context, manifest *SnapshotArtifactManifest, r io.Reader, opts ImportOptions) error {
	if err := ValidateManifest(manifest); err != nil {
		return err
	}

	info := v.Info()
	if info.VolumeSize != manifest.SourceSizeBytes {
		return fmt.Errorf("%w: target=%d manifest=%d", ErrImportSizeMismatch, info.VolumeSize, manifest.SourceSizeBytes)
	}
	if info.BlockSize != manifest.SourceBlockSize {
		return fmt.Errorf("%w: target=%d manifest=%d", ErrImportBlockSizeMismatch, info.BlockSize, manifest.SourceBlockSize)
	}

	// Empty check: nextLSN > 1 means the volume has been written to.
	if !opts.AllowOverwrite && v.nextLSN.Load() > 1 {
		return ErrImportTargetNotEmpty
	}

	// Pause flusher — we write directly to extent.
	if err := v.flusher.PauseAndFlush(); err != nil {
		v.flusher.Resume()
		return fmt.Errorf("blockvol: import flush: %w", err)
	}
	defer v.flusher.Resume()

	// Stream data to extent, computing SHA-256.
	h := sha256.New()
	tr := io.TeeReader(r, h)

	extentStart := v.super.WALOffset + v.super.WALSize
	chunkSize := uint64(exportChunkBlocks) * uint64(info.BlockSize)
	buf := make([]byte, chunkSize)
	var totalRead uint64

	for totalRead < manifest.DataSizeBytes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		remaining := manifest.DataSizeBytes - totalRead
		readSize := chunkSize
		if readSize > remaining {
			readSize = remaining
		}

		n, err := io.ReadFull(tr, buf[:readSize])
		if n > 0 {
			writeOff := int64(extentStart) + int64(totalRead)
			if _, werr := v.fd.WriteAt(buf[:n], writeOff); werr != nil {
				return fmt.Errorf("blockvol: import write at offset %d: %w", writeOff, werr)
			}
			totalRead += uint64(n)
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("blockvol: import read: %w", err)
		}
	}

	if totalRead != manifest.DataSizeBytes {
		return fmt.Errorf("%w: read %d bytes, manifest declares %d", ErrImportDataShort, totalRead, manifest.DataSizeBytes)
	}

	// Verify checksum.
	got := hex.EncodeToString(h.Sum(nil))
	if got != manifest.SHA256 {
		return fmt.Errorf("%w: got %s, want %s", ErrChecksumMismatch, got, manifest.SHA256)
	}

	// Fsync extent.
	if err := v.fd.Sync(); err != nil {
		return fmt.Errorf("blockvol: import sync: %w", err)
	}

	// Reset WAL and dirty map for clean state.
	v.dirtyMap.Clear()
	v.wal.Reset()
	v.super.WALHead = 0
	v.super.WALTail = 0
	if err := v.persistSuperblock(); err != nil {
		return fmt.Errorf("blockvol: import persist superblock: %w", err)
	}

	return nil
}
