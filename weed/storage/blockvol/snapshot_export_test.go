package blockvol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
)

func TestManifest_RoundTrip(t *testing.T) {
	m := &SnapshotArtifactManifest{
		FormatVersion:     SnapshotArtifactFormatV1,
		SourceVolume:      "/data/vol1",
		SourceSizeBytes:   1 << 20,
		SourceBlockSize:   4096,
		StorageProfile:    "single",
		CreatedAt:         "2026-03-13T00:00:00Z",
		ArtifactLayout:    ArtifactLayoutSingleFile,
		DataObjectKey:     "backup/vol1/data.raw",
		DataSizeBytes:     1 << 20,
		SHA256:            "abcd1234",
		Compression:       "none",
		ExportToolVersion: ExportToolVersion,
	}

	data, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("MarshalManifest: %v", err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("UnmarshalManifest: %v", err)
	}

	if got.FormatVersion != m.FormatVersion {
		t.Errorf("FormatVersion = %d, want %d", got.FormatVersion, m.FormatVersion)
	}
	if got.SourceSizeBytes != m.SourceSizeBytes {
		t.Errorf("SourceSizeBytes = %d, want %d", got.SourceSizeBytes, m.SourceSizeBytes)
	}
	if got.SHA256 != m.SHA256 {
		t.Errorf("SHA256 = %q, want %q", got.SHA256, m.SHA256)
	}
	if got.DataObjectKey != m.DataObjectKey {
		t.Errorf("DataObjectKey = %q, want %q", got.DataObjectKey, m.DataObjectKey)
	}
}

func TestManifest_Validate_BadVersion(t *testing.T) {
	m := validManifest()
	m.FormatVersion = 99
	if err := ValidateManifest(m); err == nil {
		t.Fatal("expected error for bad version")
	}
}

func TestManifest_Validate_BadProfile(t *testing.T) {
	m := validManifest()
	m.StorageProfile = "striped"
	if err := ValidateManifest(m); err == nil {
		t.Fatal("expected error for bad profile")
	}
}

func TestManifest_Validate_BadLayout(t *testing.T) {
	m := validManifest()
	m.ArtifactLayout = "multi-part"
	if err := ValidateManifest(m); err == nil {
		t.Fatal("expected error for bad layout")
	}
}

func TestManifest_Validate_MissingFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SnapshotArtifactManifest)
	}{
		{"no size", func(m *SnapshotArtifactManifest) { m.SourceSizeBytes = 0 }},
		{"no block size", func(m *SnapshotArtifactManifest) { m.SourceBlockSize = 0 }},
		{"no sha256", func(m *SnapshotArtifactManifest) { m.SHA256 = "" }},
		{"no data size", func(m *SnapshotArtifactManifest) { m.DataSizeBytes = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.mutate(m)
			if err := ValidateManifest(m); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestExportSnapshot_Basic(t *testing.T) {
	vol := createExportTestVol(t, 64*1024) // 64KB volume

	// Write known pattern.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{
		DataObjectKey: "test/data.raw",
	})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	if manifest.DataSizeBytes != 64*1024 {
		t.Errorf("DataSizeBytes = %d, want %d", manifest.DataSizeBytes, 64*1024)
	}
	if manifest.SourceSizeBytes != 64*1024 {
		t.Errorf("SourceSizeBytes = %d, want %d", manifest.SourceSizeBytes, 64*1024)
	}
	if manifest.SHA256 == "" {
		t.Error("SHA256 is empty")
	}
	if manifest.StorageProfile != "single" {
		t.Errorf("StorageProfile = %q, want single", manifest.StorageProfile)
	}
	if uint64(buf.Len()) != manifest.DataSizeBytes {
		t.Errorf("buffer len = %d, want %d", buf.Len(), manifest.DataSizeBytes)
	}
}

func TestExportSnapshot_ChecksumCorrect(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)

	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xBB
	}
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Independently compute SHA-256 of the exported data.
	h := sha256.Sum256(buf.Bytes())
	want := hex.EncodeToString(h[:])
	if manifest.SHA256 != want {
		t.Errorf("manifest SHA256 = %q, independently computed = %q", manifest.SHA256, want)
	}
}

func TestExportSnapshot_ExistingSnapshot(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)

	data := make([]byte, 4096)
	data[0] = 0xCC
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Create snapshot manually.
	if err := vol.CreateSnapshot(42); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Write more data after snapshot.
	data[0] = 0xDD
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Export from existing snapshot — should get pre-write data.
	var buf bytes.Buffer
	_, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{SnapshotID: 42})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// First byte should be 0xCC (snapshot-time), not 0xDD (current).
	if buf.Bytes()[0] != 0xCC {
		t.Errorf("exported first byte = 0x%02X, want 0xCC", buf.Bytes()[0])
	}

	vol.DeleteSnapshot(42)
}

func TestExportSnapshot_ProfileReject(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)
	// Force profile to non-single for test.
	vol.super.StorageProfile = uint8(ProfileStriped)

	var buf bytes.Buffer
	_, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err == nil {
		t.Fatal("expected error for non-single profile")
	}
}

func TestImportSnapshot_Basic(t *testing.T) {
	// Create source, write pattern, export.
	srcVol := createExportTestVol(t, 64*1024)
	pattern := make([]byte, 4096)
	for i := range pattern {
		pattern[i] = byte(i % 251) // prime-mod pattern for uniqueness
	}
	if err := srcVol.WriteLBA(0, pattern); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{
		DataObjectKey: "test/data.raw",
	})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Create target, import.
	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	// Read back and verify pattern.
	got, err := dstVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, pattern) {
		t.Error("imported data does not match source pattern")
	}
}

func TestImportSnapshot_SizeMismatch(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Create target with different size.
	dstVol := createExportTestVol(t, 128*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
}

func TestImportSnapshot_NonEmptyReject(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Create target and write to it (makes it non-empty).
	dstVol := createExportTestVol(t, 64*1024)
	if err := dstVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err == nil {
		t.Fatal("expected non-empty target error")
	}
}

func TestImportSnapshot_AllowOverwrite(t *testing.T) {
	// Source with known pattern.
	srcVol := createExportTestVol(t, 64*1024)
	pattern := make([]byte, 4096)
	pattern[0] = 0xEE
	if err := srcVol.WriteLBA(0, pattern); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Target with existing data.
	dstVol := createExportTestVol(t, 64*1024)
	if err := dstVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{AllowOverwrite: true})
	if err != nil {
		t.Fatalf("ImportSnapshot with AllowOverwrite: %v", err)
	}

	got, err := dstVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if got[0] != 0xEE {
		t.Errorf("overwritten data first byte = 0x%02X, want 0xEE", got[0])
	}
}

func TestImportSnapshot_ChecksumMismatch(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Corrupt the data.
	exported := buf.Bytes()
	exported[0] ^= 0xFF

	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(exported), ImportOptions{})
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}

// createExportTestVol creates a temporary BlockVol with a specified size for export tests.
func createExportTestVol(t *testing.T, volumeSize uint64) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.vol")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volumeSize,
		WALSize:    32 << 10, // 32KB WAL
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	t.Cleanup(func() { vol.Close() })
	return vol
}

// validManifest returns a valid manifest for test mutation.
func validManifest() *SnapshotArtifactManifest {
	return &SnapshotArtifactManifest{
		FormatVersion:     SnapshotArtifactFormatV1,
		SourceVolume:      "/data/vol1",
		SourceSizeBytes:   1 << 20,
		SourceBlockSize:   4096,
		StorageProfile:    "single",
		CreatedAt:         "2026-03-13T00:00:00Z",
		ArtifactLayout:    ArtifactLayoutSingleFile,
		DataObjectKey:     "backup/data.raw",
		DataSizeBytes:     1 << 20,
		SHA256:            "abc123",
		Compression:       "none",
		ExportToolVersion: ExportToolVersion,
	}
}
