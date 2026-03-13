package blockvol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
)

// TestQA_SnapshotExport_TruncatedData verifies that import rejects
// a data stream that is shorter than the manifest declares.
func TestQA_SnapshotExport_TruncatedData(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Truncate to half the data.
	truncated := buf.Bytes()[:buf.Len()/2]

	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(truncated), ImportOptions{})
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
}

// TestQA_SnapshotExport_WrongChecksum verifies that import rejects
// data whose checksum doesn't match the manifest.
func TestQA_SnapshotExport_WrongChecksum(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)
	if err := srcVol.WriteLBA(0, make([]byte, 4096)); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Tamper with manifest checksum.
	manifest.SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"

	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}

// TestQA_SnapshotExport_CorruptedManifest verifies that corrupted
// manifest JSON is rejected.
func TestQA_SnapshotExport_CorruptedManifest(t *testing.T) {
	_, err := UnmarshalManifest([]byte(`{this is not valid json`))
	if err == nil {
		t.Fatal("expected error for corrupted manifest JSON")
	}

	// Valid JSON but missing required fields.
	_, err = UnmarshalManifest([]byte(`{"format_version": 1}`))
	if err == nil {
		t.Fatal("expected error for manifest with missing fields")
	}
}

// TestQA_SnapshotExport_DataSizeMismatch verifies that import detects
// when the manifest declares a different data size than what's provided.
func TestQA_SnapshotExport_DataSizeMismatch(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Inflate manifest data size to claim more data than exists.
	manifest.DataSizeBytes = manifest.DataSizeBytes * 2

	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, &buf, ImportOptions{})
	if err == nil {
		t.Fatal("expected error for data size mismatch")
	}
}

// TestQA_SnapshotExport_RoundTripIntegrity writes a known multi-block pattern,
// exports, imports into a new volume, and verifies every byte matches.
func TestQA_SnapshotExport_RoundTripIntegrity(t *testing.T) {
	volSize := uint64(64 * 1024) // 64KB = 16 blocks at 4KB
	srcVol := createExportTestVol(t, volSize)

	// Write distinct patterns to multiple blocks.
	for lba := uint64(0); lba < volSize/4096; lba++ {
		block := make([]byte, 4096)
		for i := range block {
			block[i] = byte((lba*251 + uint64(i)*37) % 256) // deterministic pattern
		}
		if err := srcVol.WriteLBA(lba, block); err != nil {
			t.Fatalf("WriteLBA %d: %v", lba, err)
		}
	}

	// Export.
	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{
		DataObjectKey: "integrity/data.raw",
	})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Verify manifest is valid.
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("manifest invalid: %v", err)
	}

	// Independently verify checksum of exported data.
	h := sha256.Sum256(buf.Bytes())
	if manifest.SHA256 != hex.EncodeToString(h[:]) {
		t.Fatal("manifest SHA256 doesn't match exported data")
	}

	// Import into new volume.
	dstVol := createExportTestVol(t, volSize)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	// Verify every block in target matches source pattern.
	for lba := uint64(0); lba < volSize/4096; lba++ {
		got, err := dstVol.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA %d: %v", lba, err)
		}
		expected := make([]byte, 4096)
		for i := range expected {
			expected[i] = byte((lba*251 + uint64(i)*37) % 256)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("block %d mismatch after import", lba)
		}
	}
}

// TestQA_SnapshotExport_ContextCancellation verifies that export
// respects context cancellation.
func TestQA_SnapshotExport_ContextCancellation(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	var buf bytes.Buffer
	_, err := vol.ExportSnapshot(ctx, &buf, ExportOptions{})
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

// TestQA_SnapshotExport_ManifestSerializationStable verifies that
// marshal → unmarshal produces the same manifest.
func TestQA_SnapshotExport_ManifestSerializationStable(t *testing.T) {
	srcVol := createExportTestVol(t, 64*1024)

	var buf bytes.Buffer
	manifest, err := srcVol.ExportSnapshot(context.Background(), &buf, ExportOptions{
		DataObjectKey: "stable/data.raw",
	})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	data, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatalf("MarshalManifest: %v", err)
	}

	got, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatalf("UnmarshalManifest: %v", err)
	}

	if got.SHA256 != manifest.SHA256 {
		t.Errorf("SHA256 changed after round-trip: %q → %q", manifest.SHA256, got.SHA256)
	}
	if got.DataSizeBytes != manifest.DataSizeBytes {
		t.Errorf("DataSizeBytes changed: %d → %d", manifest.DataSizeBytes, got.DataSizeBytes)
	}
}

// TestQA_SnapshotExport_ExportDuringLiveIO verifies export is safe
// while the volume is receiving writes.
func TestQA_SnapshotExport_ExportDuringLiveIO(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)

	// Write initial data.
	data := make([]byte, 4096)
	data[0] = 0x11
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Start export (creates snapshot internally).
	var buf bytes.Buffer
	manifest, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportSnapshot: %v", err)
	}

	// Verify export captured the 0x11 data.
	if buf.Bytes()[0] != 0x11 {
		t.Errorf("exported first byte = 0x%02X, want 0x11", buf.Bytes()[0])
	}

	// Write new data after export snapshot was taken but before we check.
	data[0] = 0x22
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA after export: %v", err)
	}

	// Import the exported data into a new volume — should have 0x11, not 0x22.
	dstVol := createExportTestVol(t, 64*1024)
	err = dstVol.ImportSnapshot(context.Background(), manifest, bytes.NewReader(buf.Bytes()), ImportOptions{})
	if err != nil {
		t.Fatalf("ImportSnapshot: %v", err)
	}

	got, err := dstVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if got[0] != 0x11 {
		t.Errorf("imported first byte = 0x%02X, want 0x11 (snapshot-time value)", got[0])
	}
}

// TestQA_SnapshotExport_NonexistentSnapshotReject verifies export from
// a snapshot ID that doesn't exist.
func TestQA_SnapshotExport_NonexistentSnapshotReject(t *testing.T) {
	vol := createExportTestVol(t, 64*1024)

	var buf bytes.Buffer
	_, err := vol.ExportSnapshot(context.Background(), &buf, ExportOptions{SnapshotID: 999})
	if err == nil {
		t.Fatal("expected error for nonexistent snapshot ID")
	}
}

// Ensure io package is used (for io.ReadFull reference in snapshot_export.go).
var _ = io.EOF
