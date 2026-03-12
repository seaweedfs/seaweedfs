package blockvol

import (
	"bytes"
	"path/filepath"
	"testing"
)

const (
	expandVolSize = 1024 * 1024     // 1MB
	expandBlkSize = 4096
	expandWALSize = 64 * 1024       // 64KB
	expandNewSize = 2 * 1024 * 1024 // 2MB
)

func createExpandTestVol(t *testing.T) (*BlockVol, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: expandVolSize,
		BlockSize:  expandBlkSize,
		WALSize:    expandWALSize,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return vol, path
}

func TestExpand_Standalone_DirectCommit(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.Expand(expandNewSize); err != nil {
		t.Fatalf("expand: %v", err)
	}
	if vol.Info().VolumeSize != expandNewSize {
		t.Fatalf("VolumeSize: got %d, want %d", vol.Info().VolumeSize, expandNewSize)
	}
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState: got (%d,%d), want (0,0)", ps, ee)
	}
}

func TestExpand_Standalone_Idempotent(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.Expand(expandVolSize); err != nil {
		t.Fatalf("same-size expand should be no-op: %v", err)
	}
	if vol.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize changed: %d", vol.Info().VolumeSize)
	}
}

func TestExpand_Standalone_ShrinkRejected(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	err := vol.Expand(expandVolSize / 2)
	if err != ErrShrinkNotSupported {
		t.Fatalf("expected ErrShrinkNotSupported, got %v", err)
	}
}

func TestExpand_Standalone_SurvivesReopen(t *testing.T) {
	vol, path := createExpandTestVol(t)

	if err := vol.Expand(expandNewSize); err != nil {
		t.Fatalf("expand: %v", err)
	}
	vol.Close()

	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer vol2.Close()

	if vol2.Info().VolumeSize != expandNewSize {
		t.Fatalf("VolumeSize after reopen: got %d, want %d", vol2.Info().VolumeSize, expandNewSize)
	}
}

func TestPrepareExpand_Success(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 42); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if vol.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize should be unchanged: %d", vol.Info().VolumeSize)
	}
	ps, ee := vol.ExpandState()
	if ps != expandNewSize || ee != 42 {
		t.Fatalf("ExpandState: got (%d,%d), want (%d,42)", ps, ee, expandNewSize)
	}
}

func TestPrepareExpand_WriteBeyondOldSize_Rejected(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	newLBA := uint64(expandVolSize / expandBlkSize)
	data := make([]byte, expandBlkSize)
	err := vol.WriteLBA(newLBA, data)
	if err == nil {
		t.Fatal("write beyond old size should be rejected while in prepared state")
	}
}

func TestPrepareExpand_WriteWithinOldSize_OK(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	data := make([]byte, expandBlkSize)
	data[0] = 0xCC
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write within old size: %v", err)
	}
	got, err := vol.ReadLBA(0, expandBlkSize)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch")
	}
}

func TestCommitExpand_Success(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 7); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CommitExpand(7); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if vol.Info().VolumeSize != expandNewSize {
		t.Fatalf("VolumeSize: got %d, want %d", vol.Info().VolumeSize, expandNewSize)
	}
	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState: got (%d,%d), want (0,0)", ps, ee)
	}
}

func TestCommitExpand_WriteBeyondNewSize_OK(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CommitExpand(1); err != nil {
		t.Fatalf("commit: %v", err)
	}

	newLBA := uint64(expandVolSize / expandBlkSize)
	data := make([]byte, expandBlkSize)
	data[0] = 0xDD
	if err := vol.WriteLBA(newLBA, data); err != nil {
		t.Fatalf("write in expanded region: %v", err)
	}
	got, err := vol.ReadLBA(newLBA, expandBlkSize)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch in expanded region")
	}
}

func TestCommitExpand_EpochMismatch_Rejected(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 5); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	err := vol.CommitExpand(99)
	if err != ErrExpandEpochMismatch {
		t.Fatalf("expected ErrExpandEpochMismatch, got %v", err)
	}
	if vol.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize should be unchanged: %d", vol.Info().VolumeSize)
	}
}

func TestCancelExpand_ClearsPreparedState(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 3); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CancelExpand(3); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	ps, ee := vol.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState: got (%d,%d), want (0,0)", ps, ee)
	}
	if vol.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize should be unchanged: %d", vol.Info().VolumeSize)
	}
}

func TestCancelExpand_WriteStillRejectedInNewRange(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := vol.CancelExpand(1); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	newLBA := uint64(expandVolSize / expandBlkSize)
	data := make([]byte, expandBlkSize)
	err := vol.WriteLBA(newLBA, data)
	if err == nil {
		t.Fatal("write in expanded region should still be rejected after cancel")
	}
}

func TestPrepareExpand_AlreadyInFlight_Rejected(t *testing.T) {
	vol, _ := createExpandTestVol(t)
	defer vol.Close()

	if err := vol.PrepareExpand(expandNewSize, 1); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	err := vol.PrepareExpand(expandNewSize*2, 2)
	if err != ErrExpandAlreadyInFlight {
		t.Fatalf("expected ErrExpandAlreadyInFlight, got %v", err)
	}
}

func TestRecovery_PreparedState_Cleared(t *testing.T) {
	vol, path := createExpandTestVol(t)

	if err := vol.PrepareExpand(expandNewSize, 10); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	vol.Close()

	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer vol2.Close()

	ps, ee := vol2.ExpandState()
	if ps != 0 || ee != 0 {
		t.Fatalf("ExpandState after reopen: got (%d,%d), want (0,0)", ps, ee)
	}
	if vol2.Info().VolumeSize != expandVolSize {
		t.Fatalf("VolumeSize should be original after recovery: %d", vol2.Info().VolumeSize)
	}
}

func TestExpand_WithProfile_Single(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "profile.blk")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     expandVolSize,
		BlockSize:      expandBlkSize,
		WALSize:        expandWALSize,
		StorageProfile: ProfileSingle,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	if vol.Profile() != ProfileSingle {
		t.Fatalf("profile: got %d, want %d", vol.Profile(), ProfileSingle)
	}
	if err := vol.Expand(expandNewSize); err != nil {
		t.Fatalf("expand with single profile: %v", err)
	}
	if vol.Info().VolumeSize != expandNewSize {
		t.Fatalf("VolumeSize: got %d, want %d", vol.Info().VolumeSize, expandNewSize)
	}
}
