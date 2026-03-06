package blockvol

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParseDurabilityMode_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  DurabilityMode
	}{
		{"best_effort", DurabilityBestEffort},
		{"sync_all", DurabilitySyncAll},
		{"sync_quorum", DurabilitySyncQuorum},
		{"", DurabilityBestEffort}, // empty = backward compat
	}
	for _, tt := range tests {
		got, err := ParseDurabilityMode(tt.input)
		if err != nil {
			t.Errorf("ParseDurabilityMode(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseDurabilityMode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseDurabilityMode_Invalid(t *testing.T) {
	_, err := ParseDurabilityMode("invalid_mode")
	if !errors.Is(err, ErrInvalidDurabilityMode) {
		t.Errorf("ParseDurabilityMode(invalid) error = %v, want ErrInvalidDurabilityMode", err)
	}
}

func TestDurabilityMode_StringRoundTrip(t *testing.T) {
	for _, m := range []DurabilityMode{DurabilityBestEffort, DurabilitySyncAll, DurabilitySyncQuorum} {
		s := m.String()
		got, err := ParseDurabilityMode(s)
		if err != nil {
			t.Errorf("round-trip %v -> %q: parse error: %v", m, s, err)
		}
		if got != m {
			t.Errorf("round-trip %v -> %q -> %v", m, s, got)
		}
	}
}

func TestDurabilityMode_Validate_SyncQuorum_RF2_Rejected(t *testing.T) {
	err := DurabilitySyncQuorum.Validate(2)
	if !errors.Is(err, ErrSyncQuorumRequiresRF3) {
		t.Errorf("Validate(sync_quorum, RF=2) = %v, want ErrSyncQuorumRequiresRF3", err)
	}
}

func TestDurabilityMode_Validate_SyncQuorum_RF3_OK(t *testing.T) {
	if err := DurabilitySyncQuorum.Validate(3); err != nil {
		t.Errorf("Validate(sync_quorum, RF=3) = %v, want nil", err)
	}
}

func TestDurabilityMode_Validate_BestEffort_RF1_OK(t *testing.T) {
	if err := DurabilityBestEffort.Validate(1); err != nil {
		t.Errorf("Validate(best_effort, RF=1) = %v, want nil", err)
	}
}

func TestDurabilityMode_RequiredReplicas_SyncAll_RF2(t *testing.T) {
	got := DurabilitySyncAll.RequiredReplicas(2)
	if got != 1 {
		t.Errorf("RequiredReplicas(sync_all, RF=2) = %d, want 1", got)
	}
}

func TestDurabilityMode_RequiredReplicas_SyncQuorum_RF3(t *testing.T) {
	got := DurabilitySyncQuorum.RequiredReplicas(3)
	if got != 1 { // quorum=2, primary counts as 1, need 1 replica
		t.Errorf("RequiredReplicas(sync_quorum, RF=3) = %d, want 1", got)
	}
}

func TestDurabilityMode_IsStrict(t *testing.T) {
	if DurabilityBestEffort.IsStrict() {
		t.Error("best_effort should not be strict")
	}
	if !DurabilitySyncAll.IsStrict() {
		t.Error("sync_all should be strict")
	}
	if !DurabilitySyncQuorum.IsStrict() {
		t.Error("sync_quorum should be strict")
	}
}

func TestDurabilityMode_CreateCloseOpenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		DurabilityMode: DurabilitySyncAll,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if vol.DurabilityMode() != DurabilitySyncAll {
		t.Fatalf("DurabilityMode() = %v, want sync_all", vol.DurabilityMode())
	}
	vol.Close()

	vol2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer vol2.Close()
	if vol2.DurabilityMode() != DurabilitySyncAll {
		t.Errorf("DurabilityMode() after reopen = %v, want sync_all", vol2.DurabilityMode())
	}
}

func TestDurabilityMode_Accessor_Default(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer vol.Close()
	if vol.DurabilityMode() != DurabilityBestEffort {
		t.Errorf("default DurabilityMode() = %v, want best_effort", vol.DurabilityMode())
	}
}

func TestDurabilityMode_InvalidOnDisk_OpenFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")

	vol, err := CreateBlockVol(path, CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	vol.Close()

	// Corrupt the DurabilityMode byte on disk.
	// Epoch ends at offset 104. DurabilityMode is at offset 104.
	// Actually, let me compute: 4(magic)+2(ver)+2(flags)+16(uuid)+8(volsz)+4(ext)+4(blk)+
	// 8(waloff)+8(walsz)+8(walhead)+8(waltail)+8(walcplsn)+4(repl)+8(created)+4(snapcnt)+8(epoch) = 104
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt([]byte{99}, 104); err != nil {
		t.Fatalf("write corrupt byte: %v", err)
	}
	f.Close()

	_, err = OpenBlockVol(path)
	if err == nil {
		t.Fatal("OpenBlockVol should fail with invalid DurabilityMode")
	}
}
