package csi

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTestManager(t *testing.T) *VolumeManager {
	t.Helper()
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-vm] ", log.LstdFlags)
	mgr := NewVolumeManager(dir, "127.0.0.1:0", "iqn.2024.com.seaweedfs", logger)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { mgr.Stop() })
	return mgr
}

func TestVolumeManager_CreateOpenClose(t *testing.T) {
	mgr := newTestManager(t)

	if err := mgr.CreateVolume("vol1", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}
	if !mgr.VolumeExists("vol1") {
		t.Fatal("expected vol1 to exist after create")
	}
	iqn := mgr.VolumeIQN("vol1")
	if iqn == "" {
		t.Fatal("expected non-empty IQN")
	}

	// Close
	if err := mgr.CloseVolume("vol1"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if mgr.VolumeExists("vol1") {
		t.Fatal("expected vol1 to not exist after close")
	}

	// Reopen
	if err := mgr.OpenVolume("vol1"); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !mgr.VolumeExists("vol1") {
		t.Fatal("expected vol1 to exist after reopen")
	}
}

func TestVolumeManager_DeleteRemovesFile(t *testing.T) {
	mgr := newTestManager(t)

	if err := mgr.CreateVolume("delvol", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	volPath := filepath.Join(mgr.dataDir, "delvol.blk")
	if _, err := os.Stat(volPath); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}

	if err := mgr.DeleteVolume("delvol"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := os.Stat(volPath); !os.IsNotExist(err) {
		t.Fatalf("expected file to be removed, got: %v", err)
	}
	if mgr.VolumeExists("delvol") {
		t.Fatal("expected volume to not exist after delete")
	}
}

func TestVolumeManager_DuplicateCreate(t *testing.T) {
	mgr := newTestManager(t)

	if err := mgr.CreateVolume("dup", 4*1024*1024); err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Same size -> idempotent success.
	if err := mgr.CreateVolume("dup", 4*1024*1024); err != nil {
		t.Fatalf("duplicate create (same size): expected success, got: %v", err)
	}
	// Larger size -> mismatch error.
	err := mgr.CreateVolume("dup", 8*1024*1024)
	if err != ErrVolumeSizeMismatch {
		t.Fatalf("expected ErrVolumeSizeMismatch, got: %v", err)
	}
}

func TestVolumeManager_ListenAddr(t *testing.T) {
	mgr := newTestManager(t)

	addr := mgr.ListenAddr()
	if addr == "" {
		t.Fatal("expected non-empty listen addr")
	}
}

func TestVolumeManager_OpenNonExistent(t *testing.T) {
	mgr := newTestManager(t)

	err := mgr.OpenVolume("nonexistent")
	if err == nil {
		t.Fatal("expected error opening non-existent volume")
	}
}

func TestVolumeManager_CloseAlreadyClosed(t *testing.T) {
	mgr := newTestManager(t)

	// Close a volume that was never opened -- should be idempotent.
	if err := mgr.CloseVolume("nope"); err != nil {
		t.Fatalf("close non-existent: %v", err)
	}
}

func TestVolumeManager_ConcurrentCreateDelete(t *testing.T) {
	mgr := newTestManager(t)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		name := "conc" + string(rune('0'+i))
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if err := mgr.CreateVolume(n, 4*1024*1024); err != nil {
				t.Errorf("create %s: %v", n, err)
				return
			}
			if err := mgr.DeleteVolume(n); err != nil {
				t.Errorf("delete %s: %v", n, err)
			}
		}(name)
	}
	wg.Wait()
}

func TestVolumeManager_SanitizeIQN(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"pvc-abc123", "pvc-abc123"},
		{"PVC_ABC123", "pvc-abc123"},
		{"hello world!", "hello-world-"},
		{"a/b\\c:d", "a-b-c-d"},
	}
	for _, tt := range tests {
		got := SanitizeIQN(tt.input)
		if got != tt.want {
			t.Errorf("SanitizeIQN(%q): got %q, want %q", tt.input, got, tt.want)
		}
	}

	// Test truncation to 64 chars.
	long := ""
	for i := 0; i < 100; i++ {
		long += "a"
	}
	if len(SanitizeIQN(long)) != 64 {
		t.Fatalf("expected truncation to 64, got %d", len(SanitizeIQN(long)))
	}
}

// TestVolumeManager_CreateIdempotentAfterRestart simulates driver restart:
// existing .blk file on disk but not tracked in-memory.
func TestVolumeManager_CreateIdempotentAfterRestart(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-vm] ", log.LstdFlags)

	// Phase 1: create a volume, then stop the manager.
	mgr1 := NewVolumeManager(dir, "127.0.0.1:0", "iqn.2024.com.seaweedfs", logger)
	if err := mgr1.Start(context.Background()); err != nil {
		t.Fatalf("start1: %v", err)
	}
	if err := mgr1.CreateVolume("restart-vol", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}
	mgr1.Stop()

	// Verify .blk file still exists on disk.
	volPath := filepath.Join(dir, "restart-vol.blk")
	if _, err := os.Stat(volPath); err != nil {
		t.Fatalf("expected .blk file to exist: %v", err)
	}

	// Phase 2: new manager (simulates restart) -- CreateVolume should
	// adopt the existing file and return success.
	mgr2 := NewVolumeManager(dir, "127.0.0.1:0", "iqn.2024.com.seaweedfs", logger)
	if err := mgr2.Start(context.Background()); err != nil {
		t.Fatalf("start2: %v", err)
	}
	defer mgr2.Stop()

	if err := mgr2.CreateVolume("restart-vol", 4*1024*1024); err != nil {
		t.Fatalf("create after restart: expected idempotent success, got: %v", err)
	}
	if !mgr2.VolumeExists("restart-vol") {
		t.Fatal("expected volume to be tracked after adoption")
	}
	if mgr2.VolumeSizeBytes("restart-vol") < 4*1024*1024 {
		t.Fatalf("expected size >= 4MiB, got %d", mgr2.VolumeSizeBytes("restart-vol"))
	}
}

// TestVolumeManager_IQNCollision verifies that two long names sharing a prefix
// produce distinct IQNs after truncation.
func TestVolumeManager_IQNCollision(t *testing.T) {
	prefix := ""
	for i := 0; i < 70; i++ {
		prefix += "a"
	}
	name1 := prefix + "-suffix1"
	name2 := prefix + "-suffix2"

	iqn1 := SanitizeIQN(name1)
	iqn2 := SanitizeIQN(name2)

	if iqn1 == iqn2 {
		t.Fatalf("IQN collision: both names produced %q", iqn1)
	}
	if len(iqn1) > 64 || len(iqn2) > 64 {
		t.Fatalf("IQN too long: %d, %d", len(iqn1), len(iqn2))
	}
}
