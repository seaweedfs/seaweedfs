package dash

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
)

func countTaskStateFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, TasksSubdir))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("read tasks dir: %v", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".pb" {
			count++
		}
	}
	return count
}

// TestCancelledTaskFilesDoNotAccumulate reproduces issue #11595: every scan
// cycle cancels pending tasks of each detected type and re-detects them, so a
// cancelled task file per candidate volume accumulates on disk forever.
func TestCancelledTaskFilesDoNotAccumulate(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	queue := maintenance.NewMaintenanceQueue(nil)
	queue.SetPersistence(cp)

	for cycle := 0; cycle < 3; cycle++ {
		queue.AddTask(&maintenance.MaintenanceTask{
			ID:       fmt.Sprintf("ec_vol_%d_cycle_%d", cycle, cycle),
			Type:     "erasure_coding",
			VolumeID: uint32(cycle + 1),
			Server:   "server1",
		})
		if cancelled := queue.CancelPendingTasksByType("erasure_coding"); cancelled != 1 {
			t.Fatalf("cycle %d: cancelled %d tasks, want 1", cycle, cancelled)
		}
	}

	if n := countTaskStateFiles(t, dir); n != 0 {
		t.Errorf("%d task files on disk after %d cancel cycles, want 0", n, 3)
	}
}

// TestManuallyCancelledTaskFileIsRemoved covers the CancelTask path used by
// the UI: a cancelled pending task must not leave its file behind, where a
// restart would resurrect it as pending.
func TestManuallyCancelledTaskFileIsRemoved(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	manager := maintenance.NewMaintenanceManager(nil, nil, cp)
	queue := manager.GetQueue()
	queue.SetPersistence(cp)

	queue.AddTask(&maintenance.MaintenanceTask{
		ID:       "manual_1",
		Type:     "vacuum",
		VolumeID: 7,
		Server:   "server1",
	})
	if n := countTaskStateFiles(t, dir); n != 1 {
		t.Fatalf("%d task files after AddTask, want 1", n)
	}

	if err := manager.CancelTask("manual_1"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	if n := countTaskStateFiles(t, dir); n != 0 {
		t.Errorf("%d task files on disk after CancelTask, want 0", n)
	}
}

// TestCleanupCompletedTasksBoundsCancelledFiles checks retention covers
// cancelled files, e.g. ones written by older versions.
func TestCleanupCompletedTasksBoundsCancelledFiles(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	for i := 0; i < MaxCompletedTasks+5; i++ {
		if err := cp.SaveTaskState(&maintenance.MaintenanceTask{
			ID:     fmt.Sprintf("old_cancelled_%02d", i),
			Type:   "erasure_coding",
			Status: maintenance.TaskStatusCancelled,
		}); err != nil {
			t.Fatalf("save task state: %v", err)
		}
	}

	if err := cp.CleanupCompletedTasks(); err != nil {
		t.Fatalf("CleanupCompletedTasks: %v", err)
	}
	if n := countTaskStateFiles(t, dir); n > MaxCompletedTasks {
		t.Errorf("%d task files after cleanup, want at most %d", n, MaxCompletedTasks)
	}
}
