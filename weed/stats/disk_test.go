//go:build !netbsd && !plan9

package stats

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestSetDiskStatusSuccess(t *testing.T) {
	disk := &volume_server_pb.DiskStatus{Dir: t.TempDir()}
	diskProbe(disk, DefaultDiskIOProbeConfig())

	if disk.Error != "" {
		t.Fatalf("unexpected disk error: %s", disk.Error)
	}
	if disk.All == 0 {
		t.Fatalf("expected non-zero capacity for a real directory")
	}
}

func TestSetDiskStatusReportsRepeatedFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	config := DefaultDiskIOProbeConfig()
	var disk *volume_server_pb.DiskStatus
	for i := 0; i < config.MaxStatFailures; i++ {
		disk = &volume_server_pb.DiskStatus{Dir: missing}
		diskProbe(disk, config)
	}

	if disk.Error == "" {
		t.Fatalf("expected disk error after %d failed checks", config.MaxStatFailures)
	}
}

func TestSetDiskStatusRequiresRepeatedSuccessesToRecover(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "temporarily-missing")
	config := DefaultDiskIOProbeConfig()
	var disk *volume_server_pb.DiskStatus
	for i := 0; i < config.MaxStatFailures; i++ {
		disk = &volume_server_pb.DiskStatus{Dir: missing}
		diskProbe(disk, config)
	}
	if disk.Error == "" {
		t.Fatalf("expected disk error after %d failed checks", config.MaxStatFailures)
	}

	if err := os.Mkdir(missing, 0755); err != nil {
		t.Fatalf("mkdir %s: %v", missing, err)
	}

	recoveryChecks := statRecoverySuccesses(config)
	for i := 1; i < recoveryChecks; i++ {
		disk = &volume_server_pb.DiskStatus{Dir: missing}
		diskProbe(disk, config)
		if disk.Error == "" {
			t.Fatalf("expected disk error to persist after %d successful checks", i)
		}
	}

	disk = &volume_server_pb.DiskStatus{Dir: missing}
	diskProbe(disk, config)
	if disk.Error != "" {
		t.Fatalf("expected disk error to recover after %d successful checks: %s", recoveryChecks, disk.Error)
	}
}

func TestSlowLatencyForFallsBackToDefault(t *testing.T) {
	config := DiskIOProbeConfig{
		SlowLatency: 500 * time.Millisecond,
		SlowLatencyByDiskType: map[string]time.Duration{
			"hdd":  500 * time.Millisecond,
			"nvme": 50 * time.Millisecond,
		},
	}

	for diskType, want := range map[string]time.Duration{
		"hdd":       500 * time.Millisecond,
		"nvme":      50 * time.Millisecond,
		"ssd":       500 * time.Millisecond,
		"nvme-gen5": 500 * time.Millisecond,
	} {
		if got := config.SlowLatencyFor(diskType); got != want {
			t.Errorf("disk type %q: got %v, want %v", diskType, got, want)
		}
	}

	empty := DiskIOProbeConfig{SlowLatency: 100 * time.Millisecond}
	if got := empty.SlowLatencyFor("nvme"); got != 100*time.Millisecond {
		t.Errorf("unconfigured table: got %v, want %v", got, 100*time.Millisecond)
	}
}
