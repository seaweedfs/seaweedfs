//go:build !netbsd && !plan9

package stats

import (
	"os"
	"path/filepath"
	"testing"

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
