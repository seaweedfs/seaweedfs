//go:build !netbsd && !plan9

package stats

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func TestSetDiskStatusSuccess(t *testing.T) {
	disk := &volume_server_pb.DiskStatus{Dir: t.TempDir()}
	setDiskStatus(disk)

	if disk.Error != "" {
		t.Fatalf("unexpected disk error: %s", disk.Error)
	}
	if disk.All == 0 {
		t.Fatalf("expected non-zero capacity for a real directory")
	}
}

func TestSetDiskStatusReportsRepeatedFailures(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")

	var disk *volume_server_pb.DiskStatus
	for i := 0; i < maxFailuresBeforeAlert; i++ {
		disk = &volume_server_pb.DiskStatus{Dir: missing}
		setDiskStatus(disk)
	}

	if disk.Error == "" {
		t.Fatalf("expected disk error after %d failed checks", maxFailuresBeforeAlert)
	}
}
