package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestReadOnlyReport(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("NewVolume: %v", err)
	}
	t.Cleanup(v.Close)
	v.location = &DiskLocation{Directory: dir, DiskType: types.HddType}

	check := func(name string, wantReadOnly, wantCanDelete, wantLowDisk bool) {
		t.Helper()
		readOnly, canDelete, lowDisk := v.readOnlyReport()
		if readOnly != wantReadOnly || canDelete != wantCanDelete || lowDisk != wantLowDisk {
			t.Fatalf("%s: got readOnly=%t canDelete=%t lowDisk=%t, want %t %t %t", name, readOnly, canDelete, lowDisk, wantReadOnly, wantCanDelete, wantLowDisk)
		}
	}

	check("writable", false, false, false)

	v.location.isDiskSpaceLow.Store(true)
	check("low disk space only", true, false, true)

	v.noWriteCanDelete = true
	check("low disk space and marked read-only-can-delete", true, true, false)

	v.noWriteCanDelete = false
	v.noWriteOrDelete = true
	check("low disk space and no-write", true, false, false)

	v.location.isDiskSpaceLow.Store(false)
	check("no-write only", true, false, false)
}

func TestReportHashCoversReadOnlyLowDisk(t *testing.T) {
	base := VolumeInfo{Id: 1, Size: 100, ReadOnly: true, ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: needle.EMPTY_TTL}
	lowDisk := base
	lowDisk.ReadOnlyLowDisk = true
	if base.ReportHash() == lowDisk.ReportHash() {
		t.Fatal("ReportHash does not change with ReadOnlyLowDisk, so the master would never learn about it")
	}
}
