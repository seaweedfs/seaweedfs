package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestResolveMetadataLogAssignDiskTypeUsesPathRule(t *testing.T) {
	fc := NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/topics/.system/log",
		DiskType:       "hot",
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	f := &Filer{
		FilerConf:       fc,
		DefaultDiskType: "hdd",
	}

	got, rule := f.resolveMetadataLogAssignDiskType("/topics/.system/log/2026-06-23/12-00.1")
	if got != "hot" {
		t.Fatalf("disk type = %q, want %q", got, "hot")
	}
	if rule.DiskType != "hot" {
		t.Fatalf("rule disk type = %q, want %q", rule.DiskType, "hot")
	}
}

func TestResolveMetadataLogAssignDiskTypeFallsBackToFilerDefault(t *testing.T) {
	f := &Filer{
		FilerConf:       NewFilerConf(),
		DefaultDiskType: "hot",
	}

	got, _ := f.resolveMetadataLogAssignDiskType("/topics/.system/log/2026-06-23/12-00.1")
	if got != "hot" {
		t.Fatalf("disk type = %q, want %q", got, "hot")
	}
}

func TestResolveMetadataLogAssignDiskTypeNilFilerConf(t *testing.T) {
	f := &Filer{
		DefaultDiskType: "hot",
	}

	got, rule := f.resolveMetadataLogAssignDiskType("/topics/.system/log/2026-06-23/12-00.1")
	if got != "hot" {
		t.Fatalf("disk type = %q, want %q", got, "hot")
	}
	if rule == nil {
		t.Fatal("expected non-nil empty rule")
	}
}
