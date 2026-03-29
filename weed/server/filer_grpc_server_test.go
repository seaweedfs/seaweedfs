package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestResolveAssignStorageOptionUsesBucketRuleBeforeFilerDiskDefault(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/zot",
		DiskType:       "disk",
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := &FilerServer{
		option: &FilerOption{
			DiskType: "hdd",
		},
		filer: &filer.Filer{
			DirBucketsPath:    "/buckets",
			FilerConf:         fc,
			MaxFilenameLength: 255,
		},
	}

	so, err := fs.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/buckets/zot/.uploads/upload-id/0001_part.part",
	})
	if err != nil {
		t.Fatalf("resolve assign storage option: %v", err)
	}

	if got, want := so.Collection, "zot"; got != want {
		t.Fatalf("collection = %q, want %q", got, want)
	}
	if got, want := so.DiskType, "disk"; got != want {
		t.Fatalf("disk type = %q, want %q", got, want)
	}
}

func TestResolveAssignStorageOptionFallsBackToFilerDiskDefault(t *testing.T) {
	fs := &FilerServer{
		option: &FilerOption{
			DiskType: "hdd",
		},
		filer: &filer.Filer{
			DirBucketsPath:    "/buckets",
			FilerConf:         filer.NewFilerConf(),
			MaxFilenameLength: 255,
		},
	}

	so, err := fs.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/tmp/unmatched/file.bin",
	})
	if err != nil {
		t.Fatalf("resolve assign storage option: %v", err)
	}

	if got, want := so.DiskType, "hdd"; got != want {
		t.Fatalf("disk type = %q, want %q", got, want)
	}
}
