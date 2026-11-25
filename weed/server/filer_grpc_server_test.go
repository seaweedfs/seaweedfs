package weed_server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newAssignTestServer(fc *filer.FilerConf, diskType string) *FilerServer {
	return &FilerServer{
		option: &FilerOption{
			DiskType: diskType,
		},
		filer: &filer.Filer{
			DirBucketsPath:    "/buckets",
			FilerConf:         fc,
			MaxFilenameLength: 255,
		},
	}
}

func TestResolveAssignStorageOptionUsesBucketRuleBeforeFilerDiskDefault(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/zot",
		DiskType:       "disk",
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := newAssignTestServer(fc, "hdd")

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
	fs := newAssignTestServer(filer.NewFilerConf(), "hdd")

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

// A read-only path is a verdict, not a transport failure: it must come back as a
// successful RPC (so clients neither retry nor fail over) carrying READ_ONLY.
func TestAssignVolumeReadOnlyReturnsErrorCode(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/overquota",
		ReadOnly:       true,
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := newAssignTestServer(fc, "")

	resp, err := fs.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
		Path: "/buckets/overquota/x",
	})
	if err != nil {
		t.Fatalf("AssignVolume err = %v, want a response", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_READ_ONLY {
		t.Fatalf("AssignVolume error_code = %v, want %v", resp.ErrorCode, filer_pb.FilerError_READ_ONLY)
	}
	// Clients that predate error_code still get the old message.
	if want := "assign volume: read only: /buckets/overquota"; !strings.HasPrefix(resp.Error, want) {
		t.Fatalf("AssignVolume error = %q, want prefix %q", resp.Error, want)
	}
	if err := filer_pb.AssignVolumeResponseError(resp); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("AssignVolumeResponseError = %v, want ErrReadOnly", err)
	}
}

// A create on a read-only path must come back as a coded response, like
// AssignVolume. A gRPC error reaches clients as codes.Unknown, and they retry
// that like a filer outage.
func TestCreateEntryReadOnlyReturnsErrorCode(t *testing.T) {
	fc := filer.NewFilerConf()
	if err := fc.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/buckets/overquota",
		ReadOnly:       true,
	}); err != nil {
		t.Fatalf("set location conf: %v", err)
	}

	fs := newAssignTestServer(fc, "")

	resp, err := fs.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/buckets/overquota/dir",
		Entry: &filer_pb.Entry{
			Name:       "x",
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
		},
	})
	if err != nil {
		t.Fatalf("CreateEntry err = %v, want a response", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_READ_ONLY {
		t.Fatalf("CreateEntry error_code = %v, want %v", resp.ErrorCode, filer_pb.FilerError_READ_ONLY)
	}
	if want := "read only: /buckets/overquota"; !strings.HasPrefix(resp.Error, want) {
		t.Fatalf("CreateEntry error = %q, want prefix %q", resp.Error, want)
	}
}

func TestResolveAssignStorageOptionKeepsScopedCallerInPathCollection(t *testing.T) {
	fs := newAssignTestServer(filer.NewFilerConf(), "hdd")
	scoped := withScopeKeepOnly(context.Background())
	path := "/buckets/zot/file.bin"

	for _, collection := range []string{"", "zot"} {
		so, err := fs.resolveAssignStorageOption(scoped, &filer_pb.AssignVolumeRequest{Path: path, Collection: collection})
		if err != nil || so.Collection != "zot" {
			t.Fatalf("scoped assign with collection %q: so=%+v err=%v, want collection zot", collection, so, err)
		}
	}
	if _, err := fs.resolveAssignStorageOption(scoped, &filer_pb.AssignVolumeRequest{Path: path, Collection: "other"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped assign into another collection: %v, want PermissionDenied", err)
	}
	if resp, err := fs.AssignVolume(scoped, &filer_pb.AssignVolumeRequest{Path: path, Collection: "other", Count: 1}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped AssignVolume into another collection: resp=%v err=%v, want PermissionDenied", resp, err)
	}
	so, err := fs.resolveAssignStorageOption(context.Background(), &filer_pb.AssignVolumeRequest{Path: path, Collection: "other"})
	if err != nil || so.Collection != "other" {
		t.Fatalf("unscoped assign: so=%+v err=%v, want collection other", so, err)
	}
}
