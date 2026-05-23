package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newFinalizeTestServer(store *renameTestStore) *FilerServer {
	f := newRenameTestFiler(store)
	f.DirBucketsPath = "/buckets"
	return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}
}

func seedVersions(store *renameTestStore, latestFile, latestEtag string) {
	store.entries["/buckets/b/obj/.versions"] = &filer.Entry{
		FullPath: "/buckets/b/obj/.versions",
		Attr:     filer.Attr{Inode: 10, Mode: 0755 | (1 << 31)},
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionFileNameKey: []byte(latestFile),
			s3_constants.ExtLatestVersionETagKey:     []byte(latestEtag),
		},
	}
	store.entries["/buckets/b/obj/.versions/"+latestFile] = &filer.Entry{
		FullPath: util.FullPath("/buckets/b/obj/.versions/" + latestFile),
		Attr:     filer.Attr{Inode: 11},
		Extended: map[string][]byte{},
	}
}

func finalizeReq(cond *filer_pb.WriteCondition) *filer_pb.FinalizeVersionedWriteRequest {
	return &filer_pb.FinalizeVersionedWriteRequest{
		VersionsDir: "/buckets/b/obj/.versions",
		SetExtended: map[string][]byte{
			s3_constants.ExtLatestVersionFileNameKey: []byte("v2.ver"),
			s3_constants.ExtLatestVersionIdKey:       []byte("vid2"),
		},
		DeleteExtended:        []string{s3_constants.ExtLatestVersionETagKey},
		PriorLatestKey:        s3_constants.ExtLatestVersionFileNameKey,
		NoncurrentSinceKey:    s3_constants.ExtNoncurrentSinceNsKey,
		NoncurrentSinceNs:     77777,
		Condition:             cond,
		LatestEtagKey:         s3_constants.ExtLatestVersionETagKey,
		LatestDeleteMarkerKey: s3_constants.ExtLatestVersionIsDeleteMarker,
	}
}

// The finalize flips the latest pointer and stamps the previously-latest version
// noncurrent, atomically under the .versions lock.
func TestFinalizeVersionedWriteFlipAndDemote(t *testing.T) {
	store := newRenameTestStore()
	seedVersions(store, "v1.ver", "etag1")
	fs := newFinalizeTestServer(store)

	resp, err := fs.FinalizeVersionedWrite(context.Background(), finalizeReq(&filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_NONE}))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected resp error: %s", resp.Error)
	}

	if got := string(store.entries["/buckets/b/obj/.versions"].Extended[s3_constants.ExtLatestVersionFileNameKey]); got != "v2.ver" {
		t.Errorf("latest pointer not flipped: %q", got)
	}
	if got := string(store.entries["/buckets/b/obj/.versions/v1.ver"].Extended[s3_constants.ExtNoncurrentSinceNsKey]); got != "77777" {
		t.Errorf("previous latest not demoted: %q", got)
	}
}

// A failing precondition is reported and nothing is flipped or demoted.
func TestFinalizeVersionedWritePreconditionFailed(t *testing.T) {
	store := newRenameTestStore()
	seedVersions(store, "v1.ver", "etag1")
	fs := newFinalizeTestServer(store)

	// IF_NOT_EXISTS against an existing latest must fail.
	resp, err := fs.FinalizeVersionedWrite(context.Background(), finalizeReq(&filer_pb.WriteCondition{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.ErrorCode != filer_pb.FilerError_PRECONDITION_FAILED {
		t.Fatalf("want PRECONDITION_FAILED, got %v (%q)", resp.ErrorCode, resp.Error)
	}
	if got := string(store.entries["/buckets/b/obj/.versions"].Extended[s3_constants.ExtLatestVersionFileNameKey]); got != "v1.ver" {
		t.Errorf("pointer should be unchanged on precondition failure, got %q", got)
	}
	if _, stamped := store.entries["/buckets/b/obj/.versions/v1.ver"].Extended[s3_constants.ExtNoncurrentSinceNsKey]; stamped {
		t.Errorf("previous latest should not be demoted on precondition failure")
	}
}
