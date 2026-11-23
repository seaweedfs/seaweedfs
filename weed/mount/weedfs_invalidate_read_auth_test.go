package mount

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestRenameReTracksReadAuthUnderNewPath covers the read-token index across a
// rename. A read-only handle never reaches SetEntry/AddChunks/truncate again,
// so if the rename did not re-track its chunks, a later token refresh would
// look up the vacated old path, find nothing, and the read would lose its
// credential. After the rename the handle's chunk must map to the new path.
func TestRenameReTracksReadAuthUnderNewPath(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.readAuth = newReadAuth(wfs)

	const fid = "3,abc"
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:     []*filer_pb.FileChunk{{FileId: fid, Size: 200}},
	}, 0, 0)

	// Precondition: the chunk is indexed under the original path, as SetEntry
	// would have done when the handle was opened.
	wfs.readAuth.Track(fh.FullPath(), []*filer_pb.FileChunk{{FileId: fid}})
	if got := wfs.readAuth.paths[fid]; got != util.FullPath("/dir/file") {
		t.Fatalf("precondition: chunk indexed to %q, want /dir/file", got)
	}

	rename := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "renamed", Attributes: &filer_pb.FuseAttributes{FileSize: 200}, Chunks: []*filer_pb.FileChunk{{FileId: fid, Size: 200}}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), rename, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if got := fh.FullPath(); got != util.FullPath("/dir/renamed") {
		t.Fatalf("handle path = %q, want /dir/renamed", got)
	}
	if got := wfs.readAuth.paths[fid]; got != util.FullPath("/dir/renamed") {
		t.Fatalf("chunk still indexed to %q after rename, want /dir/renamed", got)
	}
}

func TestSurvivingHardLinkReTracksReadAuthUnderNewPath(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.readAuth = newReadAuth(wfs)

	const fid = "3,abc"
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	wfs.inodeToPath.AddPath(inode, util.FullPath("/dir/survivor"))
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:            "file",
		HardLinkId:      []byte("link"),
		HardLinkCounter: 2,
		Attributes:      &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:          []*filer_pb.FileChunk{{FileId: fid, Size: 200}},
	}, 0, 0)

	// Precondition: the chunk is indexed to the name that the open handle was
	// using when the peer removed that name.
	wfs.readAuth.Track(fh.FullPath(), []*filer_pb.FileChunk{{FileId: fid}})

	deleteResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name:            "file",
				HardLinkId:      []byte("link"),
				HardLinkCounter: 2,
			},
		},
	}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), deleteResp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply hard-link delete: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()

	if got := fh.FullPath(); got != util.FullPath("/dir/survivor") {
		t.Fatalf("handle path = %q, want /dir/survivor", got)
	}
	if got := wfs.readAuth.paths[fid]; got != util.FullPath("/dir/survivor") {
		t.Fatalf("chunk still indexed to %q after hard-link delete, want /dir/survivor", got)
	}
}

// A writeback mount skips its own rename event, so the local rename response
// is the only place the open handle's chunks move to the new name.
func TestLocalRenameReTracksReadAuthUnderNewPath(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	wfs.readAuth = newReadAuth(wfs)

	const fid = "3,abc"
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, false)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200},
		Chunks:     []*filer_pb.FileChunk{{FileId: fid, Size: 200}},
	}, 0, 0)
	wfs.readAuth.Track(fh.FullPath(), []*filer_pb.FileChunk{{FileId: fid}})

	resp := &filer_pb.StreamRenameEntryResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "renamed", Attributes: &filer_pb.FuseAttributes{FileSize: 200, Inode: inode}, Chunks: []*filer_pb.FileChunk{{FileId: fid, Size: 200}}},
			NewParentPath: "/dir",
		},
	}
	if err := wfs.handleRenameResponse(context.Background(), resp, util.FullPath("/dir/renamed"), nil); err != nil {
		t.Fatalf("handle rename response: %v", err)
	}

	if got := fh.FullPath(); got != util.FullPath("/dir/renamed") {
		t.Fatalf("handle path = %q, want /dir/renamed", got)
	}
	if got := wfs.readAuth.paths[fid]; got != util.FullPath("/dir/renamed") {
		t.Fatalf("chunk still indexed to %q after local rename, want /dir/renamed", got)
	}
}
