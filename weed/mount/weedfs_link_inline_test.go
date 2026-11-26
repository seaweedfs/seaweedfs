package mount

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A small file the filer keeps inside its entry (-saveToFilerLimit, or
// ?saveInside=true on the upload) has content and no chunks. The filer writes
// a hard link's shared record from each write of one of its names, so the
// create of the new name has to carry the content as the update of the source
// does. Left out, the record lost it, and both names read zeros from then on.
func TestLinkCarriesInlineContentToTheNewName(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	content := []byte("kept inside the entry\n")
	source := &filer_pb.Entry{
		Name:    "inline.txt",
		Content: content,
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: uint64(len(content)),
			Crtime:   1,
			Mtime:    1,
			Uid:      99,
			Gid:      100,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", source), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	sourcePath := util.FullPath("/inline.txt")
	sourceInode := wfs.inodeToPath.Lookup(sourcePath, source.Attributes.Crtime, false, false, 0, true)

	doLink(t, wfs, sourceInode, "hardlink.txt")

	// The test filer returns no event, so the mount caches each name as it
	// sent it, and its cache keeps one record for a hard link's names as the
	// filer does: a create without the content empties it for both.
	for _, path := range []util.FullPath{sourcePath, "/hardlink.txt"} {
		if got := cachedLink(t, wfs, path).Content; !bytes.Equal(got, content) {
			t.Fatalf("%s has content %q after the link, want %q", path, got, content)
		}
	}
}

// A file under a remote mount that is not cached yet has no chunks and no
// content, and its remote object is keyed by its own path, which the new name
// does not resolve to. The link caches it first, so both names share local
// chunks and keep the RemoteEntry.
func TestLinkCachesARemoteOnlyFileForTheNewName(t *testing.T) {
	wfs, server := newCreateTestWFS(t)

	remote := &filer_pb.RemoteEntry{StorageName: "cloud1", RemoteMtime: 1, RemoteSize: 4096, RemoteETag: "etag"}
	attributes := &filer_pb.FuseAttributes{
		FileMode: 0o644,
		FileSize: uint64(remote.RemoteSize),
		Crtime:   1,
		Mtime:    1,
		Uid:      99,
		Gid:      100,
	}
	source := &filer_pb.Entry{Name: "remote.bin", RemoteEntry: remote, Attributes: attributes}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", source), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	chunk := &filer_pb.FileChunk{FileId: "3,01637037d6", Size: uint64(remote.RemoteSize), ModifiedTsNs: 1}
	server.cachedRemote = &filer_pb.Entry{
		Name:        source.Name,
		RemoteEntry: remote,
		Attributes:  proto.Clone(attributes).(*filer_pb.FuseAttributes),
		Chunks:      []*filer_pb.FileChunk{chunk},
	}
	sourcePath := util.FullPath("/remote.bin")
	sourceInode := wfs.inodeToPath.Lookup(sourcePath, source.Attributes.Crtime, false, false, 0, true)

	doLink(t, wfs, sourceInode, "hardlink.bin")

	for _, path := range []util.FullPath{sourcePath, "/hardlink.bin"} {
		got := cachedLink(t, wfs, path)
		if !proto.Equal(got.Remote, remote) {
			t.Fatalf("%s has remote entry %v after the link, want %v", path, got.Remote, remote)
		}
		if len(got.GetChunks()) != 1 || got.GetChunks()[0].GetFileIdString() != chunk.GetFileIdString() {
			t.Fatalf("%s has chunks %v after the link, want the cached %v", path, got.GetChunks(), chunk)
		}
	}
}

func (s *createEntryTestServer) CacheRemoteObjectToLocalCluster(ctx context.Context, req *filer_pb.CacheRemoteObjectToLocalClusterRequest) (*filer_pb.CacheRemoteObjectToLocalClusterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cachedRemote == nil {
		return nil, status.Errorf(codes.NotFound, "%s/%s", req.GetDirectory(), req.GetName())
	}
	return &filer_pb.CacheRemoteObjectToLocalClusterResponse{Entry: proto.Clone(s.cachedRemote).(*filer_pb.Entry)}, nil
}
