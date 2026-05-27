package filersink

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

// recordingFiler is a SeaweedFilerServer stub that records the RPCs
// FilerSink.UpdateEntry triggers, so a test can assert which side of the
// sync (source vs sink) a given lookup landed on.
type recordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	lookupVolume   atomic.Int64
	lookupDirEntry atomic.Int64
	updateEntry    atomic.Int64

	dirEntry *filer_pb.Entry
}

func (r *recordingFiler) LookupVolume(_ context.Context, _ *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	r.lookupVolume.Add(1)
	return &filer_pb.LookupVolumeResponse{
		LocationsMap: map[string]*filer_pb.Locations{},
	}, nil
}

func (r *recordingFiler) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	r.lookupDirEntry.Add(1)
	return &filer_pb.LookupDirectoryEntryResponse{Entry: r.dirEntry}, nil
}

func (r *recordingFiler) UpdateEntry(_ context.Context, _ *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	r.updateEntry.Add(1)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func startRecordingFiler(t *testing.T) (*recordingFiler, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	rec := &recordingFiler{}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, rec)

	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = listener.Close()
	})

	return rec, listener.Addr().String()
}

// Regression for #9330: source-side chunk manifests in UpdateEntry must be
// resolved via the source filer's volume map, not the sink's. Two clusters
// with overlapping volume IDs would otherwise 404 on the manifest blob and
// silently drop the file's chunks. The discriminator here is which filer
// received the LookupVolume RPC — only the source should see it.
func TestUpdateEntryResolvesManifestViaSourceFiler(t *testing.T) {
	srcFiler, srcAddr := startRecordingFiler(t)
	sinkFiler, sinkAddr := startRecordingFiler(t)

	// existingEntry mtime must be older than newEntry's so UpdateEntry
	// enters the diff branch instead of the late-updates skip.
	sinkFiler.dirEntry = &filer_pb.Entry{
		Name:       "f",
		Attributes: &filer_pb.FuseAttributes{Mtime: 1},
	}

	src := &source.FilerSource{}
	if err := src.DoInitialize(srcAddr, srcAddr, "/", false); err != nil {
		t.Fatalf("source init: %v", err)
	}
	src.SetGrpcDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))

	fs := &FilerSink{
		filerSource:    src,
		grpcAddress:    sinkAddr,
		address:        sinkAddr,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		dir:            "/dst",
	}

	oldEntry := &filer_pb.Entry{
		Name:       "f",
		Attributes: &filer_pb.FuseAttributes{Mtime: 1},
	}
	newEntry := &filer_pb.Entry{
		Name:       "f",
		Attributes: &filer_pb.FuseAttributes{Mtime: 2},
		Chunks: []*filer_pb.FileChunk{{
			FileId:          "631,0babe7a0a8718478",
			Size:            100,
			IsChunkManifest: true,
		}},
	}

	_, err := fs.UpdateEntry("/dst/f", oldEntry, "/dst", newEntry, true, nil)
	if err == nil {
		t.Fatal("expected manifest resolution to fail (recording filer has no volume locations)")
	}
	if !strings.Contains(err.Error(), "compare chunks error") {
		t.Fatalf("expected compareChunks failure, got: %v", err)
	}

	if got := srcFiler.lookupVolume.Load(); got == 0 {
		t.Fatalf("source filer received no LookupVolume; sink got %d (manifest lookup wired to the wrong filer)",
			sinkFiler.lookupVolume.Load())
	}
	if got := sinkFiler.lookupVolume.Load(); got != 0 {
		t.Fatalf("sink filer should not see source-side LookupVolume, got %d", got)
	}
	if got := sinkFiler.lookupDirEntry.Load(); got == 0 {
		t.Fatal("sink should still receive the existing-entry LookupDirectoryEntry")
	}
}
