package shell

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fsVerifyTestVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	missingNeedles map[uint64]bool
}

func (s *fsVerifyTestVolumeServer) VolumeNeedleStatus(_ context.Context, req *volume_server_pb.VolumeNeedleStatusRequest) (*volume_server_pb.VolumeNeedleStatusResponse, error) {
	if s.missingNeedles[req.NeedleId] {
		return nil, fmt.Errorf("needle not found %d", req.NeedleId)
	}
	return &volume_server_pb.VolumeNeedleStatusResponse{NeedleId: req.NeedleId}, nil
}

type fsVerifyTestFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entry   *filer_pb.Entry
	deleted []*filer_pb.DeleteEntryRequest
}

func (s *fsVerifyTestFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if s.entry == nil {
		return nil, fmt.Errorf("no entry is found in filer store")
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: s.entry}, nil
}

func (s *fsVerifyTestFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.deleted = append(s.deleted, req)
	// mirror the real filer: an entry newer than IfNotModifiedAfter is
	// silently skipped, no error and no Error field
	if s.entry != nil && req.IfNotModifiedAfter > 0 && s.entry.Attributes.GetMtime() > req.IfNotModifiedAfter {
		return &filer_pb.DeleteEntryResponse{}, nil
	}
	s.entry = nil
	return &filer_pb.DeleteEntryResponse{}, nil
}

var fsVerifyTestPortCounter atomic.Int64

func newFsVerifyTestCommandEnv(t *testing.T, filerServer filer_pb.SeaweedFilerServer, volumeServer volume_server_pb.VolumeServerServer) (*CommandEnv, pb.ServerAddress, func()) {
	t.Helper()

	socketDir, err := os.MkdirTemp("", "swverify-")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })

	filerListener, err := net.Listen("unix", filepath.Join(socketDir, "filer.sock"))
	if err != nil {
		t.Fatalf("listen filer socket: %v", err)
	}
	filerGrpc := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(filerGrpc, filerServer)
	go func() { _ = filerGrpc.Serve(filerListener) }()

	volumeListener, err := net.Listen("unix", filepath.Join(socketDir, "volume.sock"))
	if err != nil {
		t.Fatalf("listen volume socket: %v", err)
	}
	volumeGrpc := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(volumeGrpc, volumeServer)
	go func() { _ = volumeGrpc.Serve(volumeListener) }()

	// unique ports per env: pb caches gRPC connections by address, so
	// reusing a port across envs would dial a dead cached connection
	filerPort := int(fsVerifyTestPortCounter.Add(1))
	volumePort := int(fsVerifyTestPortCounter.Add(1))
	pb.RegisterLocalGrpcSocket("127.0.0.1", filerPort, filepath.Join(socketDir, "filer.sock"))
	pb.RegisterLocalGrpcSocket("127.0.0.1", volumePort, filepath.Join(socketDir, "volume.sock"))

	cleanup := func() {
		filerGrpc.Stop()
		volumeGrpc.Stop()
		_ = filerListener.Close()
		_ = volumeListener.Close()
	}

	commandEnv := &CommandEnv{
		option: &ShellOptions{
			FilerAddress:   pb.ServerAddress(fmt.Sprintf("127.0.0.1:8888.%d", filerPort)),
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			Directory:      "/",
		},
	}
	volumeLocation := pb.NewServerAddress("127.0.0.1", 19080, volumePort)
	return commandEnv, volumeLocation, cleanup
}

func newFsVerifyTestCommand(t *testing.T, commandEnv *CommandEnv, output *bytes.Buffer, volumeLocations ...pb.ServerAddress) *commandFsVerify {
	t.Helper()
	pruneEntries := false
	metadataFromLog := false
	verbose := false
	concurrency := 0
	c := &commandFsVerify{
		env:             commandEnv,
		writer:          output,
		volumeIds:       make(map[uint32][]pb.ServerAddress),
		verbose:         &verbose,
		metadataFromLog: &metadataFromLog,
		pruneEntries:    &pruneEntries,
		concurrency:     &concurrency,
		waitChan:        make(map[string]chan struct{}),
	}
	for _, loc := range volumeLocations {
		c.volumeIds[7] = append(c.volumeIds[7], loc)
		c.waitChan[string(loc)] = make(chan struct{}, 4)
	}
	return c
}

func TestFsVerifyIsNeedleMissingError(t *testing.T) {
	if !isNeedleMissingError(fmt.Errorf("needle not found 42")) {
		t.Error("needle not found must classify as missing")
	}
	// truncated volume files surface as EOF wrapped in gRPC code Unknown
	if !isNeedleMissingError(status.Error(codes.Unknown, "EOF")) {
		t.Error("EOF must classify as missing (truncated needle data)")
	}
	// anchored matching: unrelated errors merely containing the substrings
	// must not classify as missing
	if isNeedleMissingError(status.Error(codes.Unknown, "unexpected EOF while reading trailer")) {
		t.Error("unrelated EOF-containing errors must NOT classify as missing")
	}
	if isNeedleMissingError(fmt.Errorf("rpc error: code = Unknown desc = read needle: context deadline exceeded, stream EOF")) {
		t.Error("deadline errors mentioning EOF must NOT classify as missing")
	}
	if isNeedleMissingError(status.Error(codes.NotFound, "needle not foundxyz")) {
		t.Error("malformed needle-not-found prefix must NOT classify as missing")
	}
	// new Go servers and the Rust volume server answer with code NotFound
	if !isNeedleMissingError(status.Error(codes.NotFound, "needle not found 42")) {
		t.Error("NotFound needle-not-found must classify as missing")
	}
	// a NotFound for the volume itself says nothing about the needle
	if isNeedleMissingError(status.Error(codes.NotFound, "volume 7 not found")) {
		t.Error("NotFound volume-not-found must NOT classify as missing")
	}
	if isNeedleMissingError(status.Error(codes.Unavailable, "connection refused")) {
		t.Error("transport errors must NOT classify as missing")
	}
	if isNeedleMissingError(fmt.Errorf("volume not found 7")) {
		t.Error("volume not found must NOT classify as missing (it says nothing about the needle)")
	}
	// older Go servers wrap EC's NotFoundError as code Unknown
	if !isNeedleMissingError(status.Error(codes.Unknown, "locate in local ec volume: FindNeedleFromEcx: needle not found")) {
		t.Error("EC wrapped needle-not-found must classify as missing")
	}
	if isNeedleMissingError(status.Error(codes.Unknown, "locate in local ec volume: ReadEcShardIntervals: shard 3 missing")) {
		t.Error("EC errors that do not end in needle not found must NOT classify as missing")
	}
	if isNeedleMissingError(nil) {
		t.Error("nil error must not classify as missing")
	}
}

// A needle gone from every location holding the volume means the data is lost,
// so the entry is prunable. One healthy location keeps it (reads can still
// succeed there).
func TestFsVerifyVerifyEntryPrunability(t *testing.T) {
	deadChunk := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 42}}

	t.Run("missing on all locations", func(t *testing.T) {
		commandEnv, loc, cleanup := newFsVerifyTestCommandEnv(t,
			&fsVerifyTestFilerServer{},
			&fsVerifyTestVolumeServer{missingNeedles: map[uint64]bool{42: true}})
		defer cleanup()
		c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{}, loc)

		verified, prunable := c.verifyEntry("/buckets/b/file", []*filer_pb.FileChunk{deadChunk}, atomic.NewUint64(0), &sync.WaitGroup{})
		if verified {
			t.Error("expected verification failure for missing needle")
		}
		if !prunable {
			t.Error("needle missing on the only location must be prunable")
		}
	})

	t.Run("healthy on one of two locations", func(t *testing.T) {
		commandEnv, loc, cleanup := newFsVerifyTestCommandEnv(t,
			&fsVerifyTestFilerServer{},
			&fsVerifyTestVolumeServer{})
		defer cleanup()
		c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{}, loc, loc)

		verified, prunable := c.verifyEntry("/buckets/b/file", []*filer_pb.FileChunk{deadChunk}, atomic.NewUint64(0), &sync.WaitGroup{})
		if !verified {
			t.Error("needle present on one location should not fail verification")
		}
		if prunable {
			t.Error("needle present somewhere must not be prunable")
		}
	})

	t.Run("concurrent path matches sequential", func(t *testing.T) {
		commandEnv, loc, cleanup := newFsVerifyTestCommandEnv(t,
			&fsVerifyTestFilerServer{},
			&fsVerifyTestVolumeServer{missingNeedles: map[uint64]bool{42: true}})
		defer cleanup()
		c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{}, loc)
		concurrency := 2
		c.concurrency = &concurrency

		verified, prunable := c.verifyEntry("/buckets/b/file", []*filer_pb.FileChunk{deadChunk}, atomic.NewUint64(0), &sync.WaitGroup{})
		if verified || !prunable {
			t.Errorf("concurrent verification = (%v, %v), want (false, true)", verified, prunable)
		}
	})
}

// Pruning must only delete the entry when it did not change since the
// verification pass — a re-upload fixes the file and must survive.
func TestFsVerifyPruneEntryGuards(t *testing.T) {
	filer := &fsVerifyTestFilerServer{entry: &filer_pb.Entry{
		Name: "file",
		Attributes: &filer_pb.FuseAttributes{
			Mtime: 100,
			Md5:   []byte("md5"),
		},
		Chunks: []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 42}}},
	}}
	commandEnv, _, cleanup := newFsVerifyTestCommandEnv(t, filer, &fsVerifyTestVolumeServer{})
	defer cleanup()
	c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{})

	chunks := []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 42}}}

	// unchanged entry: deleted
	pruned, err := c.pruneEntry(util.NewFullPath("/buckets/b", "file"), 100, []byte("md5"), chunks)
	if err != nil || !pruned {
		t.Fatalf("expected prune of unchanged entry, got pruned=%v err=%v", pruned, err)
	}

	// re-uploaded entry (new mtime/md5): must survive
	filer.entry = &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{Mtime: 200, Md5: []byte("new")}}
	pruned, err = c.pruneEntry(util.NewFullPath("/buckets/b", "file"), 100, []byte("md5"), chunks)
	if err != nil || pruned {
		t.Fatalf("changed entry must not be pruned, got pruned=%v err=%v", pruned, err)
	}

	// same-second rewrite: timestamps identical but the chunk set differs —
	// the new upload must survive
	filer.entry = &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{Mtime: 100, Md5: []byte("md5")},
		Chunks:     []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 999}}},
	}
	pruned, err = c.pruneEntry(util.NewFullPath("/buckets/b", "file"), 100, []byte("md5"), chunks)
	if err != nil || pruned {
		t.Fatalf("same-second rewrite must not be pruned, got pruned=%v err=%v", pruned, err)
	}

	// identical chunk set (order-insensitive): prunes
	filer.entry = &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{Mtime: 100, Md5: []byte("md5")},
		Chunks:     []*filer_pb.FileChunk{{Fid: &filer_pb.FileId{VolumeId: 7, FileKey: 42}}},
	}
	pruned, err = c.pruneEntry(util.NewFullPath("/buckets/b", "file"), 100, []byte("md5"), chunks)
	if err != nil || !pruned {
		t.Fatalf("expected prune when chunk set matches, got pruned=%v err=%v", pruned, err)
	}

	// entry already gone: no error, no delete
	filer.entry = nil
	pruned, err = c.pruneEntry(util.NewFullPath("/buckets/b", "file"), 100, nil, chunks)
	if err != nil || pruned {
		t.Fatalf("missing entry must be a no-op, got pruned=%v err=%v", pruned, err)
	}
}

// An unresolvable chunk manifest is an entry-level verification failure even
// when the raw manifest needle is healthy; raw chunks are still checked so a
// missing manifest needle can be pruned.
func TestFsVerifyManifestResolutionFailure(t *testing.T) {
	t.Run("healthy raw needle but unresolvable manifest is not verified", func(t *testing.T) {
		// IsChunkManifest forces ResolveChunkManifest to read children via
		// LookupFn; the test filer does not implement LookupVolume, so
		// resolution fails. Needle 100 is present, so verifyEntry alone
		// would report verified=true — the manifest failure must override.
		manifestChunk := &filer_pb.FileChunk{
			Fid:             &filer_pb.FileId{VolumeId: 7, FileKey: 100},
			IsChunkManifest: true,
			Size:            100,
		}
		commandEnv, loc, cleanup := newFsVerifyTestCommandEnv(t,
			&fsVerifyTestFilerServer{},
			&fsVerifyTestVolumeServer{})
		defer cleanup()
		c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{}, loc)

		verified, hasMissing := c.resolveAndVerify("/buckets/b/file",
			[]*filer_pb.FileChunk{manifestChunk}, atomic.NewUint64(0), &sync.WaitGroup{})
		if verified {
			t.Error("entry with an unresolvable manifest must not count as verified")
		}
		if hasMissing {
			t.Error("healthy raw manifest needle must not be classified as missing")
		}
	})

	t.Run("missing manifest needle is prunable", func(t *testing.T) {
		// The manifest needle itself is gone: resolution fails AND verifyEntry
		// classifies the raw needle as missing, so the entry is prunable.
		manifestChunk := &filer_pb.FileChunk{
			Fid:             &filer_pb.FileId{VolumeId: 7, FileKey: 100},
			IsChunkManifest: true,
			Size:            100,
		}
		commandEnv, loc, cleanup := newFsVerifyTestCommandEnv(t,
			&fsVerifyTestFilerServer{},
			&fsVerifyTestVolumeServer{missingNeedles: map[uint64]bool{100: true}})
		defer cleanup()
		c := newFsVerifyTestCommand(t, commandEnv, &bytes.Buffer{}, loc)

		verified, hasMissing := c.resolveAndVerify("/buckets/b/file",
			[]*filer_pb.FileChunk{manifestChunk}, atomic.NewUint64(0), &sync.WaitGroup{})
		if verified {
			t.Error("entry with a missing manifest needle must not count as verified")
		}
		if !hasMissing {
			t.Error("missing manifest needle must be classified as prunable")
		}
	})
}
