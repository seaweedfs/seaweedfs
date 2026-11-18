package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// startEcCopySource serves srcDir's EC files over CopyFile and returns the
// address a VolumeEcShardsCopy names as its source.
func startEcCopySource(t *testing.T, srcDir string) string {
	t.Helper()
	return serveEcCopySource(t, &VolumeServer{store: newRecoverTestStore(t, srcDir)})
}

func serveEcCopySource(t *testing.T, srv volume_server_pb.VolumeServerServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, srv)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	port := lis.Addr().(*net.TCPAddr).Port
	return string(pb.NewServerAddress("127.0.0.1", port-10000, port))
}

// failingExtCopySource counts CopyFile requests per extension and fails the
// one for failExt, running onFail first.
type failingExtCopySource struct {
	*VolumeServer
	failExt   string
	onFail    func() error
	mu        sync.Mutex
	requested map[string]int
}

func (s *failingExtCopySource) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {
	s.mu.Lock()
	if s.requested == nil {
		s.requested = make(map[string]int)
	}
	s.requested[req.Ext]++
	s.mu.Unlock()
	if req.Ext == s.failExt {
		if s.onFail != nil {
			if err := s.onFail(); err != nil {
				return status.Errorf(codes.Internal, "fixture: %v", err)
			}
		}
		return status.Errorf(codes.Unavailable, "injected %s failure", req.Ext)
	}
	return s.VolumeServer.CopyFile(req, stream)
}

func (s *failingExtCopySource) requests(ext string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requested[ext]
}

func newEcCopyReceiver(t *testing.T, dstDir string) *VolumeServer {
	t.Helper()
	return &VolumeServer{
		store:                         newRecoverTestStore(t, dstDir),
		grpcDialOption:                grpc.WithTransportCredentials(insecure.NewCredentials()),
		AllowUntrustedRemoteEndpoints: true,
	}
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// A source whose .ecx is 0 bytes must fail the copy without stranding a shard,
// index or sidecar on the destination, and without touching what an existing
// holder of the same volume already has there.
// waitForLockWaiter waits until a goroutine running handler is blocked on a
// mutex. The test holds the lifecycle lock and nothing else, so that is the
// lock the handler waits for.
func waitForLockWaiter(t *testing.T, handler string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(g, "sync.(*Mutex).lockSlow") && strings.Contains(g, handler) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s never waited for the lifecycle lock", handler)
}

func TestVolumeEcShardsCopy_EmptySourceEcxLeavesNoFiles(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(57)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", nil)
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".ecj", nil)
	writeFile(t, srcBase+".vif", []byte("{}"))

	req := &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyEcjFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	}

	t.Run("fresh destination", func(t *testing.T) {
		dstDir := filepath.Join(t.TempDir(), "dst")
		receiver := newEcCopyReceiver(t, dstDir)
		before := listDir(t, dstDir)

		_, err := receiver.VolumeEcShardsCopy(context.Background(), req)
		if err == nil || !strings.Contains(err.Error(), "source .ecx is 0 bytes") {
			t.Fatalf("VolumeEcShardsCopy err = %v, want source .ecx is 0 bytes", err)
		}
		if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("destination files after failed copy = %v, want %v", after, before)
		}
	})

	t.Run("existing holder", func(t *testing.T) {
		dstDir := filepath.Join(t.TempDir(), "dst")
		receiver := newEcCopyReceiver(t, dstDir)
		dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
		ecx := make([]byte, types.NeedleMapEntrySize)
		ecx[0] = 7
		ec05 := bytes.Repeat([]byte{5}, 4096)
		writeFile(t, dstBase+".ecx", ecx)
		writeFile(t, dstBase+".ec05", ec05)
		before := listDir(t, dstDir)

		if _, err := receiver.VolumeEcShardsCopy(context.Background(), req); err == nil {
			t.Fatalf("VolumeEcShardsCopy succeeded against a 0-byte source .ecx")
		}
		if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
			t.Fatalf("destination files after failed copy = %v, want %v", after, before)
		}
		if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
			t.Errorf("pre-existing .ecx changed: %x, want %x", got, ecx)
		}
		if got, _ := os.ReadFile(dstBase + ".ec05"); !bytes.Equal(got, ec05) {
			t.Errorf("pre-existing .ec05 changed")
		}
	})
}

// A copy failing after some shards landed removes exactly those shards and
// keeps the ones the destination held before the call, its index included.
func TestVolumeEcShardsCopy_FailedShardRemovesCopiedShards(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(58)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	srcEcx := make([]byte, types.NeedleMapEntrySize)
	srcEcx[0] = 9
	writeFile(t, srcBase+".ecx", srcEcx)
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".ec05", bytes.Repeat([]byte{2}, 4096))

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	ecx := make([]byte, types.NeedleMapEntrySize)
	ecx[0] = 7
	writeFile(t, dstBase+".ecx", ecx)
	ec05 := bytes.Repeat([]byte{5}, 4096)
	writeFile(t, dstBase+".ec05", ec05)
	ec01 := bytes.Repeat([]byte{6}, 4096)
	writeFile(t, dstBase+".ec01", ec01)
	before := listDir(t, dstDir)

	// Shard 0 copies, shard 5 replaces the existing holder's copy, shard 1 is
	// held here but missing on the source and fails the request.
	_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0, 5, 1},
		CopyEcxFile:    true,
		SourceDataNode: source,
	})
	if err == nil || !strings.Contains(err.Error(), ".ec01") {
		t.Fatalf("VolumeEcShardsCopy error = %v, want the .ec01 transfer to fail", err)
	}
	if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("destination files after failed copy = %v, want %v", after, before)
	}
	if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
		t.Errorf("pre-existing .ecx changed: %x, want %x", got, ecx)
	}
	if got, _ := os.ReadFile(dstBase + ".ec05"); !bytes.Equal(got, ec05) {
		t.Errorf("pre-existing .ec05 changed")
	}
	if got, _ := os.ReadFile(dstBase + ".ec01"); !bytes.Equal(got, ec01) {
		t.Errorf("pre-existing .ec01 changed by its failed transfer")
	}
}

// A copied index replaces an existing holder's in place, so a mounted
// EcVolume's open handle reads the source's index, tombstones included.
func TestVolumeEcShardsCopy_ExistingEcxUpdatedInPlace(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(59)

	srcDir := filepath.Join(t.TempDir(), "src")
	source := startEcCopySource(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	srcEcx := bytes.Repeat([]byte{9}, 2*types.NeedleMapEntrySize)
	writeFile(t, srcBase+".ecx", srcEcx)
	srcEc00 := bytes.Repeat([]byte{1}, 4096)
	writeFile(t, srcBase+".ec00", srcEc00)
	srcVif := []byte(`{"version":3}`)
	writeFile(t, srcBase+".vif", srcVif)

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, 2*types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec00", bytes.Repeat([]byte{3}, 1024))
	held, err := os.Open(dstBase + ".ecx")
	if err != nil {
		t.Fatalf("open .ecx: %v", err)
	}
	defer held.Close()

	if _, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	}); err != nil {
		t.Fatalf("VolumeEcShardsCopy: %v", err)
	}
	got := make([]byte, len(srcEcx))
	if _, err := held.ReadAt(got, 0); err != nil {
		t.Fatalf("read open .ecx: %v", err)
	}
	if !bytes.Equal(got, srcEcx) {
		t.Fatalf("open .ecx handle reads %x after the copy, want the source index %x", got, srcEcx)
	}
	if got, _ := os.ReadFile(dstBase + ".ec00"); !bytes.Equal(got, srcEc00) {
		t.Errorf("existing .ec00 not replaced by the source's")
	}
	if got, _ := os.ReadFile(dstBase + ".vif"); !bytes.Equal(got, srcVif) {
		t.Errorf("copied .vif = %s, want %s", got, srcVif)
	}
	if _, err := os.Stat(dstBase + erasure_coding.BitrotSidecarExt); !os.IsNotExist(err) {
		t.Errorf("a source without .ecsum left one here: %v", err)
	}
	for _, name := range listDir(t, dstDir) {
		if strings.HasSuffix(name, ".copying") {
			t.Errorf("staged file %s left behind", name)
		}
	}
}

// A shard listed twice is copied once, and replacing one already held here
// still succeeds.
func TestVolumeEcShardsCopy_DuplicateShardIdCopiedOnce(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(62)

	srcDir := filepath.Join(t.TempDir(), "src")
	src := &failingExtCopySource{VolumeServer: &VolumeServer{store: newRecoverTestStore(t, srcDir)}}
	source := serveEcCopySource(t, src)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", bytes.Repeat([]byte{9}, types.NeedleMapEntrySize))
	srcEc05 := bytes.Repeat([]byte{2}, 4096)
	writeFile(t, srcBase+".ec05", srcEc05)

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec05", bytes.Repeat([]byte{5}, 4096))

	if _, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{5, 5},
		CopyEcxFile:    true,
		SourceDataNode: source,
	}); err != nil {
		t.Fatalf("VolumeEcShardsCopy: %v", err)
	}
	if got, _ := os.ReadFile(dstBase + ".ec05"); !bytes.Equal(got, srcEc05) {
		t.Errorf("existing .ec05 not replaced by the source's")
	}
	if n := src.requests(".ec05"); n != 1 {
		t.Errorf("source served .ec05 %d times, want once", n)
	}
}

// A copy failing after the .vif transferred keeps the existing holder's .vif,
// which must stay paired with the index the failed copy left in place.
func TestVolumeEcShardsCopy_FailedSidecarKeepsExistingVif(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(60)

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))

	srcVif := []byte(`{"version":3}`)
	stagedVif := make(chan []byte, 1)
	srcDir := filepath.Join(t.TempDir(), "src")
	src := &failingExtCopySource{
		VolumeServer: &VolumeServer{store: newRecoverTestStore(t, srcDir)},
		failExt:      erasure_coding.BitrotSidecarExt,
		onFail: func() error {
			data, err := os.ReadFile(dstBase + ".vif.copying")
			stagedVif <- data
			return err
		},
	}
	source := serveEcCopySource(t, src)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", bytes.Repeat([]byte{9}, types.NeedleMapEntrySize))
	writeFile(t, srcBase+".ec00", bytes.Repeat([]byte{1}, 4096))
	writeFile(t, srcBase+".vif", srcVif)
	writeFile(t, srcBase+erasure_coding.BitrotSidecarExt, []byte("sum"))

	ecx := make([]byte, types.NeedleMapEntrySize)
	writeFile(t, dstBase+".ecx", ecx)
	vif := []byte(`{"version":2}`)
	writeFile(t, dstBase+".vif", vif)
	before := listDir(t, dstDir)

	_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyVifFile:    true,
		CopyEcsumFile:  true,
		SourceDataNode: source,
	})
	if err == nil || !strings.Contains(err.Error(), "injected "+erasure_coding.BitrotSidecarExt+" failure") {
		t.Fatalf("VolumeEcShardsCopy error = %v, want the %s transfer to fail", err, erasure_coding.BitrotSidecarExt)
	}
	select {
	case got := <-stagedVif:
		if !bytes.Equal(got, srcVif) {
			t.Fatalf("staged .vif at the %s failure = %q, want the source's %q", erasure_coding.BitrotSidecarExt, got, srcVif)
		}
	default:
		t.Fatalf("the %s request never reached the source", erasure_coding.BitrotSidecarExt)
	}
	if after := listDir(t, dstDir); strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("destination files after failed copy = %v, want %v", after, before)
	}
	if got, _ := os.ReadFile(dstBase + ".vif"); !bytes.Equal(got, vif) {
		t.Errorf("pre-existing .vif changed to %s, want %s", got, vif)
	}
	if got, _ := os.ReadFile(dstBase + ".ecx"); !bytes.Equal(got, ecx) {
		t.Errorf("pre-existing .ecx changed")
	}
}

// A mount waits out an in-flight copy of the same volume, whose failure cleanup
// would remove a shard mounted meanwhile.
func TestVolumeEcShardsMount_WaitsForInFlightCopy(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(63)
	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec00", bytes.Repeat([]byte{1}, 4096))

	lifecycle := receiver.store.EcLifecycleLock(vid)
	lifecycle.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := receiver.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			ShardIds:   []uint32{0},
		})
		done <- err
	}()
	waitForLockWaiter(t, "VolumeEcShardsMount")
	_, _, mountedEarly := receiver.store.FindEcShard(vid, 0)
	lifecycle.Unlock()
	if mountedEarly {
		t.Fatal("mount ran while a copy of the same volume held its lifecycle lock")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("mount: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("mount did not finish after the copy released the lifecycle lock")
	}
	if _, _, found := receiver.store.FindEcShard(vid, 0); !found {
		t.Fatal("shard 0 not mounted after the copy released the lifecycle lock")
	}
}

// A full teardown waits out an in-flight copy of the same volume, which would
// otherwise recreate the files after the teardown is acknowledged.
func TestVolumeEcShardsDelete_FullTeardownWaitsForInFlightCopy(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(64)
	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec00", bytes.Repeat([]byte{1}, 4096))

	lifecycle := receiver.store.EcLifecycleLock(vid)
	lifecycle.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := receiver.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:     uint32(vid),
			Collection:   collection,
			FullTeardown: true,
		})
		done <- err
	}()
	waitForLockWaiter(t, "VolumeEcShardsDelete")
	_, statErr := os.Stat(dstBase + ".ecx")
	lifecycle.Unlock()
	if statErr != nil {
		t.Fatalf("full teardown ran while a copy of the same volume held its lifecycle lock: %v", statErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("full teardown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("full teardown did not finish after the copy released the lifecycle lock")
	}
	if _, err := os.Stat(dstBase + ".ecx"); !os.IsNotExist(err) {
		t.Fatalf(".ecx after the teardown: %v, want it removed", err)
	}
}

// A shard another writer puts here while a copy is in flight survives the
// copy's failure cleanup.
func TestVolumeEcShardsCopy_FailedCopyKeepsShardWrittenMeanwhile(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(65)

	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	received := bytes.Repeat([]byte{7}, 4096)
	sent := bytes.Repeat([]byte{1}, 4096)

	srcDir := filepath.Join(t.TempDir(), "src")
	src := &failingExtCopySource{
		VolumeServer: &VolumeServer{store: newRecoverTestStore(t, srcDir)},
		failExt:      ".ec01",
		onFail: func() error {
			if staged, err := os.ReadFile(dstBase + ".ec00.copying"); err != nil || !bytes.Equal(staged, sent) {
				return fmt.Errorf(".ec00 not staged before .ec01: %d bytes, %v", len(staged), err)
			}
			return os.WriteFile(dstBase+".ec00", received, 0o644)
		},
	}
	source := serveEcCopySource(t, src)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	writeFile(t, srcBase+".ecx", bytes.Repeat([]byte{9}, types.NeedleMapEntrySize))
	writeFile(t, srcBase+".ec00", sent)
	writeFile(t, srcBase+".ec01", bytes.Repeat([]byte{2}, 4096))

	_, err := receiver.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       uint32(vid),
		Collection:     collection,
		ShardIds:       []uint32{0, 1},
		CopyEcxFile:    true,
		SourceDataNode: source,
	})
	if err == nil || !strings.Contains(err.Error(), "injected .ec01 failure") {
		t.Fatalf("VolumeEcShardsCopy error = %v, want the .ec01 transfer to fail", err)
	}
	if got, err := os.ReadFile(dstBase + ".ec00"); err != nil || !bytes.Equal(got, received) {
		t.Fatalf(".ec00 written during the copy = %d bytes, %v; want it kept", len(got), err)
	}
}

// A rebuild waits out an in-flight copy of the same volume, which would
// otherwise overwrite the index the rebuild folded tombstones into.
func TestVolumeEcShardsRebuild_WaitsForInFlightCopy(t *testing.T) {
	vid := needle.VolumeId(66)
	receiver := newEcCopyReceiver(t, filepath.Join(t.TempDir(), "dst"))
	lifecycle := receiver.store.EcLifecycleLock(vid)
	lifecycle.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = receiver.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   uint32(vid),
			Collection: "col",
		})
	}()
	waitForLockWaiter(t, "VolumeEcShardsRebuild")
	select {
	case <-done:
		lifecycle.Unlock()
		t.Fatal("rebuild ran while a copy of the same volume held its lifecycle lock")
	default:
	}
	lifecycle.Unlock()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("rebuild did not finish after the copy released the lifecycle lock")
	}
}

// A shard delete waits out an in-flight copy of the same volume, whose staged
// shards the delete's last-shard check cannot see.
func TestVolumeEcShardsDelete_WaitsForInFlightCopy(t *testing.T) {
	const collection = "col"
	vid := needle.VolumeId(67)
	dstDir := filepath.Join(t.TempDir(), "dst")
	receiver := newEcCopyReceiver(t, dstDir)
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	writeFile(t, dstBase+".ecx", make([]byte, types.NeedleMapEntrySize))
	writeFile(t, dstBase+".ec00", bytes.Repeat([]byte{1}, 4096))

	lifecycle := receiver.store.EcLifecycleLock(vid)
	lifecycle.Lock()
	done := make(chan error, 1)
	go func() {
		_, err := receiver.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			ShardIds:   []uint32{0},
		})
		done <- err
	}()
	waitForLockWaiter(t, "VolumeEcShardsDelete")
	_, statErr := os.Stat(dstBase + ".ec00")
	lifecycle.Unlock()
	if statErr != nil {
		t.Fatalf("shard deleted while a copy of the same volume held its lifecycle lock: %v", statErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shard delete: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shard delete did not finish after the copy released the lifecycle lock")
	}
	if _, err := os.Stat(dstBase + ".ec00"); !os.IsNotExist(err) {
		t.Fatalf(".ec00 after the delete: %v, want it removed", err)
	}
}
