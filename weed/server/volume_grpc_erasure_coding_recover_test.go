package weed_server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// fakeMaster serves only LookupEcVolume, returning the configured holders for
// every shard of one volume. Used to drive the receiver's peer discovery.
type fakeMaster struct {
	master_pb.UnimplementedSeaweedServer
	volumeId  uint32
	locations []*master_pb.Location
}

func (m *fakeMaster) LookupEcVolume(_ context.Context, req *master_pb.LookupEcVolumeRequest) (*master_pb.LookupEcVolumeResponse, error) {
	resp := &master_pb.LookupEcVolumeResponse{VolumeId: req.VolumeId}
	if req.VolumeId != m.volumeId {
		return resp, nil
	}
	// Recovery only needs one peer holding the index; report a couple of shards
	// pointing at the holder, independent of the EC ratio.
	for shardId := 0; shardId < 2; shardId++ {
		resp.ShardIdLocations = append(resp.ShardIdLocations, &master_pb.LookupEcVolumeResponse_EcShardIdLocation{
			ShardId:   uint32(shardId),
			Locations: m.locations,
		})
	}
	return resp, nil
}

// serveGrpc registers register(s) on a fresh localhost listener and returns its
// grpc port, stopping the server on test cleanup.
func serveGrpc(t *testing.T, register func(*grpc.Server)) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	register(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().(*net.TCPAddr).Port
}

func newRecoverTestStore(t *testing.T, dir string) *storage.Store {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		storage.NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3, stats.DefaultDiskIOProbeConfig())
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-store.NewEcShardsChan:
			case <-store.NewVolumesChan:
			case <-store.DeletedVolumesChan:
			case <-store.DeletedEcShardsChan:
			case <-store.StateUpdateChan:
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		store.Close()
		close(done)
	})
	return store
}

// TestFetchEcIndexFromPeers_CopiesIndexOverGrpc stands up a source volume server
// that holds the .ecx/.ecj/.vif for a volume and verifies the receiver pulls
// them over a real CopyFile gRPC stream into its own disk, the way #10104
// recovery does when the index lives only on a peer.
func TestFetchEcIndexFromPeers_CopiesIndexOverGrpc(t *testing.T) {
	const collection = "video-recordings"
	vid := needle.VolumeId(6190)

	// Source server: has the index files on disk.
	srcDir := filepath.Join(t.TempDir(), "src")
	srcStore := newRecoverTestStore(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	ecxBytes := make([]byte, types.NeedleMapEntrySize*3)
	for i := range ecxBytes {
		ecxBytes[i] = byte(i)
	}
	if err := os.WriteFile(srcBase+".ecx", ecxBytes, 0o644); err != nil {
		t.Fatalf("write source .ecx: %v", err)
	}
	if err := os.WriteFile(srcBase+".ecj", []byte("journal"), 0o644); err != nil {
		t.Fatalf("write source .ecj: %v", err)
	}
	if err := os.WriteFile(srcBase+".vif", []byte("volinfo"), 0o644); err != nil {
		t.Fatalf("write source .vif: %v", err)
	}

	// Serve the source over a real TCP gRPC listener.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, &VolumeServer{store: srcStore})
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	grpcPort := lis.Addr().(*net.TCPAddr).Port
	peer := pb.NewServerAddress("127.0.0.1", grpcPort-10000, grpcPort)

	// Receiver server: empty disk, ready to receive the index.
	dstDir := filepath.Join(t.TempDir(), "dst")
	dstStore := newRecoverTestStore(t, dstDir)
	receiver := &VolumeServer{
		store:          dstStore,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	m := storage.EcVolumeMissingIndex{
		Collection: collection,
		VolumeId:   vid,
		IdxDir:     dstDir,
		DataDir:    dstDir,
	}
	if !receiver.fetchEcIndexFromPeers([]pb.ServerAddress{peer}, m) {
		t.Fatalf("fetchEcIndexFromPeers returned false; expected a successful copy")
	}

	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	got, err := os.ReadFile(dstBase + ".ecx")
	if err != nil {
		t.Fatalf("read copied .ecx: %v", err)
	}
	if len(got) != len(ecxBytes) {
		t.Errorf("copied .ecx size = %d, want %d", len(got), len(ecxBytes))
	}
	if _, err := os.Stat(dstBase + ".ecj"); err != nil {
		t.Errorf("copied .ecj missing: %v", err)
	}
	if _, err := os.Stat(dstBase + ".vif"); err != nil {
		t.Errorf("copied .vif missing: %v", err)
	}
}

// TestFetchEcIndexFromPeers_SkipsPeerWithoutIndex verifies the receiver moves on
// to the next peer when the first has no .ecx, and reports failure when no peer
// can serve a usable index.
func TestFetchEcIndexFromPeers_SkipsPeerWithoutIndex(t *testing.T) {
	const collection = "video-recordings"
	vid := needle.VolumeId(6191)

	// Source server with NO index files for this volume.
	srcDir := filepath.Join(t.TempDir(), "src")
	srcStore := newRecoverTestStore(t, srcDir)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(grpcServer, &VolumeServer{store: srcStore})
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	grpcPort := lis.Addr().(*net.TCPAddr).Port
	peer := pb.NewServerAddress("127.0.0.1", grpcPort-10000, grpcPort)

	dstDir := filepath.Join(t.TempDir(), "dst")
	dstStore := newRecoverTestStore(t, dstDir)
	receiver := &VolumeServer{
		store:          dstStore,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	m := storage.EcVolumeMissingIndex{Collection: collection, VolumeId: vid, IdxDir: dstDir, DataDir: dstDir}
	if receiver.fetchEcIndexFromPeers([]pb.ServerAddress{peer}, m) {
		t.Fatalf("fetchEcIndexFromPeers should fail when no peer has the index")
	}

	// No stub .ecx must be left behind.
	dstBase := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
	if _, err := os.Stat(dstBase + ".ecx"); !os.IsNotExist(err) {
		t.Errorf("a .ecx stub was left behind; stat err = %v", err)
	}
}

// TestVolumeEcShardsMount_RecoverMissingIndex drives the full on-demand path:
// VolumeEcShardsMount with recover_missing_index (volume_id 0 = recover every
// orphan on this server, as ec.rebuild broadcasts it) looks up holders via a
// (fake) master, fetches the missing .ecx/.ecj/.vif from the holding peer over
// gRPC, and mounts the previously-orphaned on-disk shards (issue #10104).
func TestVolumeEcShardsMount_RecoverMissingIndex(t *testing.T) {
	const collection = "video-recordings"
	vid := needle.VolumeId(6190)

	// Holder peer: serves the index files for the volume.
	srcDir := filepath.Join(t.TempDir(), "src")
	srcStore := newRecoverTestStore(t, srcDir)
	srcBase := erasure_coding.EcShardFileName(collection, srcDir, int(vid))
	if err := os.WriteFile(srcBase+".ecx", make([]byte, types.NeedleMapEntrySize*4), 0o644); err != nil {
		t.Fatalf("write source .ecx: %v", err)
	}
	if err := os.WriteFile(srcBase+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write source .ecj: %v", err)
	}
	if err := os.WriteFile(srcBase+".vif", []byte("volinfo"), 0o644); err != nil {
		t.Fatalf("write source .vif: %v", err)
	}
	srcGrpcPort := serveGrpc(t, func(s *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(s, &VolumeServer{store: srcStore})
	})

	// Fake master points every shard at the holder peer.
	masterGrpcPort := serveGrpc(t, func(s *grpc.Server) {
		master_pb.RegisterSeaweedServer(s, &fakeMaster{
			volumeId:  uint32(vid),
			locations: []*master_pb.Location{{Url: "127.0.0.1:1", GrpcPort: uint32(srcGrpcPort)}},
		})
	})

	// Receiver: holds orphan shard files on disk, no .ecx anywhere.
	dstDir := filepath.Join(t.TempDir(), "dst")
	dstStore := newRecoverTestStore(t, dstDir)
	const shardSize = 1 << 20
	for _, sid := range []erasure_coding.ShardId{0, 5} {
		base := erasure_coding.EcShardFileName(collection, dstDir, int(vid))
		f, err := os.Create(base + erasure_coding.ToExt(int(sid)))
		if err != nil {
			t.Fatalf("create shard %d: %v", sid, err)
		}
		if err := f.Truncate(shardSize); err != nil {
			f.Close()
			t.Fatalf("truncate shard %d: %v", sid, err)
		}
		f.Close()
	}

	receiver := &VolumeServer{
		store:          dstStore,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	receiver.setCurrentMaster(pb.NewServerAddress("127.0.0.1", masterGrpcPort-10000, masterGrpcPort))

	if _, found := dstStore.FindEcVolume(vid); found {
		t.Fatalf("EC volume %d unexpectedly mounted before recovery", vid)
	}

	// volume_id 0: the receiver discovers the orphan (6190) on disk itself, even
	// though the request names no volume and the master never registered it here.
	if _, err := receiver.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
		RecoverMissingIndex: true,
	}); err != nil {
		t.Fatalf("VolumeEcShardsMount with recover_missing_index: %v", err)
	}

	ev, found := dstStore.FindEcVolume(vid)
	if !found {
		t.Fatalf("EC volume %d not mounted after recovery", vid)
	}
	for _, sid := range []erasure_coding.ShardId{0, 5} {
		if _, ok := ev.FindEcVolumeShard(sid); !ok {
			t.Errorf("shard %d.%d not registered after recovery", vid, sid)
		}
	}
}
