package storage

import (
	"bytes"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
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
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// countingEcShardPeer stands in for a peer volume server that is having a
// transient problem: every VolumeEcShardRead is counted and then held until the
// test releases it, so a stalled fan-out is observable while it is stalled.
type countingEcShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	requests atomic.Int64
	inFlight atomic.Int64
	release  chan struct{}
}

func (p *countingEcShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	p.requests.Add(1)
	p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	<-p.release
	return status.Error(codes.Unavailable, "transient failure")
}

func startCountingEcShardPeer(t *testing.T) (*countingEcShardPeer, pb.ServerAddress) {
	t.Helper()
	peer := &countingEcShardPeer{release: make(chan struct{})}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(func() {
		close(peer.release)
		srv.Stop()
	})
	return peer, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)
}

// writeEcVolumeFiles writes one needle into a volume in dir and EC-encodes it in
// place, leaving the .ec?? / .ecx / .vif set behind.
func writeEcVolumeFiles(t *testing.T, dir string, vid needle.VolumeId) (baseFileName string, n *needle.Needle) {
	t.Helper()

	v, err := NewVolume(dir, dir, "", vid, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("new volume: %v", err)
	}
	n = new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(42)
	n.Data = make([]byte, 3*erasure_coding.ErasureCodingSmallBlockSize+1234)
	rand.New(rand.NewSource(42)).Read(n.Data)
	n.Checksum = needle.NewCRC(n.Data)
	if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	baseFileName = v.DataFileName()
	v.Close()

	datSize, err := os.Stat(baseFileName + ".dat")
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	if _, err := erasure_coding.WriteEcFiles(baseFileName, erasure_coding.BackgroundECContext()); err != nil {
		t.Fatalf("write ec files: %v", err)
	}
	if err := erasure_coding.WriteSortedFileFromIdx(baseFileName, ".ecx"); err != nil {
		t.Fatalf("write .ecx: %v", err)
	}
	if err := os.WriteFile(baseFileName+".ecj", nil, 0o644); err != nil {
		t.Fatalf("write .ecj: %v", err)
	}
	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", &volume_server_pb.VolumeInfo{
		Version:     uint32(needle.GetCurrentVersion()),
		DatFileSize: datSize.Size(),
		EcShardConfig: &volume_server_pb.EcShardConfig{
			DataShards:   erasure_coding.DataShardsCount,
			ParityShards: erasure_coding.ParityShardsCount,
		},
	}); err != nil {
		t.Fatalf("save .vif: %v", err)
	}
	for _, ext := range []string{".dat", ".idx"} {
		if err := os.Remove(baseFileName + ext); err != nil {
			t.Fatalf("remove %s: %v", ext, err)
		}
	}
	return baseFileName, n
}

// mountLocalEcVolume writes an EC volume into the store's own disk and mounts
// every shard, so a recovery can be served entirely from local shards.
func mountLocalEcVolume(t *testing.T, store *Store, vid needle.VolumeId) (*erasure_coding.EcVolume, *needle.Needle) {
	t.Helper()
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	return ecVolume, n
}

// seedShardLocations points every shard of the volume at one address and marks
// the cache fresh, so a read never reaches for the master this test does not run.
func seedShardLocations(ecVolume *erasure_coding.EcVolume, addr pb.ServerAddress) {
	ecVolume.ShardLocationsLock.Lock()
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ecVolume.ShardLocations[erasure_coding.ShardId(shardId)] = []pb.ServerAddress{addr}
	}
	ecVolume.ShardLocationsRefreshTime = time.Now()
	ecVolume.ShardLocationsLock.Unlock()
}

// Recovery reconstructs a shard from the shards this server already holds, so a
// store holding the whole volume never asks a peer for one.
func TestRecoverOneRemoteEcShardIntervalUsesLocalShards(t *testing.T) {
	store := newTestStore(t, 1)
	peer, addr := startCountingEcShardPeer(t)
	ecVolume, n := mountLocalEcVolume(t, store, 7)
	seedShardLocations(ecVolume, addr)

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	shardIdToRecover, actualOffset := store.IntervalToShardIdAndOffset(intervals[0])

	got := make([]byte, intervals[0].Size)
	nRead, _, err := store.recoverOneRemoteEcShardInterval(n.Id, ecVolume, shardIdToRecover, got, actualOffset)
	if err != nil {
		t.Fatalf("recover ec shard interval: %v", err)
	}
	if nRead != len(got) {
		t.Fatalf("recovered %d bytes, want %d", nRead, len(got))
	}
	if requests := peer.requests.Load(); requests != 0 {
		t.Errorf("recovery asked peers for %d shard intervals, want 0 with every shard local", requests)
	}

	want := make([]byte, len(got))
	if err := store.readLocalEcShardInterval(ecVolume, shardIdToRecover, want, actualOffset); err != nil {
		t.Fatalf("read local ec shard %d: %v", shardIdToRecover, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovered shard %d bytes differ from the shard on disk", shardIdToRecover)
	}
}

// shardServingPeer answers VolumeEcShardRead out of an EC volume's shard files
// and counts how many shard intervals the caller asked for.
type shardServingPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	baseFileName string
	requests     atomic.Int64
}

func (p *shardServingPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	p.requests.Add(1)
	f, err := os.Open(p.baseFileName + erasure_coding.ToExt(int(req.ShardId)))
	if err != nil {
		return err
	}
	defer f.Close()
	data := make([]byte, req.Size)
	if _, err := f.ReadAt(data, req.Offset); err != nil {
		return err
	}
	return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{Data: data, EncodeTsNs: req.EncodeTsNs})
}

// Reed-Solomon needs DataShards shards, so the fan-out stops there instead of
// pulling every surviving shard off peers that may already be struggling.
func TestRecoverOneRemoteEcShardIntervalFetchesOnlyWhatItNeeds(t *testing.T) {
	baseFileName, n := writeEcVolumeFiles(t, t.TempDir(), 7)

	peer := &shardServingPeer{baseFileName: baseFileName}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	addr := pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)

	store := &Store{grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials())}
	ecVolume := &erasure_coding.EcVolume{
		VolumeId:       7,
		ShardLocations: make(map[erasure_coding.ShardId][]pb.ServerAddress),
	}
	seedShardLocations(ecVolume, addr)

	const shardIdToRecover = erasure_coding.ShardId(0)
	const offset = 0
	got := make([]byte, 4096)
	if _, _, err := store.recoverOneRemoteEcShardInterval(n.Id, ecVolume, shardIdToRecover, got, offset); err != nil {
		t.Fatalf("recover ec shard interval: %v", err)
	}
	if requests := peer.requests.Load(); requests != int64(erasure_coding.DataShardsCount) {
		t.Errorf("recovery asked peers for %d shard intervals, want %d", requests, erasure_coding.DataShardsCount)
	}

	want := make([]byte, len(got))
	f, err := os.Open(baseFileName + erasure_coding.ToExt(int(shardIdToRecover)))
	if err != nil {
		t.Fatalf("open shard %d: %v", shardIdToRecover, err)
	}
	defer f.Close()
	if _, err := f.ReadAt(want, offset); err != nil {
		t.Fatalf("read shard %d: %v", shardIdToRecover, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("recovered shard %d bytes differ from the shard on disk", shardIdToRecover)
	}
}
