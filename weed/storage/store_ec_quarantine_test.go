package storage

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func startEcShardPeer(t *testing.T, peer volume_server_pb.VolumeServerServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(srv, peer)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return pb.NewServerAddressWithGrpcPort("127.0.0.1:1", lis.Addr().(*net.TCPAddr).Port)
}

// mountEcShardRange mounts shards [from, to) of an EC volume written into the
// store's first disk.
func mountEcShardRange(t *testing.T, store *Store, vid needle.VolumeId, from, to int) (baseFileName string, ecVolume *erasure_coding.EcVolume, n *needle.Needle) {
	t.Helper()
	baseFileName, n = writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	for shardId := from; shardId < to; shardId++ {
		if err := store.MountEcShards("", vid, erasure_coding.ShardId(shardId), ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	return baseFileName, ecVolume, n
}

func readWrittenEcNeedle(t *testing.T, store *Store, vid needle.VolumeId, want *needle.Needle) error {
	t.Helper()
	got := new(needle.Needle)
	got.Id = want.Id
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		return err
	}
	if !bytes.Equal(got.Data, want.Data) {
		return fmt.Errorf("needle %s data differs from what was written", want.Id)
	}
	return nil
}

// A quarantined disk is not read while a peer can serve the shard.
func TestReadEcShardNeedleSkipsQuarantinedLocalShards(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(7)
	baseFileName, ecVolume, n := mountEcShardRange(t, store, vid, 0, erasure_coding.TotalShardsCount)

	peer := &shardServingPeer{baseFileName: baseFileName}
	seedShardLocations(ecVolume, startEcShardPeer(t, peer))

	if err := readWrittenEcNeedle(t, store, vid, n); err != nil {
		t.Fatalf("read before quarantine: %v", err)
	}
	if requests := peer.requests.Load(); requests != 0 {
		t.Fatalf("healthy local shards sent %d reads to the peer, want 0", requests)
	}

	ecVolume.MarkIoQuarantined()
	if err := readWrittenEcNeedle(t, store, vid, n); err != nil {
		t.Fatalf("read after quarantine: %v", err)
	}
	if peer.requests.Load() == 0 {
		t.Error("the needle was read from the quarantined disk while a peer held its shards")
	}
}

// When no healthy copy answers, the quarantined shard is still read rather than
// failing the needle.
func TestReadEcShardNeedleFallsBackToQuarantinedShards(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(7)
	_, ecVolume, n := mountEcShardRange(t, store, vid, 0, erasure_coding.TotalShardsCount)
	seedShardLocations(ecVolume, startEcShardPeer(t, &volume_server_pb.UnimplementedVolumeServerServer{}))

	ecVolume.MarkIoQuarantined()
	if err := readWrittenEcNeedle(t, store, vid, n); err != nil {
		t.Fatalf("read with only the quarantined disk holding the shards: %v", err)
	}
}
