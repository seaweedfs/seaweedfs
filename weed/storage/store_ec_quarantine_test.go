package storage

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

func quarantineEcVolume(ev *erasure_coding.EcVolume) {
	for i := 0; i < IoErrorTolerance; i++ {
		ev.CheckReadWriteError(fmt.Errorf("read shard: %w", syscall.EIO))
	}
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

// deletedThenFailingEcShardPeer reports the needle deleted, then fails the stream.
type deletedThenFailingEcShardPeer struct {
	volume_server_pb.UnimplementedVolumeServerServer
}

func (p *deletedThenFailingEcShardPeer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {
	if err := stream.Send(&volume_server_pb.VolumeEcShardReadResponse{IsDeleted: true, EncodeTsNs: req.EncodeTsNs}); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "stream failed after the deleted answer")
}

// A peer's deleted answer stands even when its stream fails afterwards, so a
// later source or the quarantined shard cannot answer for a deleted needle.
func TestReadRemoteEcShardIntervalKeepsDeletionWhenTheStreamFails(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	sources := []pb.ServerAddress{
		startEcShardPeer(t, &deletedThenFailingEcShardPeer{}),
		startEcShardPeer(t, &volume_server_pb.UnimplementedVolumeServerServer{}),
	}

	_, isDeleted, err := store.readRemoteEcShardInterval(sources, 42, 7, 0, make([]byte, 64), 0, 0)
	if err != nil || !isDeleted {
		t.Errorf("read got deleted=%v err=%v, want deleted and no error", isDeleted, err)
	}
}

func ecShardMessagesFor(messages []*master_pb.VolumeEcShardInformationMessage, vid needle.VolumeId) []*master_pb.VolumeEcShardInformationMessage {
	var found []*master_pb.VolumeEcShardInformationMessage
	for _, m := range messages {
		if needle.VolumeId(m.Id) == vid {
			found = append(found, m)
		}
	}
	return found
}

// The heartbeat that quarantines an EC volume tells the master its shards are
// gone, once, instead of leaving them registered until the next full EC
// heartbeat.
func TestCollectHeartbeatReportsQuarantinedEcShardsDeletedOnce(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(7)
	_, ecVolume, _ := mountEcShardRange(t, store, vid, 0, erasure_coding.TotalShardsCount)

	if deleted := ecShardMessagesFor(store.CollectHeartbeat().DeletedEcShards, vid); len(deleted) != 0 {
		t.Fatalf("healthy ec volume reported deleted: %v", deleted)
	}

	quarantineEcVolume(ecVolume)
	deleted := ecShardMessagesFor(store.CollectHeartbeat().DeletedEcShards, vid)
	if len(deleted) != 1 {
		t.Fatalf("quarantining heartbeat carried %d deleted messages for volume %d, want 1", len(deleted), vid)
	}
	if got := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(deleted[0]).Count(); got != erasure_coding.TotalShardsCount {
		t.Errorf("deleted message names %d shards, want %d", got, erasure_coding.TotalShardsCount)
	}
	if _, _, quarantined := ecVolume.GetIoErrorState(); !quarantined {
		t.Fatal("ec volume not quarantined")
	}

	if deleted := ecShardMessagesFor(store.CollectHeartbeat().DeletedEcShards, vid); len(deleted) != 0 {
		t.Errorf("next heartbeat reported the quarantined shards deleted again: %v", deleted)
	}
}

// A shard mounted onto a quarantined EC volume is not announced, and the mount
// fails so a caller moving the shard here keeps its source copy.
func TestMountEcShardsRefusesQuarantinedEcVolume(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(7)
	last := erasure_coding.ShardId(erasure_coding.TotalShardsCount - 1)
	_, ecVolume, _ := mountEcShardRange(t, store, vid, 0, int(last))
	for len(store.NewEcShardsChan) > 0 {
		<-store.NewEcShardsChan
	}

	ecVolume.MarkIoQuarantined()
	if err := store.MountEcShards("", vid, last, ""); err == nil {
		t.Error("mount onto a quarantined ec volume succeeded")
	}
	if _, _, found := store.FindEcShard(vid, last); found {
		t.Error("the refused shard stayed loaded")
	}
	if err := store.MountEcShards("", vid, 0, ""); err == nil {
		t.Error("remount of a shard on a quarantined ec volume succeeded")
	}
	if _, _, found := store.FindEcShard(vid, 0); !found {
		t.Error("a refused remount unloaded a shard that was already mounted")
	}
	store.Locations[0].ecShardNotifyHandler("", vid, last, ecVolume)
	if pending := len(store.NewEcShardsChan); pending != 0 {
		t.Errorf("%d shard announcements queued for a quarantined ec volume, want 0", pending)
	}
}

// While a quarantined sibling holds the same shard, the result follows the
// disk the store picks for the shard: the healthy disk mounts it, the
// quarantined one refuses and leaves no copy on the healthy disk.
func TestMountEcShardsBesideAQuarantinedSibling(t *testing.T) {
	store := newTestStore(t, 2)
	const vid = needle.VolumeId(7)
	baseFileName, _ := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	sibling := store.Locations[1]
	for _, ext := range []string{".ec01", ".ecx", ".ecj", ".vif"} {
		data, err := os.ReadFile(baseFileName + ext)
		if err != nil {
			t.Fatalf("read %s: %v", ext, err)
		}
		if err := os.WriteFile(filepath.Join(sibling.Directory, filepath.Base(baseFileName)+ext), data, 0o644); err != nil {
			t.Fatalf("copy %s: %v", ext, err)
		}
	}
	quarantined, err := sibling.LoadEcShard("", vid, 1)
	if err != nil {
		t.Fatalf("load sibling shard: %v", err)
	}
	quarantined.MarkIoQuarantined()
	_, picked, err := store.LoadEcShardAcrossDisks("", vid, 1)
	if err != nil {
		t.Fatalf("pick a disk for the shard: %v", err)
	}
	if picked == 0 {
		store.Locations[0].UnloadEcShard(vid, 1)
	}

	err = store.MountEcShards("", vid, 1, "")
	if (err == nil) != (picked == 0) {
		t.Errorf("mount returned %v with disk %d picked", err, picked)
	}
	if _, healthyHolds := store.Locations[0].FindEcShard(vid, 1); (err == nil) != healthyHolds {
		t.Errorf("mount returned %v, healthy disk holds the shard: %v", err, healthyHolds)
	}
}

// The last-resort read does not depend on the master: once the quarantined
// shards are dropped from it, the location lookup fails, and the read must
// still reach the local copy.
func TestReadEcShardNeedleFallsBackToQuarantinedShardsWithoutLookup(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(7)
	_, ecVolume, n := mountEcShardRange(t, store, vid, 0, erasure_coding.TotalShardsCount)

	ecVolume.MarkIoQuarantined()
	if err := readWrittenEcNeedle(t, store, vid, n); err != nil {
		t.Fatalf("read with no master and only the quarantined disk holding the shards: %v", err)
	}
}

// Reconstruction falls back to quarantined local shards when the healthy
// sources cannot supply enough of them.
func TestReadEcShardNeedleReconstructsFromQuarantinedShards(t *testing.T) {
	store := newTestStore(t, 1)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const vid = needle.VolumeId(7)
	_, ecVolume, n := mountEcShardRange(t, store, vid, erasure_coding.ParityShardsCount, erasure_coding.TotalShardsCount)
	seedShardLocations(ecVolume, startEcShardPeer(t, &volume_server_pb.UnimplementedVolumeServerServer{}))

	ecVolume.MarkIoQuarantined()
	if err := readWrittenEcNeedle(t, store, vid, n); err != nil {
		t.Fatalf("read with the needle's shards gone and only quarantined shards to rebuild from: %v", err)
	}
}
