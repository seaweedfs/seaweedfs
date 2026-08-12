package ec_balance

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// The dedup phase deletes a shard it believes is duplicated elsewhere. It picks
// its victims from the master's topology, and the topology can name a location
// that holds nothing: a volume server answering "not found ec volume id N" for
// a shard the master lists is the observable form of that. Deleting on the
// strength of a phantom peer removes the only real copy.
//
// The move phase already refuses to work on trust — it verifies the shard
// registered on the destination before removing the source. These tests hold
// dedup to the same standard, because it is the more dangerous of the two: it
// deletes without copying anything first.

const (
	dedupTestVolumeID   = uint32(42561)
	dedupTestCollection = "pm-itatiaiucu-01"
)

func dedupParams(sourceNode, keepNode string, shardID uint32) *worker_pb.TaskParams {
	// Dedup is signalled by source and target being the same node and disk.
	loc := &worker_pb.TaskSource{Node: sourceNode, DiskId: 0, ShardIds: []uint32{shardID}}
	return &worker_pb.TaskParams{
		VolumeId:   dedupTestVolumeID,
		Collection: dedupTestCollection,
		Sources:    []*worker_pb.TaskSource{loc},
		Targets:    []*worker_pb.TaskTarget{{Node: sourceNode, DiskId: 0, ShardIds: []uint32{shardID}}},
		// The dedup branch is only reachable when EC params are present; without
		// them the job silently takes the copy-and-move path instead.
		TaskParams: &worker_pb.TaskParams_EcBalanceParams{
			EcBalanceParams: &worker_pb.EcBalanceTaskParams{DedupKeepNode: keepNode},
		},
	}
}

func newDedupTask() *ECBalanceTask {
	return NewECBalanceTask("dedup-test", dedupTestVolumeID, dedupTestCollection,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// The failure that cost this cluster shards: the topology claims shard 0 lives
// on two nodes, only one of them actually has it, and dedup is pointed at the
// one that does. Nothing else in the cluster holds shard 0, so deleting it is
// unrecoverable — and no copy was made first, because dedup never makes one.
func TestDedupRefusesToDeleteTheOnlyRealCopy(t *testing.T) {
	holder := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)
	// The peer the topology believes also has shard 0. Its disks are empty,
	// exactly like a server answering "not found ec volume id".
	phantom := startFakeEcVolumeServer(t, dedupTestVolumeID)

	if !holder.has(dedupTestVolumeID, 0) {
		t.Fatal("precondition: holder should start with shard 0")
	}
	if phantom.has(dedupTestVolumeID, 0) {
		t.Fatal("precondition: phantom should hold nothing")
	}

	err := newDedupTask().Execute(context.Background(), dedupParams(holder.address(), phantom.address(), 0))

	if holder.has(dedupTestVolumeID, 0) {
		return // refused, or verified and backed off — either is safe
	}
	t.Fatalf("dedup deleted the only copy of shard %d.0 (deleted=%v, err=%v); "+
		"the surviving copy was never confirmed to exist",
		dedupTestVolumeID, holder.deletedShards(), err)
}

// A shard genuinely present on two servers is safe to thin out: one copy is
// redundant and removing it loses nothing. Dedup has to keep working here, or
// the guard above has simply disabled the feature.
func TestDedupRemovesAGenuineDuplicate(t *testing.T) {
	victim := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)
	survivor := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)

	if err := newDedupTask().Execute(context.Background(), dedupParams(victim.address(), survivor.address(), 0)); err != nil {
		t.Fatalf("dedup of a real duplicate should succeed, got %v", err)
	}
	if victim.has(dedupTestVolumeID, 0) {
		t.Error("dedup left the duplicate in place; capacity is not reclaimed")
	}
	if !survivor.has(dedupTestVolumeID, 0) {
		t.Error("dedup removed the surviving copy instead of the duplicate")
	}
}

// An unreachable peer is not evidence of a surviving copy. Treating a failed
// query as "the other side has it" is how a network blip becomes data loss.
func TestDedupRefusesWhenThePeerCannotBeQueried(t *testing.T) {
	holder := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)
	unreachable := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)
	unreachable.stop() // peer down; its inventory is unknown, not empty

	err := newDedupTask().Execute(context.Background(), dedupParams(holder.address(), unreachable.address(), 0))

	if holder.has(dedupTestVolumeID, 0) {
		return // kept the shard while the peer's state is unknown
	}
	t.Fatalf("dedup deleted shard %d.0 while the peer was unreachable (deleted=%v, err=%v)",
		dedupTestVolumeID, holder.deletedShards(), err)
}

// Volume ids are allocated cluster-wide, but the inventory RPC is keyed by
// volume id alone, so a server holding the same number for a different
// collection answers "yes, I have that shard" to a question about this one.
// Accepting that would delete the last real copy on the strength of an
// unrelated volume.
func TestDedupRefusesWhenTheKeepNodeHoldsAnotherCollection(t *testing.T) {
	holder := startFakeEcVolumeServer(t, dedupTestVolumeID, 0)
	// Same volume id, different collection: a plausible-looking but wrong match.
	impostor := startFakeEcVolumeServerInCollection(t, "morro-agudo-01", dedupTestVolumeID, 0)

	err := newDedupTask().Execute(context.Background(), dedupParams(holder.address(), impostor.address(), 0))

	if holder.has(dedupTestVolumeID, 0) {
		return // refused: the keep node's shard belongs to another collection
	}
	t.Fatalf("dedup deleted shard %d.0 after matching another collection's volume (deleted=%v, err=%v)",
		dedupTestVolumeID, holder.deletedShards(), err)
}
