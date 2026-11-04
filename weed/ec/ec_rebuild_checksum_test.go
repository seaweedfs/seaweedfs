package ec

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ecRebuildFakeServer adds the checksum scrub and the rebuild to the balance
// fake: copied shards sit on disk unmounted, and a rebuild regenerates every
// shard that is neither mounted nor copied, plus the mounted ones that fail
// the checksum.
type ecRebuildFakeServer struct {
	*ecBalanceFakeServer
	corrupt     map[uint32]bool // shard id -> fails the checksum
	copied      map[uint32]bool
	failUnmount bool
	failScrub   bool
	scrubs      int
	rebuilds    int
	copiedIds   []uint32
	deleted     []uint32
}

func (f *ecRebuildFakeServer) ScrubEcVolume(ctx context.Context, req *volume_server_pb.ScrubEcVolumeRequest) (*volume_server_pb.ScrubEcVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scrubs++
	if f.failScrub {
		return nil, fmt.Errorf("scrub refused")
	}
	resp := &volume_server_pb.ScrubEcVolumeResponse{}
	for _, vid := range req.VolumeIds {
		for sid := range f.shards[vid] {
			if f.corrupt[sid] {
				resp.BrokenShardInfos = append(resp.BrokenShardInfos, &volume_server_pb.EcShardInfo{VolumeId: vid, ShardId: sid})
			}
		}
	}
	return resp, nil
}

func (f *ecRebuildFakeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		f.copied[sid] = true
		f.copiedIds = append(f.copiedIds, sid)
	}
	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

func (f *ecRebuildFakeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rebuilds++
	resp := &volume_server_pb.VolumeEcShardsRebuildResponse{}
	for sid := uint32(0); sid < erasure_coding.TotalShardsCount; sid++ {
		mounted := f.shards[req.VolumeId][sid]
		if (mounted || f.copied[sid]) && !f.corrupt[sid] {
			continue
		}
		delete(f.corrupt, sid)
		resp.RebuiltShardIds = append(resp.RebuiltShardIds, sid)
	}
	return resp, nil
}

func (f *ecRebuildFakeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {
	if f.failUnmount {
		return nil, fmt.Errorf("unmount refused")
	}
	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (f *ecRebuildFakeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sid := range req.ShardIds {
		delete(f.shards[req.VolumeId], sid)
		delete(f.copied, sid)
		f.deleted = append(f.deleted, sid)
	}
	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func startEcRebuildFakeServer(t *testing.T, vid uint32, present []uint32, corrupt ...uint32) *ecRebuildFakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &ecRebuildFakeServer{
		ecBalanceFakeServer: &ecBalanceFakeServer{
			shards:   map[uint32]map[uint32]bool{vid: {}},
			grpcAddr: listener.Addr().String(),
		},
		corrupt: map[uint32]bool{},
		copied:  map[uint32]bool{},
	}
	for _, sid := range present {
		f.shards[vid][sid] = true
	}
	for _, sid := range corrupt {
		f.corrupt[sid] = true
	}
	server := grpc.NewServer()
	volume_server_pb.RegisterVolumeServerServer(server, f)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return f
}

func shardRange(from, to uint32) (ids []uint32) {
	for sid := from; sid < to; sid++ {
		ids = append(ids, sid)
	}
	return ids
}

func runChecksumRebuild(t *testing.T, apply, verifyChecksums bool, servers ...*ecRebuildFakeServer) string {
	t.Helper()
	out, err := runChecksumRebuildLocked(t, apply, verifyChecksums, nil, servers...)
	if err != nil {
		t.Fatalf("rebuild: %v\n%s", err, out)
	}
	return out
}

func runChecksumRebuildLocked(t *testing.T, apply, verifyChecksums bool, isLocked func() bool, servers ...*ecRebuildFakeServer) (string, error) {
	t.Helper()
	var balanceServers []*ecBalanceFakeServer
	for _, f := range servers {
		balanceServers = append(balanceServers, f.ecBalanceFakeServer)
	}
	env := &Env{
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		FetchTopology:  fetchEcBalanceTopology(balanceServers),
		IsLocked:       isLocked,
	}
	ecNodes, _, err := CollectEcNodes(env, types.HardDriveType)
	if err != nil {
		t.Fatalf("collect ec nodes: %v", err)
	}
	var out bytes.Buffer
	err = RebuildEcVolumes(env, ecNodes, &out, []string{"c1"}, nil, types.HardDriveType, 1, apply, verifyChecksums)
	return out.String(), err
}

func assertShards(t *testing.T, name string, f *ecRebuildFakeServer, vid uint32, want []uint32) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var got []uint32
	for sid := range f.shards[vid] {
		got = append(got, sid)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("%s holds shards %v, want %v", name, got, want)
	}
}

// A complete volume whose shard fails the checksum on a node other than the
// rebuilder: the shard is regenerated on the rebuilder, never copied from the
// failing node, and the failing copy is removed afterwards.
func TestRebuildChecksumReplacesCorruptShardOfCompleteVolume(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
	rebuilder := startEcRebuildFakeServer(t, vid, shardRange(9, 14))

	out := runChecksumRebuild(t, true, true, holder, rebuilder)

	if rebuilder.rebuilds != 1 || holder.rebuilds != 0 {
		t.Fatalf("rebuilds: rebuilder %d, holder %d; want 1 and 0\n%s", rebuilder.rebuilds, holder.rebuilds, out)
	}
	if slices.Contains(rebuilder.copiedIds, 3) {
		t.Errorf("the corrupt shard was copied to the rebuilder: %v", rebuilder.copiedIds)
	}
	assertShards(t, "rebuilder", rebuilder, vid, append([]uint32{3}, shardRange(9, 14)...))
	assertShards(t, "holder", holder, vid, []uint32{0, 1, 2, 4, 5, 6, 7, 8})
	if !strings.Contains(out, "removed corrupt shards 1.[3]") {
		t.Errorf("expected the corrupt copy removal in the output:\n%s", out)
	}
}

// A corrupt shard on the rebuilder itself is regenerated in place and kept.
func TestRebuildChecksumRegeneratesCorruptShardOnRebuilder(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9))
	rebuilder := startEcRebuildFakeServer(t, vid, shardRange(9, 14), 11)

	out := runChecksumRebuild(t, true, true, holder, rebuilder)

	if rebuilder.rebuilds != 1 {
		t.Fatalf("rebuilder ran %d rebuilds, want 1\n%s", rebuilder.rebuilds, out)
	}
	if slices.Contains(rebuilder.deleted, 11) || holder.deleted != nil {
		t.Errorf("deleted: rebuilder %v, holder %v; the regenerated shard must stay", rebuilder.deleted, holder.deleted)
	}
	assertShards(t, "rebuilder", rebuilder, vid, shardRange(9, 14))
	assertShards(t, "holder", holder, vid, shardRange(0, 9))
}

// A corrupt copy of a shard another holder serves intact needs no rebuild:
// only the corrupt copy is removed.
func TestRebuildChecksumRemovesCorruptDuplicate(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
	other := startEcRebuildFakeServer(t, vid, append([]uint32{3}, shardRange(9, 14)...))

	out := runChecksumRebuild(t, true, true, holder, other)

	if holder.rebuilds+other.rebuilds != 0 {
		t.Fatalf("no rebuild expected, got %d\n%s", holder.rebuilds+other.rebuilds, out)
	}
	assertShards(t, "holder", holder, vid, []uint32{0, 1, 2, 4, 5, 6, 7, 8})
	assertShards(t, "other", other, vid, append([]uint32{3}, shardRange(9, 14)...))
}

// Dry-run scrubs and reports, but copies, rebuilds and removes nothing.
func TestRebuildChecksumDryRun(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
	rebuilder := startEcRebuildFakeServer(t, vid, shardRange(9, 14))

	out := runChecksumRebuild(t, false, true, holder, rebuilder)

	if holder.scrubs != 1 || rebuilder.scrubs != 1 {
		t.Errorf("scrubs: holder %d, rebuilder %d; want 1 each", holder.scrubs, rebuilder.scrubs)
	}
	if rebuilder.rebuilds != 0 || rebuilder.copiedIds != nil || holder.deleted != nil {
		t.Errorf("dry run changed state: rebuilds %d, copied %v, deleted %v", rebuilder.rebuilds, rebuilder.copiedIds, holder.deleted)
	}
	for _, want := range []string{
		"ec volume 1 shard 3 on " + holder.nodeId() + " fails its checksum",
		"missing shard 1.3",
		"would remove corrupt shards 1.[3] from " + holder.nodeId(),
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in the output:\n%s", want, out)
		}
	}
}

// Without -checksum a complete volume is neither scrubbed nor rebuilt.
func TestRebuildWithoutChecksumSkipsCompleteVolume(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
	rebuilder := startEcRebuildFakeServer(t, vid, shardRange(9, 14))

	runChecksumRebuild(t, true, false, holder, rebuilder)

	if holder.scrubs+rebuilder.scrubs != 0 || holder.rebuilds+rebuilder.rebuilds != 0 {
		t.Errorf("scrubs %d, rebuilds %d; want none", holder.scrubs+rebuilder.scrubs, holder.rebuilds+rebuilder.rebuilds)
	}
	assertShards(t, "holder", holder, vid, shardRange(0, 9))
}

// The removal of a corrupt duplicate is the whole repair of that volume, so a
// failed removal fails the run, and nothing is removed once the lock is lost.
func TestRebuildChecksumReportsDuplicateRemovalFailures(t *testing.T) {
	const vid = uint32(1)
	t.Run("unmount fails", func(t *testing.T) {
		holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
		other := startEcRebuildFakeServer(t, vid, append([]uint32{3}, shardRange(9, 14)...))
		holder.failUnmount = true

		out, err := runChecksumRebuildLocked(t, true, true, nil, holder, other)
		if err == nil || !strings.Contains(err.Error(), "unmount refused") {
			t.Fatalf("expected the failed removal to fail the run, got %v\n%s", err, out)
		}
		assertShards(t, "holder", holder, vid, shardRange(0, 9))
	})
	t.Run("lock lost", func(t *testing.T) {
		holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
		other := startEcRebuildFakeServer(t, vid, append([]uint32{3}, shardRange(9, 14)...))

		out, err := runChecksumRebuildLocked(t, true, true, func() bool { return false }, holder, other)
		if err == nil || !strings.Contains(err.Error(), "lock is lost") {
			t.Fatalf("expected a lost lock error, got %v\n%s", err, out)
		}
		if holder.deleted != nil {
			t.Errorf("removed %v without the lock", holder.deleted)
		}
	})
}

// A node whose scrub fails fails the run, and the corruption found on the
// other nodes is still repaired.
func TestRebuildChecksumScrubFailureFailsRun(t *testing.T) {
	const vid = uint32(1)
	holder := startEcRebuildFakeServer(t, vid, shardRange(0, 9), 3)
	rebuilder := startEcRebuildFakeServer(t, vid, shardRange(9, 14))
	rebuilder.failScrub = true

	out, err := runChecksumRebuildLocked(t, true, true, nil, holder, rebuilder)
	if err == nil || !strings.Contains(err.Error(), "scrub refused") {
		t.Fatalf("expected the failed scrub to fail the run, got %v\n%s", err, out)
	}
	assertShards(t, "rebuilder", rebuilder, vid, append([]uint32{3}, shardRange(9, 14)...))
	assertShards(t, "holder", holder, vid, []uint32{0, 1, 2, 4, 5, 6, 7, 8})
}
