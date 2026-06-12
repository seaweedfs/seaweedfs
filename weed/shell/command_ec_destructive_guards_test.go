package shell

import (
	"bytes"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// topoWith builds a one-node topology snapshot where regularVids appear as
// regular volumes (.dat) and ecVids appear as EC shards.
func topoWith(regularVids, ecVids []uint32) *master_pb.TopologyInfo {
	di := &master_pb.DiskInfo{Type: "hdd"}
	for _, v := range regularVids {
		di.VolumeInfos = append(di.VolumeInfos, &master_pb.VolumeInformationMessage{Id: v})
	}
	for _, v := range ecVids {
		di.EcShardInfos = append(di.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{Id: v})
	}
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:        "n1",
					DiskInfos: map[string]*master_pb.DiskInfo{"hdd": di},
				}},
			}},
		}},
	}
}

// TestAssertEncodableRegularVolumes: ec.encode must refuse an already-EC volume
// (no .dat — re-encoding would destroy its shards) and an unknown id, while
// allowing a regular volume and a failed-encode retry (regular .dat + orphans).
func TestAssertEncodableRegularVolumes(t *testing.T) {
	cases := []struct {
		name          string
		regular, ec   []uint32
		vids          []needle.VolumeId
		wantErrSubstr string
	}{
		{"regular volume", []uint32{1}, nil, []needle.VolumeId{1}, ""},
		{"already EC", nil, []uint32{1}, []needle.VolumeId{1}, "already EC"},
		{"regular plus stale orphan shards", []uint32{1}, []uint32{1}, []needle.VolumeId{1}, ""},
		{"unknown id", nil, nil, []needle.VolumeId{1}, "not found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := assertEncodableRegularVolumes(topoWith(tc.regular, tc.ec), tc.vids)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("want nil error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErrSubstr, err)
			}
		})
	}
}

func newDryRunRebuilder(log *bytes.Buffer, nodes ...*EcNode) *ecRebuilder {
	return &ecRebuilder{
		commandEnv:   &CommandEnv{env: make(map[string]string), noLock: true},
		ecNodes:      nodes,
		writer:       log,
		applyChanges: false,
	}
}

// TestPrepareDataToRecover_DryRunRecoverableNoSideEffects: a dry-run on a
// recoverable degraded volume the rebuilder holds none of must still succeed
// and report its plan, while recording zero shards to delete (so the deferred
// cleanup issues no VolumeEcShardsDelete RPC).
func TestPrepareDataToRecover_DryRunRecoverableNoSideEffects(t *testing.T) {
	var log bytes.Buffer
	rebuilder := newEcNode("dc1", "rack1", "rebuilder", 100)
	remote := newEcNode("dc1", "rack1", "remote", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	erb := newDryRunRebuilder(&log, rebuilder, remote)

	locations := make(EcShardLocations, erasure_coding.MaxShardCount)
	for i := 0; i < 12; i++ {
		locations[i] = []*EcNode{remote}
	}

	copied, local, err := erb.prepareDataToRecover(rebuilder, "c1", needle.VolumeId(1), locations)
	if err != nil {
		t.Fatalf("dry-run on a recoverable volume must not error: %v", err)
	}
	if len(copied) != 0 {
		t.Errorf("dry-run must record no copied shards (deletion set), got %v", copied)
	}
	if len(local) != 0 {
		t.Errorf("rebuilder holds no shards locally, got %v", local)
	}
	if !strings.Contains(log.String(), "would copy") {
		t.Errorf("dry-run should report the would-copy plan, got:\n%s", log.String())
	}
}

// TestPrepareDataToRecover_UnionsLocalShardsAcrossDisks: shards the rebuilder
// holds on more than one disk must all count as local (not copied), so the
// per-disk overwrite that made one disk's shards look remote (then self-copied
// with O_TRUNC and node-wide deleted) cannot happen.
func TestPrepareDataToRecover_UnionsLocalShardsAcrossDisks(t *testing.T) {
	var log bytes.Buffer
	rebuilder := newEcNode("dc1", "rack1", "rebuilder", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1}).
		addEcVolumeShards(needle.VolumeId(1), "c1", []erasure_coding.ShardId{5}, types.SsdType)
	remote := newEcNode("dc1", "rack1", "remote", 100)
	erb := newDryRunRebuilder(&log, rebuilder)

	locations := make(EcShardLocations, erasure_coding.MaxShardCount)
	for i := 0; i < 12; i++ {
		locations[i] = []*EcNode{remote}
	}
	locations[0] = []*EcNode{rebuilder}
	locations[1] = []*EcNode{rebuilder}
	locations[5] = []*EcNode{rebuilder}

	copied, local, err := erb.prepareDataToRecover(rebuilder, "c1", needle.VolumeId(1), locations)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	missing := map[erasure_coding.ShardId]bool{0: true, 1: true, 5: true}
	for _, s := range local {
		delete(missing, s)
	}
	if len(missing) != 0 {
		t.Errorf("shards %v should be local (union across disks); local=%v", missing, local)
	}
	for _, s := range copied {
		if s == 0 || s == 1 || s == 5 {
			t.Errorf("local shard %d must never be in the copy/delete set", s)
		}
	}
}

// TestPrepareDataToRecover_SelfSourceNotCopied: if the rebuilder is the only
// listed holder of a shard it does not report locally, it must be treated as
// local rather than copied onto itself and then deleted.
func TestPrepareDataToRecover_SelfSourceNotCopied(t *testing.T) {
	var log bytes.Buffer
	rebuilder := newEcNode("dc1", "rack1", "rebuilder", 100).
		addEcVolumeAndShardsForTest(1, "c1", []erasure_coding.ShardId{0, 1, 2, 3, 4, 6, 7, 8, 9, 10})
	erb := newDryRunRebuilder(&log, rebuilder)

	locations := make(EcShardLocations, erasure_coding.MaxShardCount)
	for _, s := range []int{0, 1, 2, 3, 4, 6, 7, 8, 9, 10} {
		locations[s] = []*EcNode{rebuilder}
	}
	locations[5] = []*EcNode{rebuilder} // sole holder is the rebuilder, not in its reported shards

	copied, local, err := erb.prepareDataToRecover(rebuilder, "c1", needle.VolumeId(1), locations)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, s := range copied {
		if s == 5 {
			t.Errorf("self-sourced shard 5 must not be copied/deleted")
		}
	}
	found := false
	for _, s := range local {
		if s == 5 {
			found = true
		}
	}
	if !found {
		t.Errorf("self-sourced shard 5 should be classified local, got local=%v", local)
	}
}
