package erasure_coding

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsStubReplica(t *testing.T) {
	assert.True(t, isStubReplica(0), "0-byte .dat is a stub")
	assert.True(t, isStubReplica(uint64(super_block.SuperBlockSize)-1), "below a superblock is a stub")
	assert.True(t, isStubReplica(uint64(super_block.SuperBlockSize)), "a bare superblock holds no data — a stub")
	assert.False(t, isStubReplica(uint64(super_block.SuperBlockSize)+1), "data beyond the superblock is a real replica")
	assert.False(t, isStubReplica(200*1024*1024), "a data-bearing replica is not a stub")
}

// A 0-byte stub left by an interrupted encode often sorts to a lower server id
// than the real replica. The old lowest-server canonical pick then reported
// Size=0, tripped the min-size gate, and the volume was stranded in
// skippedTooSmall forever. selectCanonicalMetric must skip the stub and return
// the data-bearing replica.
func TestSelectCanonicalMetricPrefersCredibleOverLowServerStub(t *testing.T) {
	stub := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.1:8080", Size: 0}
	real := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.4:8080", Size: 200 * 1024 * 1024}

	got := selectCanonicalMetric([]*types.VolumeHealthMetrics{stub, real})
	require.Same(t, real, got)
}

// An EC-side metric (the partial shards from a failed encode) can also sort
// below the regular replica. Picking it would short-circuit at the
// IsECVolume guard and skip the volume, hiding it from both the orphan-source
// cleanup and the re-encode path. The credible regular replica must win.
func TestSelectCanonicalMetricSkipsECMetrics(t *testing.T) {
	ecMetric := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.1:8080", Size: 50 * 1024 * 1024, IsECVolume: true}
	real := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.4:8080", Size: 200 * 1024 * 1024}

	got := selectCanonicalMetric([]*types.VolumeHealthMetrics{ecMetric, real})
	require.Same(t, real, got)
}

// When nothing is credible there is nothing to encode. Fall back to the
// lowest-server metric so the downstream gates (min-size / IsECVolume) make
// the skip decision exactly as before — selectCanonicalMetric must not invent
// a source.
func TestSelectCanonicalMetricAllStubsFallsBackToLowestServer(t *testing.T) {
	stubHi := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.4:8080", Size: 0}
	stubLo := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.1:8080", Size: 0}

	got := selectCanonicalMetric([]*types.VolumeHealthMetrics{stubHi, stubLo})
	require.Same(t, stubLo, got)
}

// Among several credible replicas the lowest server id still wins, preserving
// the deterministic canonical choice the task-dedup logic relies on.
func TestSelectCanonicalMetricTieBreaksByServerAmongCredible(t *testing.T) {
	hi := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.7:8080", Size: 200 * 1024 * 1024}
	lo := &types.VolumeHealthMetrics{VolumeID: 13, Server: "10.0.0.2:8080", Size: 200 * 1024 * 1024}

	got := selectCanonicalMetric([]*types.VolumeHealthMetrics{hi, lo})
	require.Same(t, lo, got)
}

func TestSelectCanonicalMetricEmpty(t *testing.T) {
	require.Nil(t, selectCanonicalMetric(nil))
	require.Nil(t, selectCanonicalMetric([]*types.VolumeHealthMetrics{}))
}

// End-to-end regression for the stranded-volume bug: a volume whose lowest
// server holds a 0-byte stub and whose real replica is on a higher server must
// still be proposed for EC encoding, not silently dropped as too-small.
func TestDetectionEncodesDespiteLowServerStub(t *testing.T) {
	const volumeID uint32 = 13
	activeTopology := buildStubReplicaTopology(t, volumeID)
	clusterInfo := &types.ClusterInfo{ActiveTopology: activeTopology}

	lastModified := time.Now().Add(-2 * time.Hour)
	metrics := []*types.VolumeHealthMetrics{
		// Stub on the lowest server: an interrupted encode left a 0-byte .dat.
		{VolumeID: volumeID, Server: "127.0.0.1:8080", Size: 0, FullnessRatio: 0, LastModified: lastModified, Age: time.Since(lastModified)},
		// The real replica on a higher server.
		{VolumeID: volumeID, Server: "127.0.0.1:8081", Size: 200 * 1024 * 1024, FullnessRatio: 0.96, LastModified: lastModified, Age: time.Since(lastModified)},
	}

	results, _, err := Detection(context.Background(), metrics, clusterInfo, NewDefaultConfig(), 0)
	require.NoError(t, err)
	require.Len(t, results, 1, "volume with a real replica must be proposed for EC despite a low-server stub")
	assert.Equal(t, types.TaskTypeErasureCoding, results[0].TaskType)
	assert.Equal(t, volumeID, results[0].VolumeID)
}

// buildStubReplicaTopology builds a cluster with TotalShardsCount single-disk
// nodes (enough targets to place every shard) where node 0 holds a 0-byte stub
// replica of volumeID and node 1 holds the real replica.
func buildStubReplicaTopology(t *testing.T, volumeID uint32) *topology.ActiveTopology {
	t.Helper()
	activeTopology := topology.NewActiveTopology(10)
	nodes := make([]*master_pb.DataNodeInfo, 0, erasure_coding.TotalShardsCount)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		diskInfo := &master_pb.DiskInfo{
			DiskId:         0,
			MaxVolumeCount: 200,
		}
		switch i {
		case 0:
			diskInfo.VolumeInfos = []*master_pb.VolumeInformationMessage{{
				Id: volumeID, DiskId: 0, DiskType: "hdd", Size: 0,
			}}
			diskInfo.VolumeCount = 1
		case 1:
			diskInfo.VolumeInfos = []*master_pb.VolumeInformationMessage{{
				Id: volumeID, DiskId: 0, DiskType: "hdd", Size: 200 * 1024 * 1024,
			}}
			diskInfo.VolumeCount = 1
		}
		nodes = append(nodes, &master_pb.DataNodeInfo{
			Id:        fmt.Sprintf("127.0.0.1:%d", 8080+i),
			DiskInfos: map[string]*master_pb.DiskInfo{"hdd": diskInfo},
		})
	}
	require.NoError(t, activeTopology.UpdateTopology(&master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id:            "rack1",
				DataNodeInfos: nodes,
			}},
		}},
	}))
	return activeTopology
}
