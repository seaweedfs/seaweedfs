package vacuum

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestPickPrimaryReplicaSkipsTieredReplicas(t *testing.T) {
	m := func(id uint32, server string, remote bool) *types.VolumeHealthMetrics {
		return &types.VolumeHealthMetrics{VolumeID: id, Server: server, GarbageRatio: 0.9, Age: 72 * time.Hour,
			HasRemoteCopy: remote, IsReadOnly: remote}
	}
	if p := pickPrimaryReplica([]*types.VolumeHealthMetrics{m(1, "a:8080", false)}, 0.3, 24*time.Hour); p == nil {
		t.Fatal("a local volume at 90% garbage must be picked")
	}
	if p := pickPrimaryReplica([]*types.VolumeHealthMetrics{m(2, "a:8080", true)}, 0.3, 24*time.Hour); p != nil {
		t.Fatalf("tiered replica of volume %d picked for vacuum", p.VolumeID)
	}
	p := pickPrimaryReplica([]*types.VolumeHealthMetrics{m(3, "a:8080", true), m(3, "b:8080", false)}, 0.3, 24*time.Hour)
	if p == nil || p.Server != "b:8080" {
		t.Fatalf("partially tiered volume should pick its local replica, got %+v", p)
	}
}
