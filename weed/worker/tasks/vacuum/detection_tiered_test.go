package vacuum

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestPickPrimaryReplicaSkipsTieredVolumes(t *testing.T) {
	m := func(id uint32, remote bool) *types.VolumeHealthMetrics {
		return &types.VolumeHealthMetrics{VolumeID: id, Server: "s:8080", GarbageRatio: 0.9, Age: 72 * time.Hour,
			HasRemoteCopy: remote, IsReadOnly: remote}
	}
	if p := pickPrimaryReplica([]*types.VolumeHealthMetrics{m(1, false)}, 0.3, 24*time.Hour); p == nil {
		t.Fatal("a local volume at 90% garbage must be picked")
	}
	if p := pickPrimaryReplica([]*types.VolumeHealthMetrics{m(2, true)}, 0.3, 24*time.Hour); p != nil {
		t.Fatalf("tiered volume %d picked for vacuum", p.VolumeID)
	}
}
