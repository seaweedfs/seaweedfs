package balance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// A master that answered a capacity question for a disk type a node lacks
// published an empty DiskInfo of that type for the node (MaxVolumeCount 0, no
// volumes). The planner registered every such node as a candidate with zero
// volumes, and one zero max switched the whole disk type to raw-count
// balancing, so the empty node became the destination of every move -- which
// the volume server then refused ("no space left hdd" / "no space left ssd").
//
// The entry always has DiskId 0. With real disks at id 1 (location 0
// unavailable) it collides with nothing and must be ignored all the same.
func TestDetection_IgnoresEmptyDiskTypeEntries(t *testing.T) {
	for _, realDiskID := range []uint32{0, 1} {
		t.Run(fmt.Sprintf("real disks at id %d", realDiskID), func(t *testing.T) {
			// Raw counts and utilization disagree on purpose: by raw count hdd-3 is
			// the most loaded, by utilization hdd-2 is.
			servers := []serverSpec{
				{id: "hdd-1", diskType: "hdd", diskID: realDiskID, dc: "dc1", rack: "rack1", maxVolumes: 700},
				{id: "hdd-2", diskType: "hdd", diskID: realDiskID, dc: "dc1", rack: "rack1", maxVolumes: 700},
				{id: "hdd-3", diskType: "hdd", diskID: realDiskID, dc: "dc1", rack: "rack1", maxVolumes: 1260},
				{id: "ssd-1", diskType: "ssd", diskID: realDiskID, dc: "dc1", rack: "rack2", maxVolumes: 200},
			}
			counts := []struct {
				server, diskType string
				n                int
			}{{"hdd-1", "hdd", 600}, {"hdd-2", "hdd", 695}, {"hdd-3", "hdd", 900}, {"ssd-1", "ssd", 22}}

			var metrics []*types.VolumeHealthMetrics
			vid := uint32(1)
			for _, c := range counts {
				metrics = append(metrics, makeVolumes(c.server, c.diskType, "dc1", "rack1", "", vid, c.n)...)
				vid += uint32(c.n)
			}

			at := buildTopology(servers, metrics)
			// Add what the master published: an empty entry of the other disk
			// type on every node, DiskId 0.
			topo := at.GetTopologyInfo()
			for _, dc := range topo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						other := "ssd"
						if strings.HasPrefix(node.Id, "ssd-") {
							other = "hdd"
						}
						node.DiskInfos[other] = &master_pb.DiskInfo{Type: other}
					}
				}
			}
			if err := at.UpdateTopology(topo); err != nil {
				t.Fatalf("UpdateTopology: %v", err)
			}
			for _, s := range servers {
				disks := at.GetNodeDisks(s.id)
				if len(disks) != 1 || disks[0].DiskType != s.diskType || disks[0].DiskID != realDiskID {
					var got []string
					for _, d := range disks {
						got = append(got, fmt.Sprintf("%q@%d", d.DiskType, d.DiskID))
					}
					t.Fatalf("%s disks %v, want only its real %q disk %d", s.id, got, s.diskType, realDiskID)
				}
			}

			cfg := &Config{BaseConfig: base.BaseConfig{Enabled: true}, ImbalanceThreshold: 0.20, MinServerCount: 2}
			tasks, _, err := Detection(metrics, &types.ClusterInfo{ActiveTopology: at}, cfg, 10)
			if err != nil {
				t.Fatalf("Detection failed: %v", err)
			}
			if len(tasks) == 0 {
				t.Fatal("expected HDD balance tasks, got none")
			}
			for i, task := range tasks {
				if task.TypedParams == nil || len(task.TypedParams.Targets) == 0 {
					t.Fatalf("task %d (volume %d) has no target", i, task.VolumeID)
				}
				target := task.TypedParams.Targets[0].Node
				if task.Server == "ssd-1" {
					t.Errorf("task %d moves SSD volume %d from ssd-1 to %s: ssd-1 is the only server with an SSD disk", i, task.VolumeID, target)
				}
				if strings.HasPrefix(target, "ssd-1") {
					t.Errorf("task %d moves HDD volume %d from %s onto ssd-1, which has no HDD disk", i, task.VolumeID, task.Server)
				}
				if task.Server == "hdd-3" {
					t.Errorf("task %d picks hdd-3 (lowest utilization, 71%%) as source: balancing fell back to raw volume counts", i)
				}
			}
			if tasks[0].Server != "hdd-2" {
				t.Errorf("first source = %s, want hdd-2 (highest utilization, 99%%)", tasks[0].Server)
			}
		})
	}
}
