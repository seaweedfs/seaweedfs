package dash

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Volume servers are identified by host:port, and the detected task must
// still be saved under its ID.
func TestECBalanceTaskStateIsSaved(t *testing.T) {
	disk := func(shards ...*master_pb.VolumeEcShardInformationMessage) map[string]*master_pb.DiskInfo {
		return map[string]*master_pb.DiskInfo{"": {MaxVolumeCount: 100, EcShardInfos: shards}}
	}
	topoInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{
				{Id: "10.0.0.1:8080", DiskInfos: disk(&master_pb.VolumeEcShardInformationMessage{Id: 100, FileCount: 1, Collection: "col1", EcIndexBits: 0x3FFF})},
				{Id: "10.0.0.2:8080", DiskInfos: disk()},
				{Id: "10.0.0.3:8080", DiskInfos: disk()},
			}}},
		}},
	}
	at := topology.NewActiveTopology(0)
	if err := at.UpdateTopology(topoInfo); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}
	results, _, err := ec_balance.Detection(context.Background(), nil, &types.ClusterInfo{ActiveTopology: at}, ec_balance.NewDefaultConfig(), 0)
	if err != nil {
		t.Fatalf("Detection: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("fixture planned no moves")
	}

	cp := NewConfigPersistence(t.TempDir())
	for _, r := range results {
		task := &maintenance.MaintenanceTask{
			ID:       r.TaskID,
			Type:     maintenance.MaintenanceTaskType(r.TaskType),
			VolumeID: r.VolumeID,
			Server:   r.Server,
			Status:   maintenance.TaskStatusPending,
		}
		if err := cp.SaveTaskState(task); err != nil {
			t.Fatalf("SaveTaskState(%q): %v", r.TaskID, err)
		}
		if _, err := cp.LoadTaskState(r.TaskID); err != nil {
			t.Fatalf("LoadTaskState(%q): %v", r.TaskID, err)
		}
	}
}
