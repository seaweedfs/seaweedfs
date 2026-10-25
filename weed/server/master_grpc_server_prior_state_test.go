package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/assert"
)

func TestReportsPriorClusterState(t *testing.T) {
	topoId := "topo-1"

	// a quiet heartbeat from a synced server is not evidence
	assert.False(t, reportsPriorClusterState(&master_pb.Heartbeat{
		LastSeenTopologyId: topoId,
	}, topoId, 5))

	// echoes of volume ids this master issued are not evidence
	assert.False(t, reportsPriorClusterState(&master_pb.Heartbeat{
		MaxVolumeIdSeen:    5,
		NewVolumes:         []*master_pb.VolumeShortInformationMessage{{Id: 5}},
		ChangedVolumes:     []*master_pb.VolumeInformationMessage{{Id: 3}},
		LastSeenTopologyId: topoId,
	}, topoId, 5))

	// ids above the issued bound come from before this term
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		MaxVolumeIdSeen: 6,
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		Volumes: []*master_pb.VolumeInformationMessage{{Id: 6}},
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		NewVolumes: []*master_pb.VolumeShortInformationMessage{{Id: 6}},
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		DeletedVolumes: []*master_pb.VolumeShortInformationMessage{{Id: 6}},
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		EcShards: []*master_pb.VolumeEcShardInformationMessage{{Id: 42}},
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		NewEcShards: []*master_pb.VolumeEcShardInformationMessage{{Id: 42}},
	}, topoId, 5))
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{{Id: 42}},
	}, topoId, 5))

	// a heartbeat last synced under another topology id is prior state
	assert.True(t, reportsPriorClusterState(&master_pb.Heartbeat{
		LastSeenTopologyId: "topo-0",
	}, topoId, 5))
}
