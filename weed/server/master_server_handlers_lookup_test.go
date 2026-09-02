package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// The nodes answering for an EC volume hold shards, not a volume record, so
// asking them for one fails. Dropping them would empty the lookup and turn
// every EC read through the master into a 404.
func TestEcVolumeLocationsSurviveTheLookup(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 7, Collection: "c", EcIndexBits: 0x3fff},
	}, dn)

	machines := topo.Lookup("c", needle.VolumeId(7))
	if len(machines) == 0 {
		t.Fatalf("topology lost the EC volume")
	}
	for _, node := range machines {
		loc := topologyLocation(node, needle.VolumeId(7))
		if loc.Url == "" {
			t.Errorf("EC location came back empty: %+v", loc)
		}
		if loc.DataInRemote {
			t.Errorf("an EC shard holder was reported as remote-tier: %+v", loc)
		}
	}
}

// A volume the node really does hold keeps its tier classification.
func TestVolumeLocationCarriesTheRemoteTier(t *testing.T) {
	topo, dn := changedTestCluster(t)
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		changedTestVolume(1, 1024),
		changedTierVolume(2, 1024, "s3-bucket"),
	}, dn)

	if loc := topologyLocation(dn, needle.VolumeId(1)); loc.DataInRemote {
		t.Errorf("a local volume was reported as remote-tier: %+v", loc)
	}
	if loc := topologyLocation(dn, needle.VolumeId(2)); !loc.DataInRemote {
		t.Errorf("a tiered volume was reported as local: %+v", loc)
	}
}
