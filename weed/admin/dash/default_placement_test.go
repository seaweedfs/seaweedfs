package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestDefaultPlacementServers(t *testing.T) {
	servers := []VolumeServer{
		{Address: "a:8080", DataCenter: "dc1", Rack: "r1", Volumes: 3},
		{Address: "b:8080", DataCenter: "dc1", Rack: "r2"},
		{Address: "c:8080", DataCenter: topology.DefaultDataCenter, Rack: topology.DefaultRack, EcShards: 2030},
		{Address: "d:8080", DataCenter: "dc1", Rack: topology.DefaultRack},
	}
	got := defaultPlacementServers(servers)
	if len(got) != 2 || got[0].Address != "c:8080" || got[1].Address != "d:8080" {
		t.Fatalf("defaultPlacementServers = %+v, want c:8080 and d:8080", got)
	}
	if got[0].EcShards != 2030 {
		t.Fatalf("EC shard count not carried: %d", got[0].EcShards)
	}

	allDefault := []VolumeServer{
		{Address: "a:8080", DataCenter: topology.DefaultDataCenter, Rack: topology.DefaultRack},
		{Address: "b:8080", DataCenter: topology.DefaultDataCenter, Rack: topology.DefaultRack},
	}
	if got := defaultPlacementServers(allDefault); len(got) != 0 {
		t.Fatalf("all-default cluster flagged: %+v", got)
	}
}
