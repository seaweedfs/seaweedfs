package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

func TestMasterIsKnownPingTarget(t *testing.T) {
	topo := topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topology.NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := topology.NewRack("rack1")
	dc.LinkChildNode(rack)

	dn := topology.NewDataNode("vol1")
	dn.Ip = "10.0.0.10"
	dn.Port = 8080
	dn.GrpcPort = 18080
	rack.LinkChildNode(dn)

	c := cluster.NewCluster()
	filerAddr := pb.ServerAddress("10.0.0.20:8888")
	c.AddClusterNode("", cluster.FilerType, "dc1", "rack1", filerAddr, "test")

	ms := &MasterServer{
		option:  &MasterOption{Master: pb.ServerAddress("10.0.0.1:9333")},
		Topo:    topo,
		Cluster: c,
	}

	ctx := context.Background()
	cases := []struct {
		name       string
		target     string
		targetType string
		want       bool
	}{
		{"known filer", string(filerAddr), cluster.FilerType, true},
		{"known volume server", "10.0.0.10:8080.18080", cluster.VolumeServerType, true},
		{"known volume server http addr", "10.0.0.10:8080", cluster.VolumeServerType, true},
		{"known self master", "10.0.0.1:9333", cluster.MasterType, true},
		{"unknown localhost low port", "127.0.0.1:1", cluster.VolumeServerType, false},
		{"unknown localhost high port", "127.0.0.1:65000", cluster.FilerType, false},
		{"unrelated host", "example.com:443", cluster.MasterType, false},
		{"unknown target type", string(filerAddr), "garbage", false},
		{"filer address checked as volume server", string(filerAddr), cluster.VolumeServerType, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ms.isKnownPingTarget(ctx, tc.target, tc.targetType)
			if got != tc.want {
				t.Fatalf("isKnownPingTarget(%q,%q) = %v, want %v", tc.target, tc.targetType, got, tc.want)
			}
		})
	}
}

// On a follower master the topology is empty because only the leader receives
// volume-server heartbeats. The gate must then fall back to the volume-server
// set learned over the MasterClient subscription instead of rejecting every
// volume-server target. The positive MasterClient.HasVolumeServer lookup is
// covered by wdclient.TestHasVolumeServer; here we lock in that an empty
// topology no longer short-circuits to false and that the fallback is
// nil-safe.
func TestMasterIsKnownPingTargetFollowerEmptyTopology(t *testing.T) {
	ctx := context.Background()
	vs := "10.0.0.10:8080.18080"

	// No topology and no MasterClient: nothing to consult, so reject.
	bare := &MasterServer{option: &MasterOption{Master: pb.ServerAddress("10.0.0.1:9333")}}
	if bare.isKnownPingTarget(ctx, vs, cluster.VolumeServerType) {
		t.Fatalf("volume server must be unknown without topology or MasterClient")
	}

	// Empty topology plus a MasterClient that has not learned this volume
	// server yet: still reject, but exercise the MasterClient fallback path.
	mc := wdclient.NewMasterClient(grpc.EmptyDialOption{}, "", cluster.MasterType, "", "", "", pb.ServerDiscovery{})
	follower := &MasterServer{
		option:       &MasterOption{Master: pb.ServerAddress("10.0.0.1:9333")},
		Topo:         topology.NewTopology("test", sequence.NewMemorySequencer(), 32*1024, 5, false),
		MasterClient: mc,
	}
	if follower.isKnownPingTarget(ctx, vs, cluster.VolumeServerType) {
		t.Fatalf("volume server must be unknown until the MasterClient learns it")
	}
}

func TestVolumeServerIsKnownPingTarget(t *testing.T) {
	seed := pb.ServerAddress("10.0.0.1:9333")
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{seed},
		seedMasterSet:   map[string]struct{}{seed.ToHttpAddress(): {}},
	}
	vs.setCurrentMaster(pb.ServerAddress("10.0.0.2:9333"))

	cases := []struct {
		name       string
		target     string
		targetType string
		want       bool
	}{
		{"seed master", string(seed), cluster.MasterType, true},
		{"current master", "10.0.0.2:9333", cluster.MasterType, true},
		{"other volume server", "10.0.0.5:8080", cluster.VolumeServerType, false},
		{"random filer", "10.0.0.6:8888", cluster.FilerType, false},
		{"unknown low port", "127.0.0.1:1", cluster.MasterType, false},
		{"unknown high port", "127.0.0.1:65000", cluster.MasterType, false},
		{"unrelated host", "example.com:443", cluster.MasterType, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := vs.isKnownPingTarget(tc.target, tc.targetType)
			if got != tc.want {
				t.Fatalf("isKnownPingTarget(%q,%q) = %v, want %v", tc.target, tc.targetType, got, tc.want)
			}
		})
	}
}
