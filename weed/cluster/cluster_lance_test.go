package cluster

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestSetLancePort(t *testing.T) {
	c := NewCluster()
	gateway := pb.ServerAddress("10.0.0.30:18333")
	// Registered directly, so the test does not depend on AddClusterNode's arguments.
	c.s3Groups.getGroupMembers("group", true).members[gateway] = &ClusterNode{Address: gateway}

	c.SetLancePort("group", gateway, 9101)
	c.SetS3Endpoint("group", gateway, "https://s3.example.com")
	// A gateway of another group, or one that never registered, records nothing.
	c.SetLancePort("", gateway, 9200)
	c.SetLancePort("group", pb.ServerAddress("10.0.0.31:18333"), 9300)
	c.SetS3Endpoint("", gateway, "http://other:8333")

	nodes := c.ListClusterNode("group", S3Type)
	if len(nodes) != 1 || nodes[0].LancePort.Load() != 9101 || nodes[0].GetS3Endpoint() != "https://s3.example.com" {
		t.Fatalf("want the gateway listed with port 9101 and its S3 endpoint, got %d nodes", len(nodes))
	}
	if nodes := c.ListClusterNode("", S3Type); len(nodes) != 0 {
		t.Fatalf("SetLancePort must not register a node, got %d", len(nodes))
	}
}
