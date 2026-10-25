package cluster

import (
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestConcurrentAddRemoveNodes(t *testing.T) {
	c := NewCluster()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			address := strconv.Itoa(i)
			c.AddClusterNode("", "filer", "", "", pb.ServerAddress(address), "23.45")
		}(i)
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			address := strconv.Itoa(i)
			node := c.RemoveClusterNode("", "filer", pb.ServerAddress(address))

			if len(node) == 0 {
				t.Errorf("TestConcurrentAddRemoveNodes: node[%s] not found", address)
				return
			} else if node[0].ClusterNodeUpdate.Address != address {
				t.Errorf("TestConcurrentAddRemoveNodes: expect:%s, actual:%s", address, node[0].ClusterNodeUpdate.Address)
				return
			}
		}(i)
	}
	wg.Wait()
}

func TestListClusterNodeUpdates(t *testing.T) {
	c := NewCluster()
	filer := pb.ServerAddress("10.0.0.20:8888")
	c.AddClusterNode("group", FilerType, "dc1", "rack1", filer, "test")
	c.AddClusterNode("group", BrokerType, "dc1", "rack1", pb.ServerAddress("10.0.0.20:17777"), "test")

	updates := c.ListClusterNodeUpdates("group", FilerType)
	if len(updates) != 1 {
		t.Fatalf("expecting one filer update, got %d", len(updates))
	}
	update := updates[0].ClusterNodeUpdate
	if update.Address != string(filer) || !update.IsAdd || update.FilerGroup != "group" {
		t.Fatalf("unexpected update %+v", update)
	}

	c.RemoveClusterNode("group", FilerType, filer)
	if updates := c.ListClusterNodeUpdates("group", FilerType); len(updates) != 0 {
		t.Fatalf("expecting no update for a removed filer, got %d", len(updates))
	}
}

func TestIsKnownNode(t *testing.T) {
	c := NewCluster()
	filer := pb.ServerAddress("10.0.0.20:8888")
	c.AddClusterNode("", FilerType, "dc1", "rack1", filer, "test")

	if !c.IsKnownNode(FilerType, filer) {
		t.Fatalf("registered filer %s should be known", filer)
	}
	if c.IsKnownNode(VolumeServerType, filer) {
		t.Fatalf("filer address must not be accepted as a volume server target")
	}
	if c.IsKnownNode(FilerType, pb.ServerAddress("127.0.0.1:1")) {
		t.Fatalf("unregistered low-port target must be rejected")
	}
	if c.IsKnownNode(FilerType, pb.ServerAddress("127.0.0.1:65000")) {
		t.Fatalf("unregistered high-port target must be rejected")
	}
	if c.IsKnownNode(FilerType, pb.ServerAddress("example.com:443")) {
		t.Fatalf("unrelated host must be rejected")
	}
	if c.IsKnownNode("garbage", filer) {
		t.Fatalf("unknown node type must be rejected")
	}
}
