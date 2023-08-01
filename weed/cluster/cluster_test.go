package cluster

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"strconv"
	"sync"
	"testing"
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
