package cluster

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
)

func TestClusterAddRemoveNodes(t *testing.T) {
	c := NewCluster()

	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:1"), "23.45")
	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:2"), "23.45")
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:3"), "23.45")
	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:4"), "23.45")
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:5"), "23.45")
	c.AddClusterNode("", "filer", "", "", pb.ServerAddress("111:6"), "23.45")
	c.RemoveClusterNode("", "filer", pb.ServerAddress("111:4"))
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("", "filer", pb.ServerAddress("111:1"))
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:6"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.getGroupMembers("", "filer", true).leaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("", "filer", pb.ServerAddress("111:1"))

}

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
