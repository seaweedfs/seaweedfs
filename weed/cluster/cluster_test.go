package cluster

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClusterAddRemoveNodes(t *testing.T) {
	c := NewCluster()

	c.AddClusterNode("filer", pb.ServerAddress("111:1"), "23.45")
	c.AddClusterNode("filer", pb.ServerAddress("111:2"), "23.45")
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
	}, c.filerLeaders.GetLeaders())

	c.AddClusterNode("filer", pb.ServerAddress("111:3"), "23.45")
	c.AddClusterNode("filer", pb.ServerAddress("111:4"), "23.45")
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.filerLeaders.GetLeaders())

	c.AddClusterNode("filer", pb.ServerAddress("111:5"), "23.45")
	c.AddClusterNode("filer", pb.ServerAddress("111:6"), "23.45")
	c.RemoveClusterNode("filer", pb.ServerAddress("111:4"))
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:1"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.filerLeaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("filer", pb.ServerAddress("111:1"))
	assert.Equal(t, []pb.ServerAddress{
		pb.ServerAddress("111:6"),
		pb.ServerAddress("111:2"),
		pb.ServerAddress("111:3"),
	}, c.filerLeaders.GetLeaders())

	// remove oldest
	c.RemoveClusterNode("filer", pb.ServerAddress("111:1"))

}
