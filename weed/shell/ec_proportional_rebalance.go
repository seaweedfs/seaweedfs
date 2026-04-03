package shell

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/distribution"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// ECDistribution is an alias to the distribution package type for backward compatibility
type ECDistribution = distribution.ECDistribution

// TopologyDistributionAnalysis holds the current shard distribution analysis
// This wraps the distribution package's TopologyAnalysis with shell-specific EcNode handling
type TopologyDistributionAnalysis struct {
	inner *distribution.TopologyAnalysis

	// Shell-specific mappings
	nodeMap map[string]*EcNode // nodeID -> EcNode
}

// ECShardMove represents a planned shard move (shell-specific with EcNode references)
type ECShardMove struct {
	VolumeId   needle.VolumeId
	ShardId    erasure_coding.ShardId
	SourceNode *EcNode
	DestNode   *EcNode
	Reason     string
}

// ProportionalECRebalancer implements proportional shard distribution for shell commands
type ProportionalECRebalancer struct {
	ecNodes          []*EcNode
	replicaPlacement *super_block.ReplicaPlacement
	diskType         types.DiskType
	ecConfig         distribution.ECConfig
}

