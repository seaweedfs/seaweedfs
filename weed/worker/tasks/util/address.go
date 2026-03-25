package util

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// ResolveServerAddress resolves a server ID to its network address using the active topology
func ResolveServerAddress(serverID string, activeTopology *topology.ActiveTopology) (string, error) {
	if activeTopology == nil {
		return "", fmt.Errorf("topology not available")
	}
	allNodes := activeTopology.GetAllNodes()
	nodeInfo, exists := allNodes[serverID]
	if !exists {
		return "", fmt.Errorf("server %s not found in topology", serverID)
	}
	// Note: We use NewServerAddressFromDataNode here because it encodes the GRPC port into the server address
	// if it is non-zero, avoiding the default +10000 GRPC port offset.
	return string(pb.NewServerAddressFromDataNode(nodeInfo)), nil
}
