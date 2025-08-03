package base

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
)

// FindVolumeDisk finds the disk ID where a specific volume is located on a given server.
// Returns the disk ID and a boolean indicating whether the volume was found.
// Uses O(1) indexed lookup for optimal performance on large clusters.
//
// This is a shared utility function used by multiple task detection algorithms
// (balance, vacuum, etc.) to locate volumes efficiently.
//
// Example usage:
//
//	// In balance task: find source disk for a volume that needs to be moved
//	sourceDisk, found := base.FindVolumeDisk(topology, volumeID, collection, sourceServer)
//
//	// In vacuum task: find disk containing volume that needs cleanup
//	diskID, exists := base.FindVolumeDisk(topology, volumeID, collection, serverID)
func FindVolumeDisk(activeTopology *topology.ActiveTopology, volumeID uint32, collection string, serverID string) (uint32, bool) {
	if activeTopology == nil {
		return 0, false
	}

	// Use the new O(1) indexed lookup for better performance
	locations := activeTopology.GetVolumeLocations(volumeID, collection)
	for _, loc := range locations {
		if loc.ServerID == serverID {
			return loc.DiskID, true
		}
	}

	// Volume not found on this server
	return 0, false
}
