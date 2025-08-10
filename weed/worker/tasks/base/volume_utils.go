package base

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
)

// FindVolumeDisk finds the disk ID where a specific volume is located on a given server.
// Returns the disk ID and a boolean indicating whether the volume was found.
// Uses O(1) indexed lookup for optimal performance on large clusters.
//
// This is a shared utility function used by multiple task detection algorithms
// to locate volumes efficiently.
//
// Example usage:
//
//	// Find source disk for a volume that needs to be processed
//	sourceDisk, found := base.FindVolumeDisk(topology, volumeID, collection, sourceServer)
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
