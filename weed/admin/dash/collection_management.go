package dash

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// GetClusterCollections retrieves cluster collections data
func (s *AdminServer) GetClusterCollections() (*ClusterCollectionsData, error) {
	var collections []CollectionInfo
	var totalVolumes int
	var totalFiles int64
	var totalSize int64
	collectionMap := make(map[string]*CollectionInfo)

	// Get actual collection information from volume data
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
							for _, volInfo := range diskInfo.VolumeInfos {
								// Extract collection name from volume info
								collectionName := volInfo.Collection
								if collectionName == "" {
									collectionName = "default" // Default collection for volumes without explicit collection
								}

								// Get disk type from volume info, default to hdd if empty
								diskType := volInfo.DiskType
								if diskType == "" {
									diskType = "hdd"
								}

								// Get or create collection info
								if collection, exists := collectionMap[collectionName]; exists {
									collection.VolumeCount++
									collection.FileCount += int64(volInfo.FileCount)
									collection.TotalSize += int64(volInfo.Size)

									// Update data center if this collection spans multiple DCs
									if collection.DataCenter != dc.Id && collection.DataCenter != "multi" {
										collection.DataCenter = "multi"
									}

									// Add disk type if not already present
									diskTypeExists := false
									for _, existingDiskType := range collection.DiskTypes {
										if existingDiskType == diskType {
											diskTypeExists = true
											break
										}
									}
									if !diskTypeExists {
										collection.DiskTypes = append(collection.DiskTypes, diskType)
									}

									totalVolumes++
									totalFiles += int64(volInfo.FileCount)
									totalSize += int64(volInfo.Size)
								} else {
									newCollection := CollectionInfo{
										Name:        collectionName,
										DataCenter:  dc.Id,
										VolumeCount: 1,
										FileCount:   int64(volInfo.FileCount),
										TotalSize:   int64(volInfo.Size),
										DiskTypes:   []string{diskType},
									}
									collectionMap[collectionName] = &newCollection
									totalVolumes++
									totalFiles += int64(volInfo.FileCount)
									totalSize += int64(volInfo.Size)
								}
							}
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Convert map to slice
	for _, collection := range collectionMap {
		collections = append(collections, *collection)
	}

	// Sort collections alphabetically by name
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].Name < collections[j].Name
	})

	// If no collections found, show a message indicating no collections exist
	if len(collections) == 0 {
		// Return empty collections data instead of creating fake ones
		return &ClusterCollectionsData{
			Collections:      []CollectionInfo{},
			TotalCollections: 0,
			TotalVolumes:     0,
			TotalFiles:       0,
			TotalSize:        0,
			LastUpdated:      time.Now(),
		}, nil
	}

	return &ClusterCollectionsData{
		Collections:      collections,
		TotalCollections: len(collections),
		TotalVolumes:     totalVolumes,
		TotalFiles:       totalFiles,
		TotalSize:        totalSize,
		LastUpdated:      time.Now(),
	}, nil
}
