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
	var totalEcVolumes int
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
							// Process regular volumes
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
										Name:          collectionName,
										DataCenter:    dc.Id,
										VolumeCount:   1,
										EcVolumeCount: 0,
										FileCount:     int64(volInfo.FileCount),
										TotalSize:     int64(volInfo.Size),
										DiskTypes:     []string{diskType},
									}
									collectionMap[collectionName] = &newCollection
									totalVolumes++
									totalFiles += int64(volInfo.FileCount)
									totalSize += int64(volInfo.Size)
								}
							}

							// Process EC volumes
							ecVolumeMap := make(map[uint32]bool) // Track unique EC volumes to avoid double counting
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								// Extract collection name from EC shard info
								collectionName := ecShardInfo.Collection
								if collectionName == "" {
									collectionName = "default" // Default collection for EC volumes without explicit collection
								}

								// Only count each EC volume once (not per shard)
								if !ecVolumeMap[ecShardInfo.Id] {
									ecVolumeMap[ecShardInfo.Id] = true

									// Get disk type from disk info, default to hdd if empty
									diskType := diskInfo.Type
									if diskType == "" {
										diskType = "hdd"
									}

									// Get or create collection info
									if collection, exists := collectionMap[collectionName]; exists {
										collection.EcVolumeCount++

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

										totalEcVolumes++
									} else {
										newCollection := CollectionInfo{
											Name:          collectionName,
											DataCenter:    dc.Id,
											VolumeCount:   0,
											EcVolumeCount: 1,
											FileCount:     0,
											TotalSize:     0,
											DiskTypes:     []string{diskType},
										}
										collectionMap[collectionName] = &newCollection
										totalEcVolumes++
									}
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
			TotalEcVolumes:   0,
			TotalFiles:       0,
			TotalSize:        0,
			LastUpdated:      time.Now(),
		}, nil
	}

	return &ClusterCollectionsData{
		Collections:      collections,
		TotalCollections: len(collections),
		TotalVolumes:     totalVolumes,
		TotalEcVolumes:   totalEcVolumes,
		TotalFiles:       totalFiles,
		TotalSize:        totalSize,
		LastUpdated:      time.Now(),
	}, nil
}

// GetCollectionDetails retrieves detailed information for a specific collection including volumes and EC volumes
func (s *AdminServer) GetCollectionDetails(collectionName string, page int, pageSize int, sortBy string, sortOrder string) (*CollectionDetailsData, error) {
	// Set defaults
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 25
	}
	if sortBy == "" {
		sortBy = "volume_id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	var regularVolumes []VolumeWithTopology
	var ecVolumes []EcVolumeWithShards
	var totalFiles int64
	var totalSize int64
	dataCenters := make(map[string]bool)
	diskTypes := make(map[string]bool)

	// Get regular volumes for this collection
	regularVolumeData, err := s.GetClusterVolumes(1, 10000, "volume_id", "asc", collectionName) // Get all volumes
	if err != nil {
		return nil, err
	}

	regularVolumes = regularVolumeData.Volumes
	totalSize = regularVolumeData.TotalSize

	// Calculate total files from regular volumes
	for _, vol := range regularVolumes {
		totalFiles += int64(vol.FileCount)
	}

	// Collect data centers and disk types from regular volumes
	for _, vol := range regularVolumes {
		dataCenters[vol.DataCenter] = true
		diskTypes[vol.DiskType] = true
	}

	// Get EC volumes for this collection
	ecVolumeData, err := s.GetClusterEcVolumes(1, 10000, "volume_id", "asc", collectionName) // Get all EC volumes
	if err != nil {
		return nil, err
	}

	ecVolumes = ecVolumeData.EcVolumes

	// Collect data centers from EC volumes
	for _, ecVol := range ecVolumes {
		for _, dc := range ecVol.DataCenters {
			dataCenters[dc] = true
		}
	}

	// Combine all volumes for sorting and pagination
	type VolumeForSorting struct {
		Type          string // "regular" or "ec"
		RegularVolume *VolumeWithTopology
		EcVolume      *EcVolumeWithShards
	}

	var allVolumes []VolumeForSorting
	for i := range regularVolumes {
		allVolumes = append(allVolumes, VolumeForSorting{
			Type:          "regular",
			RegularVolume: &regularVolumes[i],
		})
	}
	for i := range ecVolumes {
		allVolumes = append(allVolumes, VolumeForSorting{
			Type:     "ec",
			EcVolume: &ecVolumes[i],
		})
	}

	// Sort all volumes
	sort.Slice(allVolumes, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "volume_id":
			var idI, idJ uint32
			if allVolumes[i].Type == "regular" {
				idI = allVolumes[i].RegularVolume.Id
			} else {
				idI = allVolumes[i].EcVolume.VolumeID
			}
			if allVolumes[j].Type == "regular" {
				idJ = allVolumes[j].RegularVolume.Id
			} else {
				idJ = allVolumes[j].EcVolume.VolumeID
			}
			less = idI < idJ
		case "type":
			// Sort by type first (regular before ec), then by volume ID
			if allVolumes[i].Type == allVolumes[j].Type {
				var idI, idJ uint32
				if allVolumes[i].Type == "regular" {
					idI = allVolumes[i].RegularVolume.Id
				} else {
					idI = allVolumes[i].EcVolume.VolumeID
				}
				if allVolumes[j].Type == "regular" {
					idJ = allVolumes[j].RegularVolume.Id
				} else {
					idJ = allVolumes[j].EcVolume.VolumeID
				}
				less = idI < idJ
			} else {
				less = allVolumes[i].Type < allVolumes[j].Type // "ec" < "regular"
			}
		default:
			// Default to volume ID sort
			var idI, idJ uint32
			if allVolumes[i].Type == "regular" {
				idI = allVolumes[i].RegularVolume.Id
			} else {
				idI = allVolumes[i].EcVolume.VolumeID
			}
			if allVolumes[j].Type == "regular" {
				idJ = allVolumes[j].RegularVolume.Id
			} else {
				idJ = allVolumes[j].EcVolume.VolumeID
			}
			less = idI < idJ
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})

	// Apply pagination
	totalVolumesAndEc := len(allVolumes)
	totalPages := (totalVolumesAndEc + pageSize - 1) / pageSize
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalVolumesAndEc {
		endIndex = totalVolumesAndEc
	}

	if startIndex >= totalVolumesAndEc {
		startIndex = 0
		endIndex = 0
	}

	// Extract paginated results
	var paginatedRegularVolumes []VolumeWithTopology
	var paginatedEcVolumes []EcVolumeWithShards

	for i := startIndex; i < endIndex; i++ {
		if allVolumes[i].Type == "regular" {
			paginatedRegularVolumes = append(paginatedRegularVolumes, *allVolumes[i].RegularVolume)
		} else {
			paginatedEcVolumes = append(paginatedEcVolumes, *allVolumes[i].EcVolume)
		}
	}

	// Convert maps to slices
	var dcList []string
	for dc := range dataCenters {
		dcList = append(dcList, dc)
	}
	sort.Strings(dcList)

	var diskTypeList []string
	for diskType := range diskTypes {
		diskTypeList = append(diskTypeList, diskType)
	}
	sort.Strings(diskTypeList)

	return &CollectionDetailsData{
		CollectionName: collectionName,
		RegularVolumes: paginatedRegularVolumes,
		EcVolumes:      paginatedEcVolumes,
		TotalVolumes:   len(regularVolumes),
		TotalEcVolumes: len(ecVolumes),
		TotalFiles:     totalFiles,
		TotalSize:      totalSize,
		DataCenters:    dcList,
		DiskTypes:      diskTypeList,
		LastUpdated:    time.Now(),
		Page:           page,
		PageSize:       pageSize,
		TotalPages:     totalPages,
		SortBy:         sortBy,
		SortOrder:      sortOrder,
	}, nil
}
