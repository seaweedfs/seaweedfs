package dash

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// GetClusterEcShards retrieves cluster EC shards data with pagination, sorting, and filtering
func (s *AdminServer) GetClusterEcShards(page int, pageSize int, sortBy string, sortOrder string, collection string) (*ClusterEcShardsData, error) {
	// Set defaults
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 100
	}
	if sortBy == "" {
		sortBy = "volume_id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

	var ecShards []EcShardWithInfo
	shardsPerVolume := make(map[uint32]int)
	volumesWithAllShards := 0
	volumesWithMissingShards := 0

	// Get detailed EC shard information via gRPC
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
							// Process EC shard information
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								// Count shards per volume
								shardsPerVolume[ecShardInfo.Id] += getShardCount(ecShardInfo.EcIndexBits)

								// Create individual shard entries for each shard this server has
								shardBits := ecShardInfo.EcIndexBits
								for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
									if (shardBits & (1 << uint(shardId))) != 0 {
										ecShard := EcShardWithInfo{
											VolumeID:     ecShardInfo.Id,
											ShardID:      uint32(shardId),
											Collection:   ecShardInfo.Collection,
											Size:         0, // EC shards don't have individual size in the API response
											Server:       node.Id,
											DataCenter:   dc.Id,
											Rack:         rack.Id,
											DiskType:     diskInfo.Type,
											ModifiedTime: 0, // Not available in current API
											EcIndexBits:  ecShardInfo.EcIndexBits,
											ShardCount:   getShardCount(ecShardInfo.EcIndexBits),
										}
										ecShards = append(ecShards, ecShard)
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

	// Calculate completeness statistics
	for volumeId, shardCount := range shardsPerVolume {
		if shardCount == erasure_coding.TotalShardsCount {
			volumesWithAllShards++
		} else {
			volumesWithMissingShards++
		}

		// Update completeness info for each shard
		for i := range ecShards {
			if ecShards[i].VolumeID == volumeId {
				ecShards[i].IsComplete = (shardCount == erasure_coding.TotalShardsCount)
				if !ecShards[i].IsComplete {
					// Calculate missing shards
					ecShards[i].MissingShards = getMissingShards(ecShards[i].EcIndexBits)
				}
			}
		}
	}

	// Filter by collection if specified
	if collection != "" {
		var filteredShards []EcShardWithInfo
		for _, shard := range ecShards {
			if shard.Collection == collection {
				filteredShards = append(filteredShards, shard)
			}
		}
		ecShards = filteredShards
	}

	// Sort the results
	sortEcShards(ecShards, sortBy, sortOrder)

	// Calculate statistics for conditional display
	dataCenters := make(map[string]bool)
	racks := make(map[string]bool)
	collections := make(map[string]bool)

	for _, shard := range ecShards {
		dataCenters[shard.DataCenter] = true
		racks[shard.Rack] = true
		if shard.Collection != "" {
			collections[shard.Collection] = true
		}
	}

	// Pagination
	totalShards := len(ecShards)
	totalPages := (totalShards + pageSize - 1) / pageSize
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalShards {
		endIndex = totalShards
	}

	if startIndex >= totalShards {
		startIndex = 0
		endIndex = 0
	}

	paginatedShards := ecShards[startIndex:endIndex]

	// Build response
	data := &ClusterEcShardsData{
		EcShards:     paginatedShards,
		TotalShards:  totalShards,
		TotalVolumes: len(shardsPerVolume),
		LastUpdated:  time.Now(),

		// Pagination
		CurrentPage: page,
		TotalPages:  totalPages,
		PageSize:    pageSize,

		// Sorting
		SortBy:    sortBy,
		SortOrder: sortOrder,

		// Statistics
		DataCenterCount: len(dataCenters),
		RackCount:       len(racks),
		CollectionCount: len(collections),

		// Conditional display flags
		ShowDataCenterColumn: len(dataCenters) > 1,
		ShowRackColumn:       len(racks) > 1,
		ShowCollectionColumn: len(collections) > 1,

		// Filtering
		FilterCollection: collection,

		// EC specific statistics
		ShardsPerVolume:          shardsPerVolume,
		VolumesWithAllShards:     volumesWithAllShards,
		VolumesWithMissingShards: volumesWithMissingShards,
	}

	// Set single values when only one exists
	if len(dataCenters) == 1 {
		for dc := range dataCenters {
			data.SingleDataCenter = dc
			break
		}
	}
	if len(racks) == 1 {
		for rack := range racks {
			data.SingleRack = rack
			break
		}
	}
	if len(collections) == 1 {
		for col := range collections {
			data.SingleCollection = col
			break
		}
	}

	return data, nil
}

// getShardCount returns the number of shards represented by the bitmap
func getShardCount(ecIndexBits uint32) int {
	count := 0
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if (ecIndexBits & (1 << uint(i))) != 0 {
			count++
		}
	}
	return count
}

// getMissingShards returns a slice of missing shard IDs for a volume
func getMissingShards(ecIndexBits uint32) []int {
	var missing []int
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if (ecIndexBits & (1 << uint(i))) == 0 {
			missing = append(missing, i)
		}
	}
	return missing
}

// sortEcShards sorts EC shards based on the specified field and order
func sortEcShards(shards []EcShardWithInfo, sortBy string, sortOrder string) {
	sort.Slice(shards, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "volume_id":
			if shards[i].VolumeID == shards[j].VolumeID {
				less = shards[i].ShardID < shards[j].ShardID
			} else {
				less = shards[i].VolumeID < shards[j].VolumeID
			}
		case "shard_id":
			if shards[i].ShardID == shards[j].ShardID {
				less = shards[i].VolumeID < shards[j].VolumeID
			} else {
				less = shards[i].ShardID < shards[j].ShardID
			}
		case "collection":
			if shards[i].Collection == shards[j].Collection {
				less = shards[i].VolumeID < shards[j].VolumeID
			} else {
				less = shards[i].Collection < shards[j].Collection
			}
		case "server":
			if shards[i].Server == shards[j].Server {
				less = shards[i].VolumeID < shards[j].VolumeID
			} else {
				less = shards[i].Server < shards[j].Server
			}
		case "datacenter":
			if shards[i].DataCenter == shards[j].DataCenter {
				less = shards[i].VolumeID < shards[j].VolumeID
			} else {
				less = shards[i].DataCenter < shards[j].DataCenter
			}
		case "rack":
			if shards[i].Rack == shards[j].Rack {
				less = shards[i].VolumeID < shards[j].VolumeID
			} else {
				less = shards[i].Rack < shards[j].Rack
			}
		default:
			less = shards[i].VolumeID < shards[j].VolumeID
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// GetEcVolumeDetails retrieves detailed information about a specific EC volume
func (s *AdminServer) GetEcVolumeDetails(volumeID uint32) (*EcVolumeDetailsData, error) {
	var shards []EcShardWithInfo
	var collection string
	dataCenters := make(map[string]bool)
	servers := make(map[string]bool)

	// Get detailed EC shard information for the specific volume via gRPC
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
							// Process EC shard information for this specific volume
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								if ecShardInfo.Id == volumeID {
									collection = ecShardInfo.Collection
									dataCenters[dc.Id] = true
									servers[node.Id] = true

									// Create individual shard entries for each shard this server has
									shardBits := ecShardInfo.EcIndexBits
									for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
										if (shardBits & (1 << uint(shardId))) != 0 {
											ecShard := EcShardWithInfo{
												VolumeID:     ecShardInfo.Id,
												ShardID:      uint32(shardId),
												Collection:   ecShardInfo.Collection,
												Size:         0, // EC shards don't have individual size in the API response
												Server:       node.Id,
												DataCenter:   dc.Id,
												Rack:         rack.Id,
												DiskType:     diskInfo.Type,
												ModifiedTime: 0, // Not available in current API
												EcIndexBits:  ecShardInfo.EcIndexBits,
												ShardCount:   getShardCount(ecShardInfo.EcIndexBits),
											}
											shards = append(shards, ecShard)
										}
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

	if len(shards) == 0 {
		return nil, fmt.Errorf("EC volume %d not found", volumeID)
	}

	// Calculate completeness
	totalShards := len(shards)
	isComplete := (totalShards == erasure_coding.TotalShardsCount)

	// Calculate missing shards
	var missingShards []int
	foundShards := make(map[int]bool)
	for _, shard := range shards {
		foundShards[int(shard.ShardID)] = true
	}
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		if !foundShards[i] {
			missingShards = append(missingShards, i)
		}
	}

	// Update completeness info for each shard
	for i := range shards {
		shards[i].IsComplete = isComplete
		shards[i].MissingShards = missingShards
	}

	// Sort shards by ID
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ShardID < shards[j].ShardID
	})

	// Convert maps to slices
	var dcList []string
	for dc := range dataCenters {
		dcList = append(dcList, dc)
	}
	var serverList []string
	for server := range servers {
		serverList = append(serverList, server)
	}

	data := &EcVolumeDetailsData{
		VolumeID:      volumeID,
		Collection:    collection,
		Shards:        shards,
		TotalShards:   totalShards,
		IsComplete:    isComplete,
		MissingShards: missingShards,
		DataCenters:   dcList,
		Servers:       serverList,
		LastUpdated:   time.Now(),
	}

	return data, nil
}
