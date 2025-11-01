package dash

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// matchesCollection checks if a volume/EC volume collection matches the filter collection.
// Handles the special case where empty collection ("") represents the "default" collection.
func matchesCollection(volumeCollection, filterCollection string) bool {
	// Handle special case where "default" filter matches empty collection
	if filterCollection == "default" && volumeCollection == "" {
		return true
	}
	// Direct string match for named collections
	return volumeCollection == filterCollection
}

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
	volumeShardsMap := make(map[uint32]map[int]bool) // volumeId -> set of shards present
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
								volumeId := ecShardInfo.Id

								// Initialize volume shards map if needed
								if volumeShardsMap[volumeId] == nil {
									volumeShardsMap[volumeId] = make(map[int]bool)
								}

								// Create individual shard entries for each shard this server has
								shardBits := ecShardInfo.EcIndexBits
								for shardId := 0; shardId < erasure_coding.MaxShardCount; shardId++ {
									if (shardBits & (1 << uint(shardId))) != 0 {
										// Mark this shard as present for this volume
										volumeShardsMap[volumeId][shardId] = true

										ecShard := EcShardWithInfo{
											VolumeID:     volumeId,
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

	// Calculate volume-level completeness (across all servers)
	volumeCompleteness := make(map[uint32]bool)
	volumeMissingShards := make(map[uint32][]int)

	for volumeId, shardsPresent := range volumeShardsMap {
		var missingShards []int
		shardCount := len(shardsPresent)

		// Find which shards are missing for this volume across ALL servers
		// Uses default 10+4 (14 total shards)
		for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
			if !shardsPresent[shardId] {
				missingShards = append(missingShards, shardId)
			}
		}

		isComplete := (shardCount == erasure_coding.TotalShardsCount)
		volumeCompleteness[volumeId] = isComplete
		volumeMissingShards[volumeId] = missingShards

		if isComplete {
			volumesWithAllShards++
		} else {
			volumesWithMissingShards++
		}
	}

	// Update completeness info for each shard based on volume-level completeness
	for i := range ecShards {
		volumeId := ecShards[i].VolumeID
		ecShards[i].IsComplete = volumeCompleteness[volumeId]
		ecShards[i].MissingShards = volumeMissingShards[volumeId]
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
		TotalVolumes: len(volumeShardsMap),
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
		ShowCollectionColumn: len(collections) > 1 || collection != "",

		// Filtering
		FilterCollection: collection,

		// EC specific statistics
		ShardsPerVolume:          make(map[uint32]int), // This will be recalculated below
		VolumesWithAllShards:     volumesWithAllShards,
		VolumesWithMissingShards: volumesWithMissingShards,
	}

	// Recalculate ShardsPerVolume for the response
	for volumeId, shardsPresent := range volumeShardsMap {
		data.ShardsPerVolume[volumeId] = len(shardsPresent)
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

// GetClusterEcVolumes retrieves cluster EC volumes data grouped by volume ID with shard locations
func (s *AdminServer) GetClusterEcVolumes(page int, pageSize int, sortBy string, sortOrder string, collection string) (*ClusterEcVolumesData, error) {
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

	volumeData := make(map[uint32]*EcVolumeWithShards)
	totalShards := 0

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
								volumeId := ecShardInfo.Id

								// Initialize volume data if needed
								if volumeData[volumeId] == nil {
									volumeData[volumeId] = &EcVolumeWithShards{
										VolumeID:       volumeId,
										Collection:     ecShardInfo.Collection,
										TotalShards:    0,
										IsComplete:     false,
										MissingShards:  []int{},
										ShardLocations: make(map[int]string),
										ShardSizes:     make(map[int]int64),
										DataCenters:    []string{},
										Servers:        []string{},
										Racks:          []string{},
									}
								}

								volume := volumeData[volumeId]

								// Track data centers and servers
								dcExists := false
								for _, existingDc := range volume.DataCenters {
									if existingDc == dc.Id {
										dcExists = true
										break
									}
								}
								if !dcExists {
									volume.DataCenters = append(volume.DataCenters, dc.Id)
								}

								serverExists := false
								for _, existingServer := range volume.Servers {
									if existingServer == node.Id {
										serverExists = true
										break
									}
								}
								if !serverExists {
									volume.Servers = append(volume.Servers, node.Id)
								}

								// Track racks
								rackExists := false
								for _, existingRack := range volume.Racks {
									if existingRack == rack.Id {
										rackExists = true
										break
									}
								}
								if !rackExists {
									volume.Racks = append(volume.Racks, rack.Id)
								}

								// Process each shard this server has for this volume
								shardBits := ecShardInfo.EcIndexBits
								for shardId := 0; shardId < erasure_coding.MaxShardCount; shardId++ {
									if (shardBits & (1 << uint(shardId))) != 0 {
										// Record shard location
										volume.ShardLocations[shardId] = node.Id
										totalShards++
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

	// Collect shard size information from volume servers
	for volumeId, volume := range volumeData {
		// Group servers by volume to minimize gRPC calls
		serverHasVolume := make(map[string]bool)
		for _, server := range volume.Servers {
			serverHasVolume[server] = true
		}

		// Query each server for shard sizes
		for server := range serverHasVolume {
			err := s.WithVolumeServerClient(pb.ServerAddress(server), func(client volume_server_pb.VolumeServerClient) error {
				resp, err := client.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{
					VolumeId: volumeId,
				})
				if err != nil {
					glog.V(1).Infof("Failed to get EC shard info from %s for volume %d: %v", server, volumeId, err)
					return nil // Continue with other servers, don't fail the entire request
				}

				// Update shard sizes
				for _, shardInfo := range resp.EcShardInfos {
					volume.ShardSizes[int(shardInfo.ShardId)] = shardInfo.Size
				}

				return nil
			})
			if err != nil {
				glog.V(1).Infof("Failed to connect to volume server %s: %v", server, err)
			}
		}
	}

	// Calculate completeness for each volume
	completeVolumes := 0
	incompleteVolumes := 0

	for _, volume := range volumeData {
		volume.TotalShards = len(volume.ShardLocations)

		// Find missing shards (default 10+4 = 14 total shards)
		var missingShards []int
		for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
			if _, exists := volume.ShardLocations[shardId]; !exists {
				missingShards = append(missingShards, shardId)
			}
		}

		volume.MissingShards = missingShards
		volume.IsComplete = (len(missingShards) == 0)

		if volume.IsComplete {
			completeVolumes++
		} else {
			incompleteVolumes++
		}
	}

	// Convert map to slice
	var ecVolumes []EcVolumeWithShards
	for _, volume := range volumeData {
		// Filter by collection if specified
		if collection == "" || matchesCollection(volume.Collection, collection) {
			ecVolumes = append(ecVolumes, *volume)
		}
	}

	// Sort the results
	sortEcVolumes(ecVolumes, sortBy, sortOrder)

	// Calculate statistics for conditional display
	dataCenters := make(map[string]bool)
	collections := make(map[string]bool)

	for _, volume := range ecVolumes {
		for _, dc := range volume.DataCenters {
			dataCenters[dc] = true
		}
		if volume.Collection != "" {
			collections[volume.Collection] = true
		}
	}

	// Pagination
	totalVolumes := len(ecVolumes)
	totalPages := (totalVolumes + pageSize - 1) / pageSize
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalVolumes {
		endIndex = totalVolumes
	}

	if startIndex >= totalVolumes {
		startIndex = 0
		endIndex = 0
	}

	paginatedVolumes := ecVolumes[startIndex:endIndex]

	// Build response
	data := &ClusterEcVolumesData{
		EcVolumes:    paginatedVolumes,
		TotalVolumes: totalVolumes,
		LastUpdated:  time.Now(),

		// Pagination
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,

		// Sorting
		SortBy:    sortBy,
		SortOrder: sortOrder,

		// Filtering
		Collection: collection,

		// Conditional display flags
		ShowDataCenterColumn: len(dataCenters) > 1,
		ShowRackColumn:       false, // We don't track racks in this view for simplicity
		ShowCollectionColumn: len(collections) > 1 || collection != "",

		// Statistics
		CompleteVolumes:   completeVolumes,
		IncompleteVolumes: incompleteVolumes,
		TotalShards:       totalShards,
	}

	return data, nil
}

// sortEcVolumes sorts EC volumes based on the specified field and order
func sortEcVolumes(volumes []EcVolumeWithShards, sortBy string, sortOrder string) {
	sort.Slice(volumes, func(i, j int) bool {
		var less bool
		switch sortBy {
		case "volume_id":
			less = volumes[i].VolumeID < volumes[j].VolumeID
		case "collection":
			if volumes[i].Collection == volumes[j].Collection {
				less = volumes[i].VolumeID < volumes[j].VolumeID
			} else {
				less = volumes[i].Collection < volumes[j].Collection
			}
		case "total_shards":
			if volumes[i].TotalShards == volumes[j].TotalShards {
				less = volumes[i].VolumeID < volumes[j].VolumeID
			} else {
				less = volumes[i].TotalShards < volumes[j].TotalShards
			}
		case "completeness":
			// Complete volumes first, then by volume ID
			if volumes[i].IsComplete == volumes[j].IsComplete {
				less = volumes[i].VolumeID < volumes[j].VolumeID
			} else {
				less = volumes[i].IsComplete && !volumes[j].IsComplete
			}
		default:
			less = volumes[i].VolumeID < volumes[j].VolumeID
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// getShardCount returns the number of shards represented by the bitmap
func getShardCount(ecIndexBits uint32) int {
	count := 0
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		if (ecIndexBits & (1 << uint(i))) != 0 {
			count++
		}
	}
	return count
}

// getMissingShards returns a slice of missing shard IDs for a volume
// Assumes default 10+4 EC configuration (14 total shards)
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
		case "shard_id":
			less = shards[i].ShardID < shards[j].ShardID
		case "server":
			if shards[i].Server == shards[j].Server {
				less = shards[i].ShardID < shards[j].ShardID // Secondary sort by shard ID
			} else {
				less = shards[i].Server < shards[j].Server
			}
		case "data_center":
			if shards[i].DataCenter == shards[j].DataCenter {
				less = shards[i].ShardID < shards[j].ShardID // Secondary sort by shard ID
			} else {
				less = shards[i].DataCenter < shards[j].DataCenter
			}
		case "rack":
			if shards[i].Rack == shards[j].Rack {
				less = shards[i].ShardID < shards[j].ShardID // Secondary sort by shard ID
			} else {
				less = shards[i].Rack < shards[j].Rack
			}
		default:
			less = shards[i].ShardID < shards[j].ShardID
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// GetEcVolumeDetails retrieves detailed information about a specific EC volume
func (s *AdminServer) GetEcVolumeDetails(volumeID uint32, sortBy string, sortOrder string) (*EcVolumeDetailsData, error) {
	// Set defaults
	if sortBy == "" {
		sortBy = "shard_id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}

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
									for shardId := 0; shardId < erasure_coding.MaxShardCount; shardId++ {
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

	// Collect shard size information from volume servers
	shardSizeMap := make(map[string]map[uint32]uint64) // server -> shardId -> size
	for _, shard := range shards {
		server := shard.Server
		if _, exists := shardSizeMap[server]; !exists {
			// Query this server for shard sizes
			err := s.WithVolumeServerClient(pb.ServerAddress(server), func(client volume_server_pb.VolumeServerClient) error {
				resp, err := client.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{
					VolumeId: volumeID,
				})
				if err != nil {
					glog.V(1).Infof("Failed to get EC shard info from %s for volume %d: %v", server, volumeID, err)
					return nil // Continue with other servers, don't fail the entire request
				}

				// Store shard sizes for this server
				shardSizeMap[server] = make(map[uint32]uint64)
				for _, shardInfo := range resp.EcShardInfos {
					shardSizeMap[server][shardInfo.ShardId] = uint64(shardInfo.Size)
				}

				return nil
			})
			if err != nil {
				glog.V(1).Infof("Failed to connect to volume server %s: %v", server, err)
			}
		}
	}

	// Update shard sizes in the shards array
	for i := range shards {
		server := shards[i].Server
		shardId := shards[i].ShardID
		if serverSizes, exists := shardSizeMap[server]; exists {
			if size, exists := serverSizes[shardId]; exists {
				shards[i].Size = size
			}
		}
	}

	// Calculate completeness based on unique shard IDs
	foundShards := make(map[int]bool)
	for _, shard := range shards {
		foundShards[int(shard.ShardID)] = true
	}

	totalUniqueShards := len(foundShards)
	// Check completeness using default 10+4 (14 total shards)
	isComplete := (totalUniqueShards == erasure_coding.TotalShardsCount)

	// Calculate missing shards
	var missingShards []int
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

	// Sort shards based on parameters
	sortEcShards(shards, sortBy, sortOrder)

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
		TotalShards:   totalUniqueShards,
		IsComplete:    isComplete,
		MissingShards: missingShards,
		DataCenters:   dcList,
		Servers:       serverList,
		LastUpdated:   time.Now(),
		SortBy:        sortBy,
		SortOrder:     sortOrder,
	}

	return data, nil
}
