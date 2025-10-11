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
	// Both empty means default collection matches default filter
	if volumeCollection == "" && filterCollection == "" {
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
								for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
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
										VolumeID:               volumeId,
										Collection:             ecShardInfo.Collection,
										TotalShards:            0,
										IsComplete:             false,
										MissingShards:          []int{},
										ShardLocations:         make(map[int]string),
										ShardSizes:             make(map[int]int64),
										DataCenters:            []string{},
										Servers:                []string{},
										Racks:                  []string{},
										Generations:            []uint32{},
										ActiveGeneration:       0,
										HasMultipleGenerations: false,
									}
								}

								volume := volumeData[volumeId]

								// Track generation information
								generationExists := false
								for _, existingGen := range volume.Generations {
									if existingGen == ecShardInfo.Generation {
										generationExists = true
										break
									}
								}
								if !generationExists {
									volume.Generations = append(volume.Generations, ecShardInfo.Generation)
								}

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
								for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
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

	// Get active generation information from master for each volume
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		for volumeId, volume := range volumeData {
			// Look up active generation
			resp, lookupErr := client.LookupEcVolume(context.Background(), &master_pb.LookupEcVolumeRequest{
				VolumeId: volumeId,
			})
			if lookupErr == nil && resp != nil {
				volume.ActiveGeneration = resp.ActiveGeneration
			}

			// Sort generations and check for multiple generations
			if len(volume.Generations) > 1 {
				// Sort generations (oldest first)
				sort.Slice(volume.Generations, func(i, j int) bool {
					return volume.Generations[i] < volume.Generations[j]
				})
				volume.HasMultipleGenerations = true
			}
		}
		return nil // Don't fail if lookup fails
	})

	// Calculate completeness for each volume
	completeVolumes := 0
	incompleteVolumes := 0

	for _, volume := range volumeData {
		volume.TotalShards = len(volume.ShardLocations)

		// Find missing shards
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
												Generation:   ecShardInfo.Generation, // Include generation information
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

	// Get EC volume health metrics (deletion information)
	volumeHealth, err := s.getEcVolumeHealthMetrics(volumeID)
	if err != nil {
		glog.V(0).Infof("ERROR: Failed to get EC volume health metrics for volume %d: %v", volumeID, err)
		// Don't fail the request, just use default values
		volumeHealth = &EcVolumeHealthInfo{
			TotalSize:        0,
			DeletedByteCount: 0,
			FileCount:        0,
			DeleteCount:      0,
			GarbageRatio:     0.0,
		}
	}

	// Analyze generation information
	generationMap := make(map[uint32]bool)
	generationShards := make(map[uint32][]uint32)
	generationComplete := make(map[uint32]bool)

	// Collect all generations and group shards by generation
	for _, shard := range shards {
		generationMap[shard.Generation] = true
		generationShards[shard.Generation] = append(generationShards[shard.Generation], shard.ShardID)
	}

	// Convert generation map to sorted slice
	var generations []uint32
	for gen := range generationMap {
		generations = append(generations, gen)
	}

	// Sort generations (oldest first)
	sort.Slice(generations, func(i, j int) bool {
		return generations[i] < generations[j]
	})

	// Check completion status for each generation
	for gen, shardIDs := range generationShards {
		generationComplete[gen] = len(shardIDs) == erasure_coding.TotalShardsCount
	}

	// Get active generation from master
	var activeGeneration uint32
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		// Use LookupEcVolume to get active generation
		resp, lookupErr := client.LookupEcVolume(context.Background(), &master_pb.LookupEcVolumeRequest{
			VolumeId: volumeID,
		})
		if lookupErr == nil && resp != nil {
			activeGeneration = resp.ActiveGeneration
		}
		return nil // Don't fail if lookup fails, just use generation 0 as default
	})

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

		// Volume health metrics (for EC vacuum)
		TotalSize:        volumeHealth.TotalSize,
		DeletedByteCount: volumeHealth.DeletedByteCount,
		FileCount:        volumeHealth.FileCount,
		DeleteCount:      volumeHealth.DeleteCount,
		GarbageRatio:     volumeHealth.GarbageRatio,

		// Generation information
		Generations:        generations,
		ActiveGeneration:   activeGeneration,
		GenerationShards:   generationShards,
		GenerationComplete: generationComplete,

		SortBy:    sortBy,
		SortOrder: sortOrder,
	}

	return data, nil
}

// getEcVolumeHealthMetrics retrieves health metrics for an EC volume
func (s *AdminServer) getEcVolumeHealthMetrics(volumeID uint32) (*EcVolumeHealthInfo, error) {
	glog.V(0).Infof("DEBUG: getEcVolumeHealthMetrics called for volume %d", volumeID)
	// Get list of servers that have shards for this EC volume
	var servers []string

	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			serverSet := make(map[string]struct{})
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
							// Check if this node has EC shards for our volume
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								if ecShardInfo.Id == volumeID {
									serverSet[node.Id] = struct{}{}
								}
							}
						}
					}
				}
			}
			for server := range serverSet {
				servers = append(servers, server)
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get topology info: %v", err)
	}

	glog.V(0).Infof("DEBUG: Found %d servers with EC shards for volume %d: %v", len(servers), volumeID, servers)
	if len(servers) == 0 {
		return nil, fmt.Errorf("no servers found with EC shards for volume %d", volumeID)
	}

	// Aggregate health metrics from ALL servers that have EC shards
	var aggregatedHealth *EcVolumeHealthInfo
	var totalSize uint64
	var totalFileCount uint64
	var totalDeletedBytes uint64
	var totalDeletedCount uint64
	validServers := 0

	for _, server := range servers {
		healthInfo, err := s.getVolumeHealthFromServer(server, volumeID)
		if err != nil {
			glog.V(2).Infof("Failed to get volume health from server %s for volume %d: %v", server, volumeID, err)
			continue // Try next server
		}
		glog.V(0).Infof("DEBUG: getVolumeHealthFromServer returned for %s: healthInfo=%v", server, healthInfo != nil)
		if healthInfo != nil {
			// Sum the values across all servers (each server contributes its shard data)
			totalSize += healthInfo.TotalSize
			totalFileCount += healthInfo.FileCount
			totalDeletedBytes += healthInfo.DeletedByteCount
			totalDeletedCount += healthInfo.DeleteCount
			validServers++

			glog.V(0).Infof("DEBUG: Added server %s data: size=%d, files=%d, deleted_bytes=%d", server, healthInfo.TotalSize, healthInfo.FileCount, healthInfo.DeletedByteCount)

			// Store first non-nil health info as template for aggregated result
			if aggregatedHealth == nil {
				aggregatedHealth = &EcVolumeHealthInfo{}
			}
		}
	}

	// If we got aggregated data, finalize it
	glog.V(0).Infof("DEBUG: Aggregation check - aggregatedHealth=%v, validServers=%d", aggregatedHealth != nil, validServers)
	if aggregatedHealth != nil && validServers > 0 {
		// Use summed totals from all servers
		aggregatedHealth.TotalSize = totalSize
		aggregatedHealth.FileCount = totalFileCount
		aggregatedHealth.DeletedByteCount = totalDeletedBytes
		aggregatedHealth.DeleteCount = totalDeletedCount

		// Calculate garbage ratio from aggregated data
		if aggregatedHealth.TotalSize > 0 {
			aggregatedHealth.GarbageRatio = float64(aggregatedHealth.DeletedByteCount) / float64(aggregatedHealth.TotalSize)
		}

		glog.V(0).Infof("SUCCESS: Aggregated EC volume %d from %d servers: %d total bytes -> %d MB",
			volumeID, validServers, totalSize, totalSize/1024/1024)

		return aggregatedHealth, nil
	}

	// If we can't get the original metrics, try to calculate from EC shards
	return s.calculateHealthFromEcShards(volumeID, servers)
}

// getVolumeHealthFromServer gets volume health information from a specific server
func (s *AdminServer) getVolumeHealthFromServer(server string, volumeID uint32) (*EcVolumeHealthInfo, error) {
	var healthInfo *EcVolumeHealthInfo

	err := s.WithVolumeServerClient(pb.ServerAddress(server), func(client volume_server_pb.VolumeServerClient) error {
		var collection string = "" // Default collection name
		var totalSize uint64 = 0
		var fileCount uint64 = 0

		// Try to get volume file status (which may include original volume metrics)
		// This will fail for EC-only volumes, so we handle that gracefully
		resp, err := client.ReadVolumeFileStatus(context.Background(), &volume_server_pb.ReadVolumeFileStatusRequest{
			VolumeId: volumeID,
		})
		if err != nil {
			glog.V(2).Infof("ReadVolumeFileStatus failed for EC volume %d on server %s (expected for EC-only volumes): %v", volumeID, server, err)
			// For EC-only volumes, we don't have original volume metrics, but we can still get deletion info
		} else if resp.VolumeInfo != nil {
			// Extract metrics from regular volume info if available
			totalSize = uint64(resp.VolumeInfo.DatFileSize)
			fileCount = resp.FileCount
			collection = resp.Collection
		}

		// Always try to get EC deletion information using the new gRPC endpoint
		deletionResp, deletionErr := client.VolumeEcDeletionInfo(context.Background(), &volume_server_pb.VolumeEcDeletionInfoRequest{
			VolumeId:   volumeID,
			Collection: collection,
			Generation: 0, // Use default generation for backward compatibility
		})

		if deletionErr != nil {
			glog.V(1).Infof("Failed to get EC deletion info for volume %d on server %s: %v", volumeID, server, deletionErr)
			// If we have some info from ReadVolumeFileStatus, still create healthInfo with that
			if totalSize > 0 {
				healthInfo = &EcVolumeHealthInfo{
					TotalSize:        totalSize,
					DeletedByteCount: 0,
					FileCount:        fileCount,
					DeleteCount:      0,
					GarbageRatio:     0.0,
				}
			}
		} else if deletionResp != nil {
			// Create health info with deletion data
			healthInfo = &EcVolumeHealthInfo{
				TotalSize:        deletionResp.TotalSize, // Get total size from EC deletion info
				DeletedByteCount: deletionResp.DeletedBytes,
				FileCount:        fileCount,
				DeleteCount:      deletionResp.DeletedCount,
				GarbageRatio:     0.0,
			}

			// Calculate garbage ratio if we have total size
			if healthInfo.TotalSize > 0 {
				healthInfo.GarbageRatio = float64(healthInfo.DeletedByteCount) / float64(healthInfo.TotalSize)
			}

			glog.V(1).Infof("EC volume %d on server %s: %d deleted bytes, %d deleted needles, total size: %d bytes",
				volumeID, server, healthInfo.DeletedByteCount, healthInfo.DeleteCount, healthInfo.TotalSize)
		}

		return nil // Return from WithVolumeServerClient callback - healthInfo is captured by closure
	})

	return healthInfo, err
}

// calculateHealthFromEcShards attempts to calculate health metrics from EC shard information
func (s *AdminServer) calculateHealthFromEcShards(volumeID uint32, servers []string) (*EcVolumeHealthInfo, error) {
	var totalShardSize uint64
	shardCount := 0

	// Get shard sizes from all servers
	for _, server := range servers {
		err := s.WithVolumeServerClient(pb.ServerAddress(server), func(client volume_server_pb.VolumeServerClient) error {
			resp, err := client.VolumeEcShardsInfo(context.Background(), &volume_server_pb.VolumeEcShardsInfoRequest{
				VolumeId: volumeID,
			})
			if err != nil {
				return err
			}

			for _, shardInfo := range resp.EcShardInfos {
				totalShardSize += uint64(shardInfo.Size)
				shardCount++
			}

			return nil
		})
		if err != nil {
			glog.V(2).Infof("Failed to get EC shard info from server %s: %v", server, err)
		}
	}

	if shardCount == 0 {
		return nil, fmt.Errorf("no EC shard information found for volume %d", volumeID)
	}

	// For EC volumes, we can estimate the original size from the data shards
	// EC uses 10 data shards + 4 parity shards = 14 total
	// The original volume size is approximately the sum of the 10 data shards
	dataShardCount := 10 // erasure_coding.DataShardsCount
	estimatedOriginalSize := totalShardSize

	if shardCount >= dataShardCount {
		// If we have info from data shards, estimate better
		avgShardSize := totalShardSize / uint64(shardCount)
		estimatedOriginalSize = avgShardSize * uint64(dataShardCount)
	}

	return &EcVolumeHealthInfo{
		TotalSize:        estimatedOriginalSize,
		DeletedByteCount: 0, // Cannot determine from EC shards alone
		FileCount:        0, // Cannot determine from EC shards alone
		DeleteCount:      0, // Cannot determine from EC shards alone
		GarbageRatio:     0.0,
	}, nil
}
