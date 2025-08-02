package dash

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// GetClusterVolumes retrieves cluster volumes data with pagination, sorting, and filtering
func (s *AdminServer) GetClusterVolumes(page int, pageSize int, sortBy string, sortOrder string, collection string) (*ClusterVolumesData, error) {
	// Set defaults
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 1000 {
		pageSize = 100
	}
	if sortBy == "" {
		sortBy = "id"
	}
	if sortOrder == "" {
		sortOrder = "asc"
	}
	var volumes []VolumeWithTopology
	var totalSize int64

	// Get detailed volume information via gRPC
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
								volume := VolumeWithTopology{
									VolumeInformationMessage: volInfo,
									Server:                   node.Id,
									DataCenter:               dc.Id,
									Rack:                     rack.Id,
								}
								volumes = append(volumes, volume)
								totalSize += int64(volInfo.Size)
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

	// Add EC shard sizes to total size calculation
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		var ecTotalSize int64
		if resp.TopologyInfo != nil {
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								// Add all shard sizes for this EC volume
								for _, shardSize := range ecShardInfo.ShardSizes {
									ecTotalSize += shardSize
								}
							}
						}
					}
				}
			}
		}
		totalSize += ecTotalSize
		return nil
	})

	if err != nil {
		// If EC shard query fails, continue with basic data but log the error
		glog.Warningf("Failed to get EC shard information for size calculation: %v", err)
	}

	// Filter by collection if specified
	if collection != "" {
		var filteredVolumes []VolumeWithTopology
		var filteredTotalSize int64
		for _, volume := range volumes {
			// Handle "default" collection filtering for empty collections
			volumeCollection := volume.Collection
			if volumeCollection == "" {
				volumeCollection = "default"
			}

			if volumeCollection == collection {
				filteredVolumes = append(filteredVolumes, volume)
				filteredTotalSize += int64(volume.Size)
			}
		}

		// Also filter EC shard sizes by collection
		err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
			resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
			if err != nil {
				return err
			}

			var filteredEcTotalSize int64
			if resp.TopologyInfo != nil {
				for _, dc := range resp.TopologyInfo.DataCenterInfos {
					for _, rack := range dc.RackInfos {
						for _, node := range rack.DataNodeInfos {
							for _, diskInfo := range node.DiskInfos {
								for _, ecShardInfo := range diskInfo.EcShardInfos {
									// Handle "default" collection filtering for empty collections
									ecCollection := ecShardInfo.Collection
									if ecCollection == "" {
										ecCollection = "default"
									}

									if ecCollection == collection {
										// Add all shard sizes for this EC volume
										for _, shardSize := range ecShardInfo.ShardSizes {
											filteredEcTotalSize += shardSize
										}
									}
								}
							}
						}
					}
				}
			}
			filteredTotalSize += filteredEcTotalSize
			return nil
		})

		if err != nil {
			// If EC shard query fails, continue with basic data but log the error
			glog.Warningf("Failed to get filtered EC shard information: %v", err)
		}

		volumes = filteredVolumes
		totalSize = filteredTotalSize
	}

	// Calculate unique data center, rack, disk type, collection, and version counts from filtered volumes
	dataCenterMap := make(map[string]bool)
	rackMap := make(map[string]bool)
	diskTypeMap := make(map[string]bool)
	collectionMap := make(map[string]bool)
	versionMap := make(map[string]bool)
	for _, volume := range volumes {
		if volume.DataCenter != "" {
			dataCenterMap[volume.DataCenter] = true
		}
		if volume.Rack != "" {
			rackMap[volume.Rack] = true
		}
		diskType := volume.DiskType
		if diskType == "" {
			diskType = "hdd" // Default to hdd if not specified
		}
		diskTypeMap[diskType] = true

		// Handle collection for display purposes
		collectionName := volume.Collection
		if collectionName == "" {
			collectionName = "default"
		}
		collectionMap[collectionName] = true

		versionMap[fmt.Sprintf("%d", volume.Version)] = true
	}
	dataCenterCount := len(dataCenterMap)
	rackCount := len(rackMap)
	diskTypeCount := len(diskTypeMap)
	collectionCount := len(collectionMap)
	versionCount := len(versionMap)

	// Sort volumes
	s.sortVolumes(volumes, sortBy, sortOrder)

	// Get volume size limit from master
	var volumeSizeLimit uint64
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		volumeSizeLimit = uint64(resp.VolumeSizeLimitMB) * 1024 * 1024 // Convert MB to bytes
		return nil
	})
	if err != nil {
		// If we can't get the limit, set a default
		volumeSizeLimit = 30 * 1024 * 1024 * 1024 // 30GB default
	}

	// Calculate pagination
	totalVolumes := len(volumes)
	totalPages := (totalVolumes + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}

	// Apply pagination
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if startIndex >= totalVolumes {
		volumes = []VolumeWithTopology{}
	} else {
		if endIndex > totalVolumes {
			endIndex = totalVolumes
		}
		volumes = volumes[startIndex:endIndex]
	}

	// Determine conditional display flags and extract single values
	showDataCenterColumn := dataCenterCount > 1
	showRackColumn := rackCount > 1
	showDiskTypeColumn := diskTypeCount > 1
	showCollectionColumn := collectionCount > 1 && collection == "" // Hide column when filtering by collection
	showVersionColumn := versionCount > 1

	var singleDataCenter, singleRack, singleDiskType, singleCollection, singleVersion string
	var allVersions, allDiskTypes []string

	if dataCenterCount == 1 {
		for dc := range dataCenterMap {
			singleDataCenter = dc
			break
		}
	}
	if rackCount == 1 {
		for rack := range rackMap {
			singleRack = rack
			break
		}
	}
	if diskTypeCount == 1 {
		for diskType := range diskTypeMap {
			singleDiskType = diskType
			break
		}
	} else {
		// Collect all disk types and sort them
		for diskType := range diskTypeMap {
			allDiskTypes = append(allDiskTypes, diskType)
		}
		sort.Strings(allDiskTypes)
	}
	if collectionCount == 1 {
		for collection := range collectionMap {
			singleCollection = collection
			break
		}
	}
	if versionCount == 1 {
		for version := range versionMap {
			singleVersion = "v" + version
			break
		}
	} else {
		// Collect all versions and sort them
		for version := range versionMap {
			allVersions = append(allVersions, "v"+version)
		}
		sort.Strings(allVersions)
	}

	return &ClusterVolumesData{
		Volumes:              volumes,
		TotalVolumes:         totalVolumes,
		TotalSize:            totalSize,
		VolumeSizeLimit:      volumeSizeLimit,
		LastUpdated:          time.Now(),
		CurrentPage:          page,
		TotalPages:           totalPages,
		PageSize:             pageSize,
		SortBy:               sortBy,
		SortOrder:            sortOrder,
		DataCenterCount:      dataCenterCount,
		RackCount:            rackCount,
		DiskTypeCount:        diskTypeCount,
		CollectionCount:      collectionCount,
		VersionCount:         versionCount,
		ShowDataCenterColumn: showDataCenterColumn,
		ShowRackColumn:       showRackColumn,
		ShowDiskTypeColumn:   showDiskTypeColumn,
		ShowCollectionColumn: showCollectionColumn,
		ShowVersionColumn:    showVersionColumn,
		SingleDataCenter:     singleDataCenter,
		SingleRack:           singleRack,
		SingleDiskType:       singleDiskType,
		SingleCollection:     singleCollection,
		SingleVersion:        singleVersion,
		AllVersions:          allVersions,
		AllDiskTypes:         allDiskTypes,
		FilterCollection:     collection,
	}, nil
}

// sortVolumes sorts the volumes slice based on the specified field and order
func (s *AdminServer) sortVolumes(volumes []VolumeWithTopology, sortBy string, sortOrder string) {
	sort.Slice(volumes, func(i, j int) bool {
		var less bool

		switch sortBy {
		case "id":
			less = volumes[i].Id < volumes[j].Id
		case "server":
			less = volumes[i].Server < volumes[j].Server
		case "datacenter":
			less = volumes[i].DataCenter < volumes[j].DataCenter
		case "rack":
			less = volumes[i].Rack < volumes[j].Rack
		case "collection":
			less = volumes[i].Collection < volumes[j].Collection
		case "size":
			less = volumes[i].Size < volumes[j].Size
		case "filecount":
			less = volumes[i].FileCount < volumes[j].FileCount
		case "replication":
			less = volumes[i].ReplicaPlacement < volumes[j].ReplicaPlacement
		case "disktype":
			less = volumes[i].DiskType < volumes[j].DiskType
		case "version":
			less = volumes[i].Version < volumes[j].Version
		default:
			less = volumes[i].Id < volumes[j].Id
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

// GetVolumeDetails retrieves detailed information about a specific volume
func (s *AdminServer) GetVolumeDetails(volumeID int, server string) (*VolumeDetailsData, error) {
	var primaryVolume VolumeWithTopology
	var replicas []VolumeWithTopology
	var volumeSizeLimit uint64
	var found bool

	// Find the volume and all its replicas in the cluster
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
								if int(volInfo.Id) == volumeID {
									diskType := volInfo.DiskType
									if diskType == "" {
										diskType = "hdd"
									}

									volume := VolumeWithTopology{
										VolumeInformationMessage: volInfo,
										Server:                   node.Id,
										DataCenter:               dc.Id,
										Rack:                     rack.Id,
									}

									// If this is the requested server, it's the primary volume
									if node.Id == server {
										primaryVolume = volume
										found = true
									} else {
										// This is a replica on another server
										replicas = append(replicas, volume)
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

	if !found {
		return nil, fmt.Errorf("volume %d not found on server %s", volumeID, server)
	}

	// Get volume size limit from master
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		volumeSizeLimit = uint64(resp.VolumeSizeLimitMB) * 1024 * 1024 // Convert MB to bytes
		return nil
	})

	if err != nil {
		// If we can't get the limit, set a default
		volumeSizeLimit = 30 * 1024 * 1024 * 1024 // 30GB default
	}

	return &VolumeDetailsData{
		Volume:           primaryVolume,
		Replicas:         replicas,
		VolumeSizeLimit:  volumeSizeLimit,
		ReplicationCount: len(replicas) + 1, // Include the primary volume
		LastUpdated:      time.Now(),
	}, nil
}

// VacuumVolume performs a vacuum operation on a specific volume
func (s *AdminServer) VacuumVolume(volumeID int, server string) error {
	return s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		_, err := client.VacuumVolume(context.Background(), &master_pb.VacuumVolumeRequest{
			VolumeId:         uint32(volumeID),
			GarbageThreshold: 0.0001, // A very low threshold to ensure all garbage is collected
			Collection:       "",     // Empty for all collections
		})
		return err
	})
}

// GetClusterVolumeServers retrieves cluster volume servers data including EC shard information
func (s *AdminServer) GetClusterVolumeServers() (*ClusterVolumeServersData, error) {
	// Get basic topology first
	topology, err := s.GetClusterTopology()
	if err != nil {
		return nil, err
	}

	// Create a map for quick lookup and to add EC shard information
	volumeServerMap := make(map[string]*VolumeServer)
	for _, vs := range topology.VolumeServers {
		// Create a copy of the volume server
		vsCopy := vs
		vsCopy.EcVolumes = 0
		vsCopy.EcShards = 0
		vsCopy.EcShardDetails = []VolumeServerEcInfo{}
		volumeServerMap[vs.Address] = &vsCopy
	}

	// Get detailed EC shard information via gRPC
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						// Find the corresponding volume server
						vs, exists := volumeServerMap[node.Id]
						if !exists {
							continue
						}

						// Process EC shard information for this server
						ecVolumeMap := make(map[uint32]*VolumeServerEcInfo)

						for _, diskInfo := range node.DiskInfos {
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								volumeId := ecShardInfo.Id

								// Initialize or get existing EC info for this volume
								if ecVolumeMap[volumeId] == nil {
									ecVolumeMap[volumeId] = &VolumeServerEcInfo{
										VolumeID:     volumeId,
										Collection:   ecShardInfo.Collection,
										ShardCount:   0,
										EcIndexBits:  ecShardInfo.EcIndexBits,
										ShardNumbers: []int{},
										ShardSizes:   make(map[int]int64),
										TotalSize:    0,
									}
								}

								// Count shards and collect shard numbers with sizes
								shardBits := ecShardInfo.EcIndexBits
								for shardId := 0; shardId < 14; shardId++ { // EC uses 14 shards
									if (shardBits & (1 << uint(shardId))) != 0 {
										ecVolumeMap[volumeId].ShardCount++
										ecVolumeMap[volumeId].ShardNumbers = append(ecVolumeMap[volumeId].ShardNumbers, shardId)

										// Add shard size if available using optimized format
										shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
										if index, found := shardBits.ShardIdToIndex(erasure_coding.ShardId(shardId)); found && index < len(ecShardInfo.ShardSizes) {
											shardSize := ecShardInfo.ShardSizes[index]
											ecVolumeMap[volumeId].ShardSizes[shardId] = shardSize
											ecVolumeMap[volumeId].TotalSize += shardSize
										}

										vs.EcShards++
									}
								}
							}
						}

						// Convert map to slice and update volume server
						for _, ecInfo := range ecVolumeMap {
							vs.EcShardDetails = append(vs.EcShardDetails, *ecInfo)
							vs.EcVolumes++
						}
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		// If EC shard query fails, continue with basic data but log the error
		glog.Warningf("Failed to get EC shard information: %v", err)
	}

	// Add EC shard sizes to each volume server's DiskUsage
	for _, vs := range volumeServerMap {
		var ecTotalSizeForServer int64
		for _, ecInfo := range vs.EcShardDetails {
			ecTotalSizeForServer += ecInfo.TotalSize
		}
		vs.DiskUsage += ecTotalSizeForServer
	}

	// Convert map back to slice
	var volumeServers []VolumeServer
	for _, vs := range volumeServerMap {
		volumeServers = append(volumeServers, *vs)
	}

	var totalCapacity int64
	var totalVolumes int
	for _, vs := range volumeServers {
		totalCapacity += vs.DiskCapacity
		totalVolumes += vs.Volumes
	}

	return &ClusterVolumeServersData{
		VolumeServers:      volumeServers,
		TotalVolumeServers: len(volumeServers),
		TotalVolumes:       totalVolumes,
		TotalCapacity:      totalCapacity,
		LastUpdated:        time.Now(),
	}, nil
}
