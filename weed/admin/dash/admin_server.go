package dash

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type AdminServer struct {
	masterAddress   string
	templateFS      http.FileSystem
	grpcDialOption  grpc.DialOption
	cacheExpiration time.Duration
	lastCacheUpdate time.Time
	cachedTopology  *ClusterTopology

	// Filer discovery and caching
	cachedFilers         []string
	lastFilerUpdate      time.Time
	filerCacheExpiration time.Duration
}

type ClusterTopology struct {
	Masters       []MasterNode   `json:"masters"`
	DataCenters   []DataCenter   `json:"datacenters"`
	VolumeServers []VolumeServer `json:"volume_servers"`
	TotalVolumes  int            `json:"total_volumes"`
	TotalFiles    int64          `json:"total_files"`
	TotalSize     int64          `json:"total_size"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

type MasterNode struct {
	Address  string `json:"address"`
	IsLeader bool   `json:"is_leader"`
	Status   string `json:"status"`
}

type DataCenter struct {
	ID    string `json:"id"`
	Racks []Rack `json:"racks"`
}

type Rack struct {
	ID    string         `json:"id"`
	Nodes []VolumeServer `json:"nodes"`
}

type VolumeServer struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	DataCenter    string    `json:"datacenter"`
	Rack          string    `json:"rack"`
	PublicURL     string    `json:"public_url"`
	Volumes       int       `json:"volumes"`
	MaxVolumes    int       `json:"max_volumes"`
	DiskUsage     int64     `json:"disk_usage"`
	DiskCapacity  int64     `json:"disk_capacity"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Status        string    `json:"status"`
}

// S3 Bucket management structures
type S3Bucket struct {
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	Size         int64     `json:"size"`
	ObjectCount  int64     `json:"object_count"`
	LastModified time.Time `json:"last_modified"`
	Status       string    `json:"status"`
	Quota        int64     `json:"quota"`         // Quota in bytes, 0 means no quota
	QuotaEnabled bool      `json:"quota_enabled"` // Whether quota is enabled
}

type S3Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag"`
	StorageClass string    `json:"storage_class"`
}

type BucketDetails struct {
	Bucket     S3Bucket   `json:"bucket"`
	Objects    []S3Object `json:"objects"`
	TotalSize  int64      `json:"total_size"`
	TotalCount int64      `json:"total_count"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

// Cluster management data structures
type ClusterVolumeServersData struct {
	Username           string         `json:"username"`
	VolumeServers      []VolumeServer `json:"volume_servers"`
	TotalVolumeServers int            `json:"total_volume_servers"`
	TotalVolumes       int            `json:"total_volumes"`
	TotalCapacity      int64          `json:"total_capacity"`
	LastUpdated        time.Time      `json:"last_updated"`
}

type VolumeInfo struct {
	ID          int    `json:"id"`
	Server      string `json:"server"`
	DataCenter  string `json:"datacenter"`
	Rack        string `json:"rack"`
	Collection  string `json:"collection"`
	Size        int64  `json:"size"`
	FileCount   int64  `json:"file_count"`
	Replication string `json:"replication"`
	Status      string `json:"status"`
}

type ClusterVolumesData struct {
	Username     string       `json:"username"`
	Volumes      []VolumeInfo `json:"volumes"`
	TotalVolumes int          `json:"total_volumes"`
	TotalSize    int64        `json:"total_size"`
	LastUpdated  time.Time    `json:"last_updated"`

	// Pagination
	CurrentPage int `json:"current_page"`
	TotalPages  int `json:"total_pages"`
	PageSize    int `json:"page_size"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`
}

type CollectionInfo struct {
	Name        string   `json:"name"`
	DataCenter  string   `json:"datacenter"`
	Replication string   `json:"replication"`
	VolumeCount int      `json:"volume_count"`
	FileCount   int64    `json:"file_count"`
	TotalSize   int64    `json:"total_size"`
	DiskTypes   []string `json:"disk_types"`
	Status      string   `json:"status"`
}

type ClusterCollectionsData struct {
	Username         string           `json:"username"`
	Collections      []CollectionInfo `json:"collections"`
	TotalCollections int              `json:"total_collections"`
	TotalVolumes     int              `json:"total_volumes"`
	TotalFiles       int64            `json:"total_files"`
	TotalSize        int64            `json:"total_size"`
	LastUpdated      time.Time        `json:"last_updated"`
}

type MasterInfo struct {
	Address  string `json:"address"`
	IsLeader bool   `json:"is_leader"`
	Status   string `json:"status"`
	Suffrage string `json:"suffrage"`
}

type ClusterMastersData struct {
	Username     string       `json:"username"`
	Masters      []MasterInfo `json:"masters"`
	TotalMasters int          `json:"total_masters"`
	LeaderCount  int          `json:"leader_count"`
	LastUpdated  time.Time    `json:"last_updated"`
}

type FilerInfo struct {
	Address    string    `json:"address"`
	DataCenter string    `json:"datacenter"`
	Rack       string    `json:"rack"`
	Version    string    `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	Status     string    `json:"status"`
}

type ClusterFilersData struct {
	Username    string      `json:"username"`
	Filers      []FilerInfo `json:"filers"`
	TotalFilers int         `json:"total_filers"`
	LastUpdated time.Time   `json:"last_updated"`
}

func NewAdminServer(masterAddress string, templateFS http.FileSystem) *AdminServer {
	return &AdminServer{
		masterAddress:        masterAddress,
		templateFS:           templateFS,
		grpcDialOption:       security.LoadClientTLS(util.GetViper(), "grpc.client"),
		cacheExpiration:      10 * time.Second,
		filerCacheExpiration: 30 * time.Second, // Cache filers for 30 seconds
	}
}

// GetFilerAddress returns a filer address, discovering from masters if needed
func (s *AdminServer) GetFilerAddress() string {
	// Discover filers from masters
	filers := s.getDiscoveredFilers()
	if len(filers) > 0 {
		return filers[0] // Return the first available filer
	}

	return ""
}

// getDiscoveredFilers returns cached filers or discovers them from masters
func (s *AdminServer) getDiscoveredFilers() []string {
	// Check if cache is still valid
	if time.Since(s.lastFilerUpdate) < s.filerCacheExpiration && len(s.cachedFilers) > 0 {
		return s.cachedFilers
	}

	// Discover filers from masters
	var filers []string
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})
		if err != nil {
			return err
		}

		for _, node := range resp.ClusterNodes {
			filers = append(filers, node.Address)
		}

		return nil
	})

	if err != nil {
		glog.Warningf("Failed to discover filers from master %s: %v", s.masterAddress, err)
		// Return cached filers even if expired, better than nothing
		return s.cachedFilers
	}

	// Update cache
	s.cachedFilers = filers
	s.lastFilerUpdate = time.Now()

	return filers
}

// WithMasterClient executes a function with a master client connection
func (s *AdminServer) WithMasterClient(f func(client master_pb.SeaweedClient) error) error {
	masterAddr := pb.ServerAddress(s.masterAddress)

	return pb.WithMasterClient(false, masterAddr, s.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		return f(client)
	})
}

// WithFilerClient executes a function with a filer client connection
func (s *AdminServer) WithFilerClient(f func(client filer_pb.SeaweedFilerClient) error) error {
	filerAddr := s.GetFilerAddress()
	if filerAddr == "" {
		return fmt.Errorf("no filer available")
	}

	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(filerAddr), s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return f(client)
	})
}

// WithVolumeServerClient executes a function with a volume server client connection
func (s *AdminServer) WithVolumeServerClient(address pb.ServerAddress, f func(client volume_server_pb.VolumeServerClient) error) error {
	return operation.WithVolumeServerClient(false, address, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		return f(client)
	})
}

// GetClusterTopology returns the current cluster topology with caching
func (s *AdminServer) GetClusterTopology() (*ClusterTopology, error) {
	now := time.Now()
	if s.cachedTopology != nil && now.Sub(s.lastCacheUpdate) < s.cacheExpiration {
		return s.cachedTopology, nil
	}

	topology := &ClusterTopology{
		UpdatedAt: now,
	}

	// Use gRPC only
	err := s.getTopologyViaGRPC(topology)
	if err != nil {
		glog.Errorf("Failed to connect to master server %s: %v", s.masterAddress, err)
		return nil, fmt.Errorf("gRPC topology request failed: %v", err)
	}

	// Cache the result
	s.cachedTopology = topology
	s.lastCacheUpdate = now

	return topology, nil
}

// getTopologyViaGRPC gets topology using gRPC (original method)
func (s *AdminServer) getTopologyViaGRPC(topology *ClusterTopology) error {
	// Get cluster status from master
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			glog.Errorf("Failed to get volume list from master %s: %v", s.masterAddress, err)
			return err
		}

		if resp.TopologyInfo != nil {
			// Process gRPC response
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				dataCenter := DataCenter{
					ID:    dc.Id,
					Racks: []Rack{},
				}

				for _, rack := range dc.RackInfos {
					rackObj := Rack{
						ID:    rack.Id,
						Nodes: []VolumeServer{},
					}

					for _, node := range rack.DataNodeInfos {
						// Calculate totals from disk infos
						var totalVolumes int64
						var totalMaxVolumes int64
						var totalSize int64
						var totalFiles int64

						for _, diskInfo := range node.DiskInfos {
							totalVolumes += diskInfo.VolumeCount
							totalMaxVolumes += diskInfo.MaxVolumeCount

							// Sum up individual volume information
							for _, volInfo := range diskInfo.VolumeInfos {
								totalSize += int64(volInfo.Size)
								totalFiles += int64(volInfo.FileCount)
							}
						}

						vs := VolumeServer{
							ID:            node.Id,
							Address:       node.Id,
							DataCenter:    dc.Id,
							Rack:          rack.Id,
							PublicURL:     node.Id,
							Volumes:       int(totalVolumes),
							MaxVolumes:    int(totalMaxVolumes),
							DiskUsage:     totalSize,
							DiskCapacity:  totalMaxVolumes * int64(resp.VolumeSizeLimitMb) * 1024 * 1024,
							LastHeartbeat: time.Now(),
							Status:        "active",
						}

						rackObj.Nodes = append(rackObj.Nodes, vs)
						topology.VolumeServers = append(topology.VolumeServers, vs)
						topology.TotalVolumes += vs.Volumes
						topology.TotalFiles += totalFiles
						topology.TotalSize += totalSize
					}

					dataCenter.Racks = append(dataCenter.Racks, rackObj)
				}

				topology.DataCenters = append(topology.DataCenters, dataCenter)
			}
		}

		return nil
	})

	return err
}

// InvalidateCache forces a refresh of cached data
func (s *AdminServer) InvalidateCache() {
	s.lastCacheUpdate = time.Time{}
	s.cachedTopology = nil
	s.lastFilerUpdate = time.Time{}
	s.cachedFilers = nil
}

// GetS3Buckets retrieves all Object Store buckets from the filer and collects size/object data from collections
func (s *AdminServer) GetS3Buckets() ([]S3Bucket, error) {
	var buckets []S3Bucket

	// Build a map of collection name to collection data
	collectionMap := make(map[string]struct {
		Size      int64
		FileCount int64
	})

	// Collect volume information by collection
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
								collection := volInfo.Collection
								if collection == "" {
									collection = "default"
								}

								if _, exists := collectionMap[collection]; !exists {
									collectionMap[collection] = struct {
										Size      int64
										FileCount int64
									}{}
								}

								data := collectionMap[collection]
								data.Size += int64(volInfo.Size)
								data.FileCount += int64(volInfo.FileCount)
								collectionMap[collection] = data
							}
						}
					}
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get volume information: %v", err)
	}

	// Get filer configuration to determine FilerGroup
	var filerGroup string
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		configResp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			glog.Warningf("Failed to get filer configuration: %v", err)
			// Continue without filer group
			return nil
		}
		filerGroup = configResp.FilerGroup
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get filer configuration: %v", err)
	}

	// Now list buckets from the filer and match with collection data
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List buckets by looking at the /buckets directory
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          "/buckets",
			Prefix:             "",
			StartFromFileName:  "",
			InclusiveStartFrom: false,
			Limit:              1000,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				return err
			}

			if resp.Entry.IsDirectory {
				bucketName := resp.Entry.Name

				// Determine collection name for this bucket
				var collectionName string
				if filerGroup != "" {
					collectionName = fmt.Sprintf("%s_%s", filerGroup, bucketName)
				} else {
					collectionName = bucketName
				}

				// Get size and object count from collection data
				var size int64
				var objectCount int64
				if collectionData, exists := collectionMap[collectionName]; exists {
					size = collectionData.Size
					objectCount = collectionData.FileCount
				}

				// Get quota information from entry
				quota := resp.Entry.Quota
				quotaEnabled := quota > 0
				if quota < 0 {
					// Negative quota means disabled
					quota = -quota
					quotaEnabled = false
				}

				bucket := S3Bucket{
					Name:         bucketName,
					CreatedAt:    time.Unix(resp.Entry.Attributes.Crtime, 0),
					Size:         size,
					ObjectCount:  objectCount,
					LastModified: time.Unix(resp.Entry.Attributes.Mtime, 0),
					Status:       "active",
					Quota:        quota,
					QuotaEnabled: quotaEnabled,
				}
				buckets = append(buckets, bucket)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list Object Store buckets: %v", err)
	}

	return buckets, nil
}

// GetBucketDetails retrieves detailed information about a specific bucket
func (s *AdminServer) GetBucketDetails(bucketName string) (*BucketDetails, error) {
	bucketPath := fmt.Sprintf("/buckets/%s", bucketName)

	details := &BucketDetails{
		Bucket: S3Bucket{
			Name:   bucketName,
			Status: "active",
		},
		Objects:   []S3Object{},
		UpdatedAt: time.Now(),
	}

	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Get bucket info
		bucketResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/buckets",
			Name:      bucketName,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %v", err)
		}

		details.Bucket.CreatedAt = time.Unix(bucketResp.Entry.Attributes.Crtime, 0)
		details.Bucket.LastModified = time.Unix(bucketResp.Entry.Attributes.Mtime, 0)

		// List objects in bucket (recursively)
		return s.listBucketObjects(client, bucketPath, "", details)
	})

	if err != nil {
		return nil, err
	}

	return details, nil
}

// listBucketObjects recursively lists all objects in a bucket
func (s *AdminServer) listBucketObjects(client filer_pb.SeaweedFilerClient, directory, prefix string, details *BucketDetails) error {
	stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
		Directory:          directory,
		Prefix:             prefix,
		StartFromFileName:  "",
		InclusiveStartFrom: false,
		Limit:              1000,
	})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}

		entry := resp.Entry
		if entry.IsDirectory {
			// Recursively list subdirectories
			subDir := fmt.Sprintf("%s/%s", directory, entry.Name)
			err := s.listBucketObjects(client, subDir, "", details)
			if err != nil {
				return err
			}
		} else {
			// Add file object
			objectKey := entry.Name
			if directory != fmt.Sprintf("/buckets/%s", details.Bucket.Name) {
				// Remove bucket prefix to get relative path
				relativePath := directory[len(fmt.Sprintf("/buckets/%s", details.Bucket.Name))+1:]
				objectKey = fmt.Sprintf("%s/%s", relativePath, entry.Name)
			}

			obj := S3Object{
				Key:          objectKey,
				Size:         int64(entry.Attributes.FileSize),
				LastModified: time.Unix(entry.Attributes.Mtime, 0),
				ETag:         "", // Could be calculated from chunks if needed
				StorageClass: "STANDARD",
			}

			details.Objects = append(details.Objects, obj)
			details.TotalSize += obj.Size
			details.TotalCount++
		}
	}

	// Update bucket totals
	details.Bucket.Size = details.TotalSize
	details.Bucket.ObjectCount = details.TotalCount

	return nil
}

// CreateS3Bucket creates a new S3 bucket
func (s *AdminServer) CreateS3Bucket(bucketName string) error {
	return s.CreateS3BucketWithQuota(bucketName, 0, false)
}

// DeleteS3Bucket deletes an S3 bucket and all its contents
func (s *AdminServer) DeleteS3Bucket(bucketName string) error {
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Delete bucket directory recursively
		_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:            "/buckets",
			Name:                 bucketName,
			IsDeleteData:         true,
			IsRecursive:          true,
			IgnoreRecursiveError: false,
		})
		if err != nil {
			return fmt.Errorf("failed to delete bucket: %v", err)
		}

		return nil
	})
}

// GetObjectStoreUsers retrieves object store users data
func (s *AdminServer) GetObjectStoreUsers() ([]ObjectStoreUser, error) {
	// For now, return mock data since SeaweedFS doesn't have built-in user management
	// In a real implementation, this would query the IAM system or user database
	users := []ObjectStoreUser{
		{
			Username:    "admin",
			Email:       "admin@example.com",
			AccessKey:   "AKIAIOSFODNN7EXAMPLE",
			SecretKey:   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			Status:      "active",
			CreatedAt:   time.Now().AddDate(0, -1, 0),
			LastLogin:   time.Now().AddDate(0, 0, -1),
			Permissions: []string{"s3:*", "iam:*"},
		},
		{
			Username:    "readonly",
			Email:       "readonly@example.com",
			AccessKey:   "AKIAI44QH8DHBEXAMPLE",
			SecretKey:   "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY",
			Status:      "active",
			CreatedAt:   time.Now().AddDate(0, -2, 0),
			LastLogin:   time.Now().AddDate(0, 0, -3),
			Permissions: []string{"s3:GetObject", "s3:ListBucket"},
		},
		{
			Username:    "backup",
			Email:       "backup@example.com",
			AccessKey:   "AKIAIGCEVSQ6C2EXAMPLE",
			SecretKey:   "BnL1dIqRF/+WoWcouZ5e3qthJhEXAMPLEKEY",
			Status:      "inactive",
			CreatedAt:   time.Now().AddDate(0, -3, 0),
			LastLogin:   time.Now().AddDate(0, -1, -15),
			Permissions: []string{"s3:PutObject", "s3:GetObject"},
		},
	}

	return users, nil
}

// GetClusterVolumeServers retrieves cluster volume servers data
func (s *AdminServer) GetClusterVolumeServers() (*ClusterVolumeServersData, error) {
	topology, err := s.GetClusterTopology()
	if err != nil {
		return nil, err
	}

	var totalCapacity int64
	var totalVolumes int
	for _, vs := range topology.VolumeServers {
		totalCapacity += vs.DiskCapacity
		totalVolumes += vs.Volumes
	}

	return &ClusterVolumeServersData{
		VolumeServers:      topology.VolumeServers,
		TotalVolumeServers: len(topology.VolumeServers),
		TotalVolumes:       totalVolumes,
		TotalCapacity:      totalCapacity,
		LastUpdated:        time.Now(),
	}, nil
}

// GetClusterVolumes retrieves cluster volumes data with pagination and sorting
func (s *AdminServer) GetClusterVolumes(page int, pageSize int, sortBy string, sortOrder string) (*ClusterVolumesData, error) {
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
	var volumes []VolumeInfo
	var totalSize int64
	volumeID := 1

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
								// Extract collection name from volume info
								collectionName := volInfo.Collection
								if collectionName == "" {
									collectionName = "default" // Default collection for volumes without explicit collection
								}

								volume := VolumeInfo{
									ID:          volumeID,
									Server:      node.Id,
									DataCenter:  dc.Id,
									Rack:        rack.Id,
									Collection:  collectionName,
									Size:        int64(volInfo.Size),
									FileCount:   int64(volInfo.FileCount),
									Replication: fmt.Sprintf("%03d", volInfo.ReplicaPlacement),
									Status:      "active",
								}
								volumes = append(volumes, volume)
								totalSize += volume.Size
								volumeID++
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

	// Sort volumes
	s.sortVolumes(volumes, sortBy, sortOrder)

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
		volumes = []VolumeInfo{}
	} else {
		if endIndex > totalVolumes {
			endIndex = totalVolumes
		}
		volumes = volumes[startIndex:endIndex]
	}

	return &ClusterVolumesData{
		Volumes:      volumes,
		TotalVolumes: totalVolumes,
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
		CurrentPage:  page,
		TotalPages:   totalPages,
		PageSize:     pageSize,
		SortBy:       sortBy,
		SortOrder:    sortOrder,
	}, nil
}

// sortVolumes sorts the volumes slice based on the specified field and order
func (s *AdminServer) sortVolumes(volumes []VolumeInfo, sortBy string, sortOrder string) {
	sort.Slice(volumes, func(i, j int) bool {
		var less bool

		switch sortBy {
		case "id":
			less = volumes[i].ID < volumes[j].ID
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
			less = volumes[i].Replication < volumes[j].Replication
		case "status":
			less = volumes[i].Status < volumes[j].Status
		default:
			less = volumes[i].ID < volumes[j].ID
		}

		if sortOrder == "desc" {
			return !less
		}
		return less
	})
}

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
										Replication: fmt.Sprintf("%03d", volInfo.ReplicaPlacement),
										VolumeCount: 1,
										FileCount:   int64(volInfo.FileCount),
										TotalSize:   int64(volInfo.Size),
										DiskTypes:   []string{diskType},
										Status:      "active",
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

// GetClusterMasters retrieves cluster masters data
func (s *AdminServer) GetClusterMasters() (*ClusterMastersData, error) {
	var masters []MasterInfo
	var leaderCount int

	// First, get master information from topology
	topology, err := s.GetClusterTopology()
	if err != nil {
		return nil, err
	}

	// Create a map to merge topology and raft data
	masterMap := make(map[string]*MasterInfo)

	// Add masters from topology
	for _, master := range topology.Masters {
		masterInfo := &MasterInfo{
			Address:  master.Address,
			IsLeader: master.IsLeader,
			Status:   master.Status,
			Suffrage: "",
		}

		if master.IsLeader {
			leaderCount++
		}

		masterMap[master.Address] = masterInfo
	}

	// Then, get additional master information from Raft cluster
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.RaftListClusterServers(context.Background(), &master_pb.RaftListClusterServersRequest{})
		if err != nil {
			return err
		}

		// Process each raft server
		for _, server := range resp.ClusterServers {
			address := server.Address

			// Update existing master info or create new one
			if masterInfo, exists := masterMap[address]; exists {
				// Update existing master with raft data
				masterInfo.IsLeader = server.IsLeader
				masterInfo.Suffrage = server.Suffrage
				masterInfo.Status = "active" // If it's in raft cluster, it's active
			} else {
				// Create new master info from raft data
				masterInfo := &MasterInfo{
					Address:  address,
					IsLeader: server.IsLeader,
					Status:   "active",
					Suffrage: server.Suffrage,
				}
				masterMap[address] = masterInfo
			}

			if server.IsLeader {
				// Update leader count based on raft data
				leaderCount = 1 // There should only be one leader
			}
		}

		return nil
	})

	if err != nil {
		// If gRPC call fails, log the error but continue with topology data
		glog.Errorf("Failed to get raft cluster servers from master %s: %v", s.masterAddress, err)
	}

	// Convert map to slice
	for _, masterInfo := range masterMap {
		masters = append(masters, *masterInfo)
	}

	// If no masters found at all, add the configured master as fallback
	if len(masters) == 0 {
		masters = append(masters, MasterInfo{
			Address:  s.masterAddress,
			IsLeader: true,
			Status:   "active",
			Suffrage: "Voter",
		})
		leaderCount = 1
	}

	return &ClusterMastersData{
		Masters:      masters,
		TotalMasters: len(masters),
		LeaderCount:  leaderCount,
		LastUpdated:  time.Now(),
	}, nil
}

// GetClusterFilers retrieves cluster filers data
func (s *AdminServer) GetClusterFilers() (*ClusterFilersData, error) {
	var filers []FilerInfo

	// Get filer information from master using ListClusterNodes
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
		})
		if err != nil {
			return err
		}

		// Process each filer node
		for _, node := range resp.ClusterNodes {
			createdAt := time.Unix(0, node.CreatedAtNs)

			filerInfo := FilerInfo{
				Address:    node.Address,
				DataCenter: node.DataCenter,
				Rack:       node.Rack,
				Version:    node.Version,
				CreatedAt:  createdAt,
				Status:     "active", // If it's in the cluster list, it's considered active
			}

			filers = append(filers, filerInfo)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get filer nodes from master: %v", err)
	}

	return &ClusterFilersData{
		Filers:      filers,
		TotalFilers: len(filers),
		LastUpdated: time.Now(),
	}, nil
}

// GetAllFilers returns all discovered filers
func (s *AdminServer) GetAllFilers() []string {
	return s.getDiscoveredFilers()
}
