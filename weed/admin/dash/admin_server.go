package dash

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
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
	filerAddress    string
	templateFS      http.FileSystem
	grpcDialOption  grpc.DialOption
	cacheExpiration time.Duration
	lastCacheUpdate time.Time
	cachedTopology  *ClusterTopology
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
	Region       string    `json:"region"`
	Status       string    `json:"status"`
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
}

type CollectionInfo struct {
	Name        string `json:"name"`
	DataCenter  string `json:"datacenter"`
	Replication string `json:"replication"`
	VolumeCount int    `json:"volume_count"`
	TTL         string `json:"ttl"`
	DiskType    string `json:"disk_type"`
	Status      string `json:"status"`
}

type ClusterCollectionsData struct {
	Username         string           `json:"username"`
	Collections      []CollectionInfo `json:"collections"`
	TotalCollections int              `json:"total_collections"`
	TotalVolumes     int              `json:"total_volumes"`
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

type FileEntry struct {
	Name        string    `json:"name"`
	FullPath    string    `json:"full_path"`
	IsDirectory bool      `json:"is_directory"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"mod_time"`
	Mode        string    `json:"mode"`
	Uid         uint32    `json:"uid"`
	Gid         uint32    `json:"gid"`
	Mime        string    `json:"mime"`
	Replication string    `json:"replication"`
	Collection  string    `json:"collection"`
	TtlSec      int32     `json:"ttl_sec"`
}

type BreadcrumbItem struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type FileBrowserData struct {
	Username     string           `json:"username"`
	CurrentPath  string           `json:"current_path"`
	ParentPath   string           `json:"parent_path"`
	Breadcrumbs  []BreadcrumbItem `json:"breadcrumbs"`
	Entries      []FileEntry      `json:"entries"`
	TotalEntries int              `json:"total_entries"`
	TotalSize    int64            `json:"total_size"`
	LastUpdated  time.Time        `json:"last_updated"`
}

func NewAdminServer(masterAddress, filerAddress string, templateFS http.FileSystem) *AdminServer {
	return &AdminServer{
		masterAddress:   masterAddress,
		filerAddress:    filerAddress,
		templateFS:      templateFS,
		grpcDialOption:  security.LoadClientTLS(util.GetViper(), "grpc.client"),
		cacheExpiration: 10 * time.Second,
	}
}

// GetFilerAddress returns the filer address
func (s *AdminServer) GetFilerAddress() string {
	return s.filerAddress
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
	return pb.WithGrpcFilerClient(false, 0, pb.ServerAddress(s.filerAddress), s.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
}

// GetS3Buckets retrieves all S3 buckets from the filer
func (s *AdminServer) GetS3Buckets() ([]S3Bucket, error) {
	var buckets []S3Bucket

	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
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
				bucket := S3Bucket{
					Name:         resp.Entry.Name,
					CreatedAt:    time.Unix(resp.Entry.Attributes.Crtime, 0),
					Size:         0, // Will be calculated if needed
					ObjectCount:  0, // Will be calculated if needed
					LastModified: time.Unix(resp.Entry.Attributes.Mtime, 0),
					Region:       "us-east-1", // Default region
					Status:       "active",
				}
				buckets = append(buckets, bucket)
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list S3 buckets: %v", err)
	}

	return buckets, nil
}

// GetBucketDetails retrieves detailed information about a specific bucket
func (s *AdminServer) GetBucketDetails(bucketName string) (*BucketDetails, error) {
	bucketPath := fmt.Sprintf("/buckets/%s", bucketName)

	details := &BucketDetails{
		Bucket: S3Bucket{
			Name:   bucketName,
			Region: "us-east-1",
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
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Create bucket directory
		_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/buckets",
			Entry: &filer_pb.Entry{
				Name:        bucketName,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileMode: 0755,
					Crtime:   time.Now().Unix(),
					Mtime:    time.Now().Unix(),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket: %v", err)
		}

		return nil
	})
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

// GetClusterVolumes retrieves cluster volumes data
func (s *AdminServer) GetClusterVolumes() (*ClusterVolumesData, error) {
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

	return &ClusterVolumesData{
		Volumes:      volumes,
		TotalVolumes: len(volumes),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}, nil
}

// GetClusterCollections retrieves cluster collections data
func (s *AdminServer) GetClusterCollections() (*ClusterCollectionsData, error) {
	var collections []CollectionInfo
	var totalVolumes int
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
								// Collection name is typically stored in volume metadata
								collectionName := volInfo.Collection
								if collectionName == "" {
									collectionName = "default" // Default collection for volumes without explicit collection
								}

								// Get or create collection info
								if collection, exists := collectionMap[collectionName]; exists {
									collection.VolumeCount++
									totalVolumes++
								} else {
									newCollection := CollectionInfo{
										Name:        collectionName,
										DataCenter:  dc.Id,
										Replication: fmt.Sprintf("%03d", volInfo.ReplicaPlacement),
										VolumeCount: 1,
										TTL:         fmt.Sprintf("%d", volInfo.Ttl),
										DiskType:    "hdd", // Default disk type
										Status:      "active",
									}
									collectionMap[collectionName] = &newCollection
									totalVolumes++
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
			LastUpdated:      time.Now(),
		}, nil
	}

	return &ClusterCollectionsData{
		Collections:      collections,
		TotalCollections: len(collections),
		TotalVolumes:     totalVolumes,
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

// GetFileBrowser retrieves file browser data for a given path
func (s *AdminServer) GetFileBrowser(path string) (*FileBrowserData, error) {
	if path == "" {
		path = "/"
	}

	var entries []FileEntry
	var totalSize int64

	// Get directory listing from filer
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          path,
			Prefix:             "",
			Limit:              1000,
			InclusiveStartFrom: false,
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
			if entry == nil {
				continue
			}

			fullPath := path
			if !strings.HasSuffix(fullPath, "/") {
				fullPath += "/"
			}
			fullPath += entry.Name

			var modTime time.Time
			if entry.Attributes != nil && entry.Attributes.Mtime > 0 {
				modTime = time.Unix(entry.Attributes.Mtime, 0)
			}

			var mode string
			var uid, gid uint32
			var size int64
			var replication, collection string
			var ttlSec int32

			if entry.Attributes != nil {
				mode = fmt.Sprintf("%o", entry.Attributes.FileMode)
				uid = entry.Attributes.Uid
				gid = entry.Attributes.Gid
				size = int64(entry.Attributes.FileSize)
				ttlSec = entry.Attributes.TtlSec
			}

			// Get replication and collection from entry extended attributes or chunks
			if entry.Extended != nil {
				if repl, ok := entry.Extended["replication"]; ok {
					replication = string(repl)
				}
				if coll, ok := entry.Extended["collection"]; ok {
					collection = string(coll)
				}
			}

			// Determine MIME type based on file extension
			mime := "application/octet-stream"
			if entry.IsDirectory {
				mime = "inode/directory"
			} else {
				ext := strings.ToLower(filepath.Ext(entry.Name))
				switch ext {
				case ".txt", ".log":
					mime = "text/plain"
				case ".html", ".htm":
					mime = "text/html"
				case ".css":
					mime = "text/css"
				case ".js":
					mime = "application/javascript"
				case ".json":
					mime = "application/json"
				case ".xml":
					mime = "application/xml"
				case ".pdf":
					mime = "application/pdf"
				case ".jpg", ".jpeg":
					mime = "image/jpeg"
				case ".png":
					mime = "image/png"
				case ".gif":
					mime = "image/gif"
				case ".svg":
					mime = "image/svg+xml"
				case ".mp4":
					mime = "video/mp4"
				case ".mp3":
					mime = "audio/mpeg"
				case ".zip":
					mime = "application/zip"
				case ".tar":
					mime = "application/x-tar"
				case ".gz":
					mime = "application/gzip"
				}
			}

			fileEntry := FileEntry{
				Name:        entry.Name,
				FullPath:    fullPath,
				IsDirectory: entry.IsDirectory,
				Size:        size,
				ModTime:     modTime,
				Mode:        mode,
				Uid:         uid,
				Gid:         gid,
				Mime:        mime,
				Replication: replication,
				Collection:  collection,
				TtlSec:      ttlSec,
			}

			entries = append(entries, fileEntry)
			if !entry.IsDirectory {
				totalSize += size
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort entries: directories first, then files, both alphabetically
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDirectory != entries[j].IsDirectory {
			return entries[i].IsDirectory
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})

	// Generate breadcrumbs
	breadcrumbs := s.generateBreadcrumbs(path)

	// Calculate parent path
	parentPath := "/"
	if path != "/" {
		parentPath = filepath.Dir(path)
		if parentPath == "." {
			parentPath = "/"
		}
	}

	return &FileBrowserData{
		CurrentPath:  path,
		ParentPath:   parentPath,
		Breadcrumbs:  breadcrumbs,
		Entries:      entries,
		TotalEntries: len(entries),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}, nil
}

// generateBreadcrumbs creates breadcrumb navigation for the current path
func (s *AdminServer) generateBreadcrumbs(path string) []BreadcrumbItem {
	var breadcrumbs []BreadcrumbItem

	// Always start with root
	breadcrumbs = append(breadcrumbs, BreadcrumbItem{
		Name: "Root",
		Path: "/",
	})

	if path == "/" {
		return breadcrumbs
	}

	// Split path and build breadcrumbs
	parts := strings.Split(strings.Trim(path, "/"), "/")
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath += "/" + part
		breadcrumbs = append(breadcrumbs, BreadcrumbItem{
			Name: part,
			Path: currentPath,
		})
	}

	return breadcrumbs
}
