package dash

import (
	"context"
	"fmt"
	"net/http"
	"time"

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

func NewAdminServer(masterAddress, filerAddress string, templateFS http.FileSystem) *AdminServer {
	return &AdminServer{
		masterAddress:   masterAddress,
		filerAddress:    filerAddress,
		templateFS:      templateFS,
		grpcDialOption:  security.LoadClientTLS(util.GetViper(), "grpc.client"),
		cacheExpiration: 30 * time.Second,
	}
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
						var totalDiskUsage int64

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
							DiskUsage:     totalDiskUsage,
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
