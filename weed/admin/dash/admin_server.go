package dash

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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

	// Credential management
	credentialManager *credential.CredentialManager
}

// Type definitions moved to types.go

func NewAdminServer(masterAddress string, templateFS http.FileSystem) *AdminServer {
	server := &AdminServer{
		masterAddress:        masterAddress,
		templateFS:           templateFS,
		grpcDialOption:       security.LoadClientTLS(util.GetViper(), "grpc.client"),
		cacheExpiration:      10 * time.Second,
		filerCacheExpiration: 30 * time.Second, // Cache filers for 30 seconds
	}

	// Initialize credential manager with defaults
	credentialManager, err := credential.NewCredentialManagerWithDefaults("")
	if err != nil {
		glog.Warningf("Failed to initialize credential manager: %v", err)
		// Continue without credential manager - will fall back to legacy approach
	} else {
		// For stores that need filer client details, set them
		if store := credentialManager.GetStore(); store != nil {
			if filerClientSetter, ok := store.(interface {
				SetFilerClient(string, grpc.DialOption)
			}); ok {
				// We'll set the filer client later when we discover filers
				// For now, just store the credential manager
				server.credentialManager = credentialManager

				// Set up a goroutine to set filer client once we discover filers
				go func() {
					for {
						filerAddr := server.GetFilerAddress()
						if filerAddr != "" {
							filerClientSetter.SetFilerClient(filerAddr, server.grpcDialOption)
							glog.V(1).Infof("Set filer client for credential manager: %s", filerAddr)
							break
						}
						time.Sleep(5 * time.Second) // Retry every 5 seconds
					}
				}()
			} else {
				server.credentialManager = credentialManager
			}
		} else {
			server.credentialManager = credentialManager
		}
	}

	return server
}

// GetCredentialManager returns the credential manager
func (s *AdminServer) GetCredentialManager() *credential.CredentialManager {
	return s.credentialManager
}

// Filer discovery methods moved to client_management.go

// Client management methods moved to client_management.go

// WithFilerClient and WithVolumeServerClient methods moved to client_management.go

// Cluster topology methods moved to cluster_topology.go

// getTopologyViaGRPC method moved to cluster_topology.go

// InvalidateCache method moved to cluster_topology.go

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
			Name: bucketName,
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

// GetObjectStoreUsers retrieves object store users from identity.json
func (s *AdminServer) GetObjectStoreUsers() ([]ObjectStoreUser, error) {
	s3cfg := &iam_pb.S3ApiConfiguration{}

	// Load IAM configuration from filer
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var buf bytes.Buffer
		if err := filer.ReadEntry(nil, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			if err == filer_pb.ErrNotFound {
				// If file doesn't exist, return empty configuration
				return nil
			}
			return err
		}
		if buf.Len() > 0 {
			return filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg)
		}
		return nil
	})

	if err != nil {
		glog.Errorf("Failed to load IAM configuration: %v", err)
		return []ObjectStoreUser{}, nil // Return empty list instead of error for UI
	}

	var users []ObjectStoreUser

	// Convert IAM identities to ObjectStoreUser format
	for _, identity := range s3cfg.Identities {
		// Skip anonymous identity
		if identity.Name == "anonymous" {
			continue
		}

		user := ObjectStoreUser{
			Username:    identity.Name,
			Permissions: identity.Actions,
		}

		// Set email from account if available
		if identity.Account != nil {
			user.Email = identity.Account.EmailAddress
		}

		// Get first access key for display
		if len(identity.Credentials) > 0 {
			user.AccessKey = identity.Credentials[0].AccessKey
			user.SecretKey = identity.Credentials[0].SecretKey
		}

		users = append(users, user)
	}

	return users, nil
}

// Volume server methods moved to volume_management.go

// Volume methods moved to volume_management.go

// sortVolumes method moved to volume_management.go

// GetClusterCollections method moved to collection_management.go

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
			} else {
				// Create new master info from raft data
				masterInfo := &MasterInfo{
					Address:  address,
					IsLeader: server.IsLeader,
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

// GetAllFilers method moved to client_management.go

// GetVolumeDetails method moved to volume_management.go

// VacuumVolume method moved to volume_management.go
