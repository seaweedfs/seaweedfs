package dash

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type AdminServer struct {
	masterAddress   string
	templateFS      http.FileSystem
	dataDir         string
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

	// Configuration persistence
	configPersistence *ConfigPersistence

	// Maintenance system
	maintenanceManager *maintenance.MaintenanceManager

	// Topic retention purger
	topicRetentionPurger *TopicRetentionPurger

	// Worker gRPC server
	workerGrpcServer *WorkerGrpcServer
}

// Type definitions moved to types.go

func NewAdminServer(masterAddress string, templateFS http.FileSystem, dataDir string) *AdminServer {
	server := &AdminServer{
		masterAddress:        masterAddress,
		templateFS:           templateFS,
		dataDir:              dataDir,
		grpcDialOption:       security.LoadClientTLS(util.GetViper(), "grpc.client"),
		cacheExpiration:      10 * time.Second,
		filerCacheExpiration: 30 * time.Second, // Cache filers for 30 seconds
		configPersistence:    NewConfigPersistence(dataDir),
	}

	// Initialize topic retention purger
	server.topicRetentionPurger = NewTopicRetentionPurger(server)

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

	// Initialize maintenance system with persistent configuration
	if server.configPersistence.IsConfigured() {
		maintenanceConfig, err := server.configPersistence.LoadMaintenanceConfig()
		if err != nil {
			glog.Errorf("Failed to load maintenance configuration: %v", err)
			maintenanceConfig = maintenance.DefaultMaintenanceConfig()
		}
		server.InitMaintenanceManager(maintenanceConfig)

		// Start maintenance manager if enabled
		if maintenanceConfig.Enabled {
			go func() {
				if err := server.StartMaintenanceManager(); err != nil {
					glog.Errorf("Failed to start maintenance manager: %v", err)
				}
			}()
		}
	} else {
		glog.V(1).Infof("No data directory configured, maintenance system will run in memory-only mode")
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

				// Get versioning and object lock information from extended attributes
				versioningEnabled := false
				objectLockEnabled := false
				objectLockMode := ""
				var objectLockDuration int32 = 0

				if resp.Entry.Extended != nil {
					if versioningBytes, exists := resp.Entry.Extended["s3.versioning"]; exists {
						versioningEnabled = string(versioningBytes) == "Enabled"
					}
					if objectLockBytes, exists := resp.Entry.Extended["s3.objectlock"]; exists {
						objectLockEnabled = string(objectLockBytes) == "Enabled"
					}
					if objectLockModeBytes, exists := resp.Entry.Extended["s3.objectlock.mode"]; exists {
						objectLockMode = string(objectLockModeBytes)
					}
					if objectLockDurationBytes, exists := resp.Entry.Extended["s3.objectlock.duration"]; exists {
						if duration, err := strconv.ParseInt(string(objectLockDurationBytes), 10, 32); err == nil {
							objectLockDuration = int32(duration)
						}
					}
				}

				bucket := S3Bucket{
					Name:               bucketName,
					CreatedAt:          time.Unix(resp.Entry.Attributes.Crtime, 0),
					Size:               size,
					ObjectCount:        objectCount,
					LastModified:       time.Unix(resp.Entry.Attributes.Mtime, 0),
					Quota:              quota,
					QuotaEnabled:       quotaEnabled,
					VersioningEnabled:  versioningEnabled,
					ObjectLockEnabled:  objectLockEnabled,
					ObjectLockMode:     objectLockMode,
					ObjectLockDuration: objectLockDuration,
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

		// Get quota information from entry
		quota := bucketResp.Entry.Quota
		quotaEnabled := quota > 0
		if quota < 0 {
			// Negative quota means disabled
			quota = -quota
			quotaEnabled = false
		}
		details.Bucket.Quota = quota
		details.Bucket.QuotaEnabled = quotaEnabled

		// Get versioning and object lock information from extended attributes
		versioningEnabled := false
		objectLockEnabled := false
		objectLockMode := ""
		var objectLockDuration int32 = 0

		if bucketResp.Entry.Extended != nil {
			if versioningBytes, exists := bucketResp.Entry.Extended["s3.versioning"]; exists {
				versioningEnabled = string(versioningBytes) == "Enabled"
			}
			if objectLockBytes, exists := bucketResp.Entry.Extended["s3.objectlock"]; exists {
				objectLockEnabled = string(objectLockBytes) == "Enabled"
			}
			if objectLockModeBytes, exists := bucketResp.Entry.Extended["s3.objectlock.mode"]; exists {
				objectLockMode = string(objectLockModeBytes)
			}
			if objectLockDurationBytes, exists := bucketResp.Entry.Extended["s3.objectlock.duration"]; exists {
				if duration, err := strconv.ParseInt(string(objectLockDurationBytes), 10, 32); err == nil {
					objectLockDuration = int32(duration)
				}
			}
		}

		details.Bucket.VersioningEnabled = versioningEnabled
		details.Bucket.ObjectLockEnabled = objectLockEnabled
		details.Bucket.ObjectLockMode = objectLockMode
		details.Bucket.ObjectLockDuration = objectLockDuration

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

// GetClusterBrokers retrieves cluster message brokers data
func (s *AdminServer) GetClusterBrokers() (*ClusterBrokersData, error) {
	var brokers []MessageBrokerInfo

	// Get broker information from master using ListClusterNodes
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
		})
		if err != nil {
			return err
		}

		// Process each broker node
		for _, node := range resp.ClusterNodes {
			createdAt := time.Unix(0, node.CreatedAtNs)

			brokerInfo := MessageBrokerInfo{
				Address:    node.Address,
				DataCenter: node.DataCenter,
				Rack:       node.Rack,
				Version:    node.Version,
				CreatedAt:  createdAt,
			}

			brokers = append(brokers, brokerInfo)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get broker nodes from master: %v", err)
	}

	return &ClusterBrokersData{
		Brokers:      brokers,
		TotalBrokers: len(brokers),
		LastUpdated:  time.Now(),
	}, nil
}

// GetAllFilers method moved to client_management.go

// GetVolumeDetails method moved to volume_management.go

// VacuumVolume method moved to volume_management.go

// ShowMaintenanceQueue displays the maintenance queue page
func (as *AdminServer) ShowMaintenanceQueue(c *gin.Context) {
	data, err := as.getMaintenanceQueueData()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// This should not render HTML template, it should use the component approach
	c.JSON(http.StatusOK, data)
}

// ShowMaintenanceWorkers displays the maintenance workers page
func (as *AdminServer) ShowMaintenanceWorkers(c *gin.Context) {
	workers, err := as.getMaintenanceWorkers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Create worker details data
	workersData := make([]*WorkerDetailsData, 0, len(workers))
	for _, worker := range workers {
		details, err := as.getMaintenanceWorkerDetails(worker.ID)
		if err != nil {
			// Create basic worker details if we can't get full details
			details = &WorkerDetailsData{
				Worker:       worker,
				CurrentTasks: []*MaintenanceTask{},
				RecentTasks:  []*MaintenanceTask{},
				Performance: &WorkerPerformance{
					TasksCompleted:  0,
					TasksFailed:     0,
					AverageTaskTime: 0,
					Uptime:          0,
					SuccessRate:     0,
				},
				LastUpdated: time.Now(),
			}
		}
		workersData = append(workersData, details)
	}

	c.JSON(http.StatusOK, gin.H{
		"workers": workersData,
		"title":   "Maintenance Workers",
	})
}

// ShowMaintenanceConfig displays the maintenance configuration page
func (as *AdminServer) ShowMaintenanceConfig(c *gin.Context) {
	config, err := as.getMaintenanceConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// This should not render HTML template, it should use the component approach
	c.JSON(http.StatusOK, config)
}

// UpdateMaintenanceConfig updates maintenance configuration from form
func (as *AdminServer) UpdateMaintenanceConfig(c *gin.Context) {
	var config MaintenanceConfig
	if err := c.ShouldBind(&config); err != nil {
		c.HTML(http.StatusBadRequest, "error.html", gin.H{"error": err.Error()})
		return
	}

	err := as.updateMaintenanceConfig(&config)
	if err != nil {
		c.HTML(http.StatusInternalServerError, "error.html", gin.H{"error": err.Error()})
		return
	}

	c.Redirect(http.StatusSeeOther, "/maintenance/config")
}

// TriggerMaintenanceScan triggers a maintenance scan
func (as *AdminServer) TriggerMaintenanceScan(c *gin.Context) {
	err := as.triggerMaintenanceScan()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Maintenance scan triggered"})
}

// GetMaintenanceTasks returns all maintenance tasks
func (as *AdminServer) GetMaintenanceTasks(c *gin.Context) {
	tasks, err := as.getMaintenanceTasks()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, tasks)
}

// GetMaintenanceTask returns a specific maintenance task
func (as *AdminServer) GetMaintenanceTask(c *gin.Context) {
	taskID := c.Param("id")
	task, err := as.getMaintenanceTask(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task not found"})
		return
	}

	c.JSON(http.StatusOK, task)
}

// CancelMaintenanceTask cancels a pending maintenance task
func (as *AdminServer) CancelMaintenanceTask(c *gin.Context) {
	taskID := c.Param("id")
	err := as.cancelMaintenanceTask(taskID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Task cancelled"})
}

// GetMaintenanceWorkersAPI returns all maintenance workers
func (as *AdminServer) GetMaintenanceWorkersAPI(c *gin.Context) {
	workers, err := as.getMaintenanceWorkers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, workers)
}

// GetMaintenanceWorker returns a specific maintenance worker
func (as *AdminServer) GetMaintenanceWorker(c *gin.Context) {
	workerID := c.Param("id")
	worker, err := as.getMaintenanceWorkerDetails(workerID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Worker not found"})
		return
	}

	c.JSON(http.StatusOK, worker)
}

// GetMaintenanceStats returns maintenance statistics
func (as *AdminServer) GetMaintenanceStats(c *gin.Context) {
	stats, err := as.getMaintenanceStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// GetMaintenanceConfigAPI returns maintenance configuration
func (as *AdminServer) GetMaintenanceConfigAPI(c *gin.Context) {
	config, err := as.getMaintenanceConfig()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, config)
}

// UpdateMaintenanceConfigAPI updates maintenance configuration via API
func (as *AdminServer) UpdateMaintenanceConfigAPI(c *gin.Context) {
	var config MaintenanceConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := as.updateMaintenanceConfig(&config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "message": "Configuration updated"})
}

// GetMaintenanceConfigData returns maintenance configuration data (public wrapper)
func (as *AdminServer) GetMaintenanceConfigData() (*maintenance.MaintenanceConfigData, error) {
	return as.getMaintenanceConfig()
}

// UpdateMaintenanceConfigData updates maintenance configuration (public wrapper)
func (as *AdminServer) UpdateMaintenanceConfigData(config *maintenance.MaintenanceConfig) error {
	return as.updateMaintenanceConfig(config)
}

// Helper methods for maintenance operations

// getMaintenanceQueueData returns data for the maintenance queue UI
func (as *AdminServer) getMaintenanceQueueData() (*maintenance.MaintenanceQueueData, error) {
	tasks, err := as.getMaintenanceTasks()
	if err != nil {
		return nil, err
	}

	workers, err := as.getMaintenanceWorkers()
	if err != nil {
		return nil, err
	}

	stats, err := as.getMaintenanceQueueStats()
	if err != nil {
		return nil, err
	}

	return &maintenance.MaintenanceQueueData{
		Tasks:       tasks,
		Workers:     workers,
		Stats:       stats,
		LastUpdated: time.Now(),
	}, nil
}

// getMaintenanceQueueStats returns statistics for the maintenance queue
func (as *AdminServer) getMaintenanceQueueStats() (*maintenance.QueueStats, error) {
	// This would integrate with the maintenance queue to get real statistics
	// For now, return mock data
	return &maintenance.QueueStats{
		PendingTasks:   5,
		RunningTasks:   2,
		CompletedToday: 15,
		FailedToday:    1,
		TotalTasks:     23,
	}, nil
}

// getMaintenanceTasks returns all maintenance tasks
func (as *AdminServer) getMaintenanceTasks() ([]*maintenance.MaintenanceTask, error) {
	if as.maintenanceManager == nil {
		return []*MaintenanceTask{}, nil
	}
	return as.maintenanceManager.GetTasks(maintenance.TaskStatusPending, "", 0), nil
}

// getMaintenanceTask returns a specific maintenance task
func (as *AdminServer) getMaintenanceTask(taskID string) (*MaintenanceTask, error) {
	if as.maintenanceManager == nil {
		return nil, fmt.Errorf("maintenance manager not initialized")
	}

	// Search for the task across all statuses since we don't know which status it has
	statuses := []MaintenanceTaskStatus{
		TaskStatusPending,
		TaskStatusAssigned,
		TaskStatusInProgress,
		TaskStatusCompleted,
		TaskStatusFailed,
		TaskStatusCancelled,
	}

	for _, status := range statuses {
		tasks := as.maintenanceManager.GetTasks(status, "", 0) // Get all tasks with this status
		for _, task := range tasks {
			if task.ID == taskID {
				return task, nil
			}
		}
	}

	return nil, fmt.Errorf("task %s not found", taskID)
}

// cancelMaintenanceTask cancels a pending maintenance task
func (as *AdminServer) cancelMaintenanceTask(taskID string) error {
	if as.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager not initialized")
	}

	return as.maintenanceManager.CancelTask(taskID)
}

// getMaintenanceWorkers returns all maintenance workers
func (as *AdminServer) getMaintenanceWorkers() ([]*maintenance.MaintenanceWorker, error) {
	if as.maintenanceManager == nil {
		return []*MaintenanceWorker{}, nil
	}
	return as.maintenanceManager.GetWorkers(), nil
}

// getMaintenanceWorkerDetails returns detailed information about a worker
func (as *AdminServer) getMaintenanceWorkerDetails(workerID string) (*WorkerDetailsData, error) {
	if as.maintenanceManager == nil {
		return nil, fmt.Errorf("maintenance manager not initialized")
	}

	workers := as.maintenanceManager.GetWorkers()
	var targetWorker *MaintenanceWorker
	for _, worker := range workers {
		if worker.ID == workerID {
			targetWorker = worker
			break
		}
	}

	if targetWorker == nil {
		return nil, fmt.Errorf("worker %s not found", workerID)
	}

	// Get current tasks for this worker
	currentTasks := as.maintenanceManager.GetTasks(TaskStatusInProgress, "", 0)
	var workerCurrentTasks []*MaintenanceTask
	for _, task := range currentTasks {
		if task.WorkerID == workerID {
			workerCurrentTasks = append(workerCurrentTasks, task)
		}
	}

	// Get recent tasks for this worker
	recentTasks := as.maintenanceManager.GetTasks(TaskStatusCompleted, "", 10)
	var workerRecentTasks []*MaintenanceTask
	for _, task := range recentTasks {
		if task.WorkerID == workerID {
			workerRecentTasks = append(workerRecentTasks, task)
		}
	}

	// Calculate performance metrics
	var totalDuration time.Duration
	var completedTasks, failedTasks int
	for _, task := range workerRecentTasks {
		if task.Status == TaskStatusCompleted {
			completedTasks++
			if task.StartedAt != nil && task.CompletedAt != nil {
				totalDuration += task.CompletedAt.Sub(*task.StartedAt)
			}
		} else if task.Status == TaskStatusFailed {
			failedTasks++
		}
	}

	var averageTaskTime time.Duration
	var successRate float64
	if completedTasks+failedTasks > 0 {
		if completedTasks > 0 {
			averageTaskTime = totalDuration / time.Duration(completedTasks)
		}
		successRate = float64(completedTasks) / float64(completedTasks+failedTasks) * 100
	}

	return &WorkerDetailsData{
		Worker:       targetWorker,
		CurrentTasks: workerCurrentTasks,
		RecentTasks:  workerRecentTasks,
		Performance: &WorkerPerformance{
			TasksCompleted:  completedTasks,
			TasksFailed:     failedTasks,
			AverageTaskTime: averageTaskTime,
			Uptime:          time.Since(targetWorker.LastHeartbeat), // This should be tracked properly
			SuccessRate:     successRate,
		},
		LastUpdated: time.Now(),
	}, nil
}

// getMaintenanceStats returns maintenance statistics
func (as *AdminServer) getMaintenanceStats() (*MaintenanceStats, error) {
	if as.maintenanceManager == nil {
		return &MaintenanceStats{
			TotalTasks:    0,
			TasksByStatus: make(map[MaintenanceTaskStatus]int),
			TasksByType:   make(map[MaintenanceTaskType]int),
			ActiveWorkers: 0,
		}, nil
	}
	return as.maintenanceManager.GetStats(), nil
}

// getMaintenanceConfig returns maintenance configuration
func (as *AdminServer) getMaintenanceConfig() (*maintenance.MaintenanceConfigData, error) {
	// Load configuration from persistent storage
	config, err := as.configPersistence.LoadMaintenanceConfig()
	if err != nil {
		glog.Errorf("Failed to load maintenance configuration: %v", err)
		// Fallback to default configuration
		config = DefaultMaintenanceConfig()
	}

	// Get system stats from maintenance manager if available
	var systemStats *MaintenanceStats
	if as.maintenanceManager != nil {
		systemStats = as.maintenanceManager.GetStats()
	} else {
		// Fallback stats
		systemStats = &MaintenanceStats{
			TotalTasks: 0,
			TasksByStatus: map[MaintenanceTaskStatus]int{
				TaskStatusPending:    0,
				TaskStatusInProgress: 0,
				TaskStatusCompleted:  0,
				TaskStatusFailed:     0,
			},
			TasksByType:     make(map[MaintenanceTaskType]int),
			ActiveWorkers:   0,
			CompletedToday:  0,
			FailedToday:     0,
			AverageTaskTime: 0,
			LastScanTime:    time.Now().Add(-time.Hour),
			NextScanTime:    time.Now().Add(time.Duration(config.ScanIntervalSeconds) * time.Second),
		}
	}

	return &MaintenanceConfigData{
		Config:       config,
		IsEnabled:    config.Enabled,
		LastScanTime: systemStats.LastScanTime,
		NextScanTime: systemStats.NextScanTime,
		SystemStats:  systemStats,
		MenuItems:    maintenance.BuildMaintenanceMenuItems(),
	}, nil
}

// updateMaintenanceConfig updates maintenance configuration
func (as *AdminServer) updateMaintenanceConfig(config *maintenance.MaintenanceConfig) error {
	// Save configuration to persistent storage
	if err := as.configPersistence.SaveMaintenanceConfig(config); err != nil {
		return fmt.Errorf("failed to save maintenance configuration: %v", err)
	}

	// Update maintenance manager if available
	if as.maintenanceManager != nil {
		if err := as.maintenanceManager.UpdateConfig(config); err != nil {
			glog.Errorf("Failed to update maintenance manager config: %v", err)
			// Don't return error here, just log it
		}
	}

	glog.V(1).Infof("Updated maintenance configuration (enabled: %v, scan interval: %ds)",
		config.Enabled, config.ScanIntervalSeconds)
	return nil
}

// triggerMaintenanceScan triggers a maintenance scan
func (as *AdminServer) triggerMaintenanceScan() error {
	if as.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager not initialized")
	}

	return as.maintenanceManager.TriggerScan()
}

// TriggerTopicRetentionPurgeAPI triggers topic retention purge via HTTP API
func (as *AdminServer) TriggerTopicRetentionPurgeAPI(c *gin.Context) {
	err := as.TriggerTopicRetentionPurge()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Topic retention purge triggered successfully"})
}

// GetConfigInfo returns information about the admin configuration
func (as *AdminServer) GetConfigInfo(c *gin.Context) {
	configInfo := as.configPersistence.GetConfigInfo()

	// Add additional admin server info
	configInfo["master_address"] = as.masterAddress
	configInfo["cache_expiration"] = as.cacheExpiration.String()
	configInfo["filer_cache_expiration"] = as.filerCacheExpiration.String()

	// Add maintenance system info
	if as.maintenanceManager != nil {
		configInfo["maintenance_enabled"] = true
		configInfo["maintenance_running"] = as.maintenanceManager.IsRunning()
	} else {
		configInfo["maintenance_enabled"] = false
		configInfo["maintenance_running"] = false
	}

	c.JSON(http.StatusOK, gin.H{
		"config_info": configInfo,
		"title":       "Configuration Information",
	})
}

// GetMaintenanceWorkersData returns workers data for the maintenance workers page
func (as *AdminServer) GetMaintenanceWorkersData() (*MaintenanceWorkersData, error) {
	workers, err := as.getMaintenanceWorkers()
	if err != nil {
		return nil, err
	}

	// Create worker details data
	workersData := make([]*WorkerDetailsData, 0, len(workers))
	activeWorkers := 0
	busyWorkers := 0
	totalLoad := 0

	for _, worker := range workers {
		details, err := as.getMaintenanceWorkerDetails(worker.ID)
		if err != nil {
			// Create basic worker details if we can't get full details
			details = &WorkerDetailsData{
				Worker:       worker,
				CurrentTasks: []*MaintenanceTask{},
				RecentTasks:  []*MaintenanceTask{},
				Performance: &WorkerPerformance{
					TasksCompleted:  0,
					TasksFailed:     0,
					AverageTaskTime: 0,
					Uptime:          0,
					SuccessRate:     0,
				},
				LastUpdated: time.Now(),
			}
		}
		workersData = append(workersData, details)

		if worker.Status == "active" {
			activeWorkers++
		} else if worker.Status == "busy" {
			busyWorkers++
		}
		totalLoad += worker.CurrentLoad
	}

	return &MaintenanceWorkersData{
		Workers:       workersData,
		ActiveWorkers: activeWorkers,
		BusyWorkers:   busyWorkers,
		TotalLoad:     totalLoad,
		LastUpdated:   time.Now(),
	}, nil
}

// StartWorkerGrpcServer starts the worker gRPC server
func (s *AdminServer) StartWorkerGrpcServer(httpPort int) error {
	if s.workerGrpcServer != nil {
		return fmt.Errorf("worker gRPC server is already running")
	}

	// Calculate gRPC port (HTTP port + 10000)
	grpcPort := httpPort + 10000

	s.workerGrpcServer = NewWorkerGrpcServer(s)
	return s.workerGrpcServer.StartWithTLS(grpcPort)
}

// StopWorkerGrpcServer stops the worker gRPC server
func (s *AdminServer) StopWorkerGrpcServer() error {
	if s.workerGrpcServer != nil {
		err := s.workerGrpcServer.Stop()
		s.workerGrpcServer = nil
		return err
	}
	return nil
}

// GetWorkerGrpcServer returns the worker gRPC server
func (s *AdminServer) GetWorkerGrpcServer() *WorkerGrpcServer {
	return s.workerGrpcServer
}

// Maintenance system integration methods

// InitMaintenanceManager initializes the maintenance manager
func (s *AdminServer) InitMaintenanceManager(config *maintenance.MaintenanceConfig) {
	s.maintenanceManager = maintenance.NewMaintenanceManager(s, config)
	glog.V(1).Infof("Maintenance manager initialized (enabled: %v)", config.Enabled)
}

// GetMaintenanceManager returns the maintenance manager
func (s *AdminServer) GetMaintenanceManager() *maintenance.MaintenanceManager {
	return s.maintenanceManager
}

// StartMaintenanceManager starts the maintenance manager
func (s *AdminServer) StartMaintenanceManager() error {
	if s.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager not initialized")
	}
	return s.maintenanceManager.Start()
}

// StopMaintenanceManager stops the maintenance manager
func (s *AdminServer) StopMaintenanceManager() {
	if s.maintenanceManager != nil {
		s.maintenanceManager.Stop()
	}
}

// TriggerTopicRetentionPurge triggers topic data purging based on retention policies
func (s *AdminServer) TriggerTopicRetentionPurge() error {
	if s.topicRetentionPurger == nil {
		return fmt.Errorf("topic retention purger not initialized")
	}

	glog.V(0).Infof("Triggering topic retention purge")
	return s.topicRetentionPurger.PurgeExpiredTopicData()
}

// GetTopicRetentionPurger returns the topic retention purger
func (s *AdminServer) GetTopicRetentionPurger() *TopicRetentionPurger {
	return s.topicRetentionPurger
}

// CreateTopicWithRetention creates a new topic with optional retention configuration
func (s *AdminServer) CreateTopicWithRetention(namespace, name string, partitionCount int32, retentionEnabled bool, retentionSeconds int64) error {
	// Find broker leader to create the topic
	brokerLeader, err := s.findBrokerLeader()
	if err != nil {
		return fmt.Errorf("failed to find broker leader: %v", err)
	}

	// Create retention configuration
	var retention *mq_pb.TopicRetention
	if retentionEnabled {
		retention = &mq_pb.TopicRetention{
			Enabled:          true,
			RetentionSeconds: retentionSeconds,
		}
	} else {
		retention = &mq_pb.TopicRetention{
			Enabled:          false,
			RetentionSeconds: 0,
		}
	}

	// Create the topic via broker
	err = s.withBrokerClient(brokerLeader, func(client mq_pb.SeaweedMessagingClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.ConfigureTopic(ctx, &mq_pb.ConfigureTopicRequest{
			Topic: &schema_pb.Topic{
				Namespace: namespace,
				Name:      name,
			},
			PartitionCount: partitionCount,
			Retention:      retention,
		})
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to create topic: %v", err)
	}

	glog.V(0).Infof("Created topic %s.%s with %d partitions (retention: enabled=%v, seconds=%d)",
		namespace, name, partitionCount, retentionEnabled, retentionSeconds)
	return nil
}

// UpdateTopicRetention updates the retention configuration for an existing topic
func (s *AdminServer) UpdateTopicRetention(namespace, name string, enabled bool, retentionSeconds int64) error {
	// Get broker information from master
	var brokerAddress string
	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
		})
		if err != nil {
			return err
		}

		// Find the first available broker
		for _, node := range resp.ClusterNodes {
			brokerAddress = node.Address
			break
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to get broker nodes from master: %v", err)
	}

	if brokerAddress == "" {
		return fmt.Errorf("no active brokers found")
	}

	// Create gRPC connection
	conn, err := grpc.Dial(brokerAddress, s.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)

	// First, get the current topic configuration to preserve existing settings
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	currentConfig, err := client.GetTopicConfiguration(ctx, &mq_pb.GetTopicConfigurationRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      name,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to get current topic configuration: %v", err)
	}

	// Create the topic configuration request, preserving all existing settings
	configRequest := &mq_pb.ConfigureTopicRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      name,
		},
		// Preserve existing partition count - this is critical!
		PartitionCount: currentConfig.PartitionCount,
		// Preserve existing record type if it exists
		RecordType: currentConfig.RecordType,
	}

	// Update only the retention configuration
	if enabled {
		configRequest.Retention = &mq_pb.TopicRetention{
			RetentionSeconds: retentionSeconds,
			Enabled:          true,
		}
	} else {
		// Set retention to disabled
		configRequest.Retention = &mq_pb.TopicRetention{
			RetentionSeconds: 0,
			Enabled:          false,
		}
	}

	// Send the configuration request with preserved settings
	_, err = client.ConfigureTopic(ctx, configRequest)
	if err != nil {
		return fmt.Errorf("failed to update topic retention: %v", err)
	}

	glog.V(0).Infof("Updated topic %s.%s retention (enabled: %v, seconds: %d) while preserving %d partitions",
		namespace, name, enabled, retentionSeconds, currentConfig.PartitionCount)
	return nil
}

// Shutdown gracefully shuts down the admin server
func (s *AdminServer) Shutdown() {
	glog.V(1).Infof("Shutting down admin server...")

	// Stop maintenance manager
	s.StopMaintenanceManager()

	// Stop worker gRPC server
	if err := s.StopWorkerGrpcServer(); err != nil {
		glog.Errorf("Failed to stop worker gRPC server: %v", err)
	}

	glog.V(1).Infof("Admin server shutdown complete")
}
