package dash

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

type AdminServer struct {
	masterClient    *wdclient.MasterClient
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

	// IAM Manager for RBAC
	iamManager *integration.IAMManager

	// IAM Client for API access
	IamClient *IAMClient

	// IAM Stores (for testing/mocking)
	roleStore   integration.RoleStore
	policyStore policy.PolicyStore

	// LogFilePath for file-based logging
	LogFilePath string
}

// SetRoleStore sets the role store (useful for testing)
func (s *AdminServer) SetRoleStore(store integration.RoleStore) {
	s.roleStore = store
}

// SetPolicyStore sets the policy store (useful for testing)
func (s *AdminServer) SetPolicyStore(store policy.PolicyStore) {
	s.policyStore = store
}

// Type definitions moved to types.go

func NewAdminServer(masters string, templateFS http.FileSystem, dataDir string, logFilePath string, iamEndpoint string) *AdminServer {
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.admin")

	// Create master client with multiple master support
	masterClient := wdclient.NewMasterClient(
		grpcDialOption,
		"",      // filerGroup - not needed for admin
		"admin", // clientType
		"",      // clientHost - not needed for admin
		"",      // dataCenter - not needed for admin
		"",      // rack - not needed for admin
		*pb.ServerAddresses(masters).ToServiceDiscovery(),
	)

	// Start master client connection process (like shell and filer do)
	ctx := context.Background()
	go masterClient.KeepConnectedToMaster(ctx)

	server := &AdminServer{
		masterClient:         masterClient,
		templateFS:           templateFS,
		dataDir:              dataDir,
		grpcDialOption:       grpcDialOption,
		cacheExpiration:      10 * time.Second,
		filerCacheExpiration: 30 * time.Second, // Cache filers for 30 seconds
		configPersistence:    NewConfigPersistence(dataDir),
		LogFilePath:          logFilePath,
	}

	// Initialize topic retention purger
	server.topicRetentionPurger = NewTopicRetentionPurger(server)

	// Initialize IAM Manager
	server.initIAMManager()

	// Initialize IAM Client
	// Priority: 1. Argument, 2. Env Var, 3. Default
	if iamEndpoint == "" {
		iamEndpoint = os.Getenv("IAM_ENDPOINT")
	}
	if iamEndpoint == "" {
		iamEndpoint = "http://localhost:8333"
	}
	server.IamClient = NewIAMClient(iamEndpoint)

	// Initialize credential manager with defaults
	credentialManager, err := credential.NewCredentialManagerWithDefaults("")
	if err != nil {
		glog.Warningf("Failed to initialize credential manager: %v", err)
		// Continue without credential manager - will fall back to legacy approach
	} else {
		server.credentialManager = credentialManager
		glog.V(0).Infof("Credential manager initialized with store type: %s", credentialManager.GetStore().GetName())

		// For stores that need filer address function, configure them
		if store := credentialManager.GetStore(); store != nil {
			if filerFuncSetter, ok := store.(interface {
				SetFilerAddressFunc(func() pb.ServerAddress, grpc.DialOption)
			}); ok {
				// Configure the filer address function to dynamically return the current active filer
				// This function will be called each time credentials need to be loaded/saved,
				// so it will automatically use whatever filer is currently available (HA-aware)
				filerFuncSetter.SetFilerAddressFunc(func() pb.ServerAddress {
					return pb.ServerAddress(server.GetFilerAddress())
				}, server.grpcDialOption)
				glog.V(0).Infof("Credential store configured with dynamic filer address function")
			} else {
				glog.V(0).Infof("Credential store %s does not support filer address function", store.GetName())
			}
			
			if masterClientSetter, ok := store.(interface {
				SetMasterClient(masterClient *wdclient.MasterClient)
			}); ok {
				masterClientSetter.SetMasterClient(server.masterClient)
				glog.V(0).Infof("Credential store configured with master client")
			}
		}
	}

	// Initialize maintenance system - always initialize even without persistent storage
	var maintenanceConfig *maintenance.MaintenanceConfig
	if server.configPersistence.IsConfigured() {
		var err error
		maintenanceConfig, err = server.configPersistence.LoadMaintenanceConfig()
		if err != nil {
			glog.Errorf("Failed to load maintenance configuration: %v", err)
			maintenanceConfig = maintenance.DefaultMaintenanceConfig()
		}

		// Apply new defaults to handle schema changes (like enabling by default)
		schema := maintenance.GetMaintenanceConfigSchema()
		if err := schema.ApplyDefaultsToProtobuf(maintenanceConfig); err != nil {
			glog.Warningf("Failed to apply schema defaults to loaded config: %v", err)
		}

		// Force enable maintenance system for new default behavior
		// This handles the case where old configs had Enabled=false as default
		if !maintenanceConfig.Enabled {
			glog.V(1).Infof("Enabling maintenance system (new default behavior)")
			maintenanceConfig.Enabled = true
		}

		glog.V(1).Infof("Maintenance system initialized with persistent configuration (enabled: %v)", maintenanceConfig.Enabled)
	} else {
		maintenanceConfig = maintenance.DefaultMaintenanceConfig()
		glog.V(1).Infof("No data directory configured, maintenance system will run in memory-only mode (enabled: %v)", maintenanceConfig.Enabled)
	}

	// Always initialize maintenance manager
	server.InitMaintenanceManager(maintenanceConfig)

	// Load saved task configurations from persistence
	server.loadTaskConfigurationsFromPersistence()

	// Start maintenance manager if enabled
	if maintenanceConfig.Enabled {
		go func() {
			// Give master client a bit of time to connect before starting scans
			time.Sleep(2 * time.Second)
			if err := server.StartMaintenanceManager(); err != nil {
				glog.Errorf("Failed to start maintenance manager: %v", err)
			}
		}()
	}

	return server
}

// loadTaskConfigurationsFromPersistence loads saved task configurations from protobuf files
func (s *AdminServer) loadTaskConfigurationsFromPersistence() {
	if s.configPersistence == nil || !s.configPersistence.IsConfigured() {
		glog.V(1).Infof("Config persistence not available, using default task configurations")
		return
	}

	// Load task configurations dynamically using the config update registry
	configUpdateRegistry := tasks.GetGlobalConfigUpdateRegistry()
	configUpdateRegistry.UpdateAllConfigs(s.configPersistence)
}

// GetCredentialManager returns the credential manager
func (s *AdminServer) GetCredentialManager() *credential.CredentialManager {
	return s.credentialManager
}

// GetIAMManager returns the IAM manager
func (s *AdminServer) GetIAMManager() *integration.IAMManager {
	return s.iamManager
}

// GetLogs retrieves logs from the log file
func (s *AdminServer) GetLogs(limit int, offsetID int64) []LogEntry {
	if s.LogFilePath == "" {
		return []LogEntry{}
	}
	logs, err := ReadLogsFromFile(s.LogFilePath, limit, offsetID)
	if err != nil {
		glog.Errorf("Failed to read logs from %s: %v", s.LogFilePath, err)
		return []LogEntry{}
	}
	return logs
}

// SetLogLevel sets the logging verbosity level
func (s *AdminServer) SetLogLevel(level int) {
	glog.SetVerbosity(level)
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
		return nil, fmt.Errorf("failed to get volume information: %w", err)
	}

	// Get filer configuration to determine FilerGroup and BucketsPath
	var filerGroup string
	bucketsPath := "/buckets" // default
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		configResp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			glog.Warningf("Failed to get filer configuration: %v", err)
			// Continue with defaults
			return nil
		}
		filerGroup = configResp.FilerGroup
		if configResp.DirBuckets != "" {
			bucketsPath = configResp.DirBuckets
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get filer configuration: %w", err)
	}

	// Now list buckets from the filer and match with collection data
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List buckets from the configured buckets directory
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          bucketsPath,
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
					// Use shared utility to extract versioning information
					versioningEnabled = extractVersioningFromEntry(resp.Entry)

					// Use shared utility to extract Object Lock information
					objectLockEnabled, objectLockMode, objectLockDuration = extractObjectLockInfoFromEntry(resp.Entry)
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
		return nil, fmt.Errorf("failed to list Object Store buckets: %w", err)
	}

	return buckets, nil
}

// GetBucketDetails retrieves detailed information about a specific bucket
func (s *AdminServer) GetBucketDetails(bucketName string) (*BucketDetails, error) {
	bucketsPath := s.getBucketsPath()
	bucketPath := fmt.Sprintf("%s/%s", bucketsPath, bucketName)

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
			Directory: bucketsPath,
			Name:      bucketName,
		})
		if err != nil {
			return fmt.Errorf("bucket not found: %w", err)
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
			// Use shared utility to extract versioning information
			versioningEnabled = extractVersioningFromEntry(bucketResp.Entry)

			// Use shared utility to extract Object Lock information
			objectLockEnabled, objectLockMode, objectLockDuration = extractObjectLockInfoFromEntry(bucketResp.Entry)
		}

		details.Bucket.VersioningEnabled = versioningEnabled
		details.Bucket.ObjectLockEnabled = objectLockEnabled
		details.Bucket.ObjectLockMode = objectLockMode
		details.Bucket.ObjectLockDuration = objectLockDuration

		// List objects in bucket (recursively)
		return s.listBucketObjects(client, bucketPath, "", bucketsPath, details)
	})

	if err != nil {
		return nil, err
	}

	return details, nil
}

// listBucketObjects recursively lists all objects in a bucket
func (s *AdminServer) listBucketObjects(client filer_pb.SeaweedFilerClient, directory, prefix, bucketsPath string, details *BucketDetails) error {
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
			err := s.listBucketObjects(client, subDir, "", bucketsPath, details)
			if err != nil {
				return err
			}
		} else {
			objectKey := entry.Name
			bucketPathPrefix := fmt.Sprintf("%s/%s", bucketsPath, details.Bucket.Name)
			if directory != bucketPathPrefix {
				// Remove bucket prefix to get relative path
				relativePath := directory[len(bucketPathPrefix)+1:]
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
		bucketsPath := s.getBucketsPath()
		// Delete bucket directory recursively
		_, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:            bucketsPath,
			Name:                 bucketName,
			IsDeleteData:         true,
			IsRecursive:          true,
			IgnoreRecursiveError: false,
		})
		if err != nil {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		return nil
	})
}

// GetObjectStoreUsers removed - use IamClient.ListUsers() instead

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
		currentMaster := s.masterClient.GetMaster(context.Background())
		glog.Errorf("Failed to get raft cluster servers from master %s: %v", currentMaster, err)
	}

	// Convert map to slice
	for _, masterInfo := range masterMap {
		masters = append(masters, *masterInfo)
	}

	// If no masters found at all, add the current master as fallback
	if len(masters) == 0 {
		currentMaster := s.masterClient.GetMaster(context.Background())
		if currentMaster != "" {
			masters = append(masters, MasterInfo{
				Address:  string(currentMaster),
				IsLeader: true,
				Suffrage: "Voter",
			})
			leaderCount = 1
		}
	}

	// Sort masters by address
	sort.Slice(masters, func(i, j int) bool {
		return masters[i].Address < masters[j].Address
	})

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
		return nil, fmt.Errorf("failed to get filer nodes from master: %w", err)
	}

	// Sort filers by address
	sort.Slice(filers, func(i, j int) bool {
		return filers[i].Address < filers[j].Address
	})

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
		return nil, fmt.Errorf("failed to get broker nodes from master: %w", err)
	}

	// Sort brokers by address
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].Address < brokers[j].Address
	})

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

// GetMaintenanceTaskDetailAPI returns detailed task information via API
func (as *AdminServer) GetMaintenanceTaskDetailAPI(c *gin.Context) {
	taskID := c.Param("id")
	taskDetail, err := as.GetMaintenanceTaskDetail(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Task detail not found", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, taskDetail)
}

// ShowMaintenanceTaskDetail renders the task detail page
func (as *AdminServer) ShowMaintenanceTaskDetail(c *gin.Context) {
	username := c.GetString("username")
	if username == "" {
		username = "admin" // Default fallback
	}

	taskID := c.Param("id")
	taskDetail, err := as.GetMaintenanceTaskDetail(taskID)
	if err != nil {
		c.HTML(http.StatusNotFound, "error.html", gin.H{
			"error":   "Task not found",
			"details": err.Error(),
		})
		return
	}

	// Prepare data for template
	data := gin.H{
		"username":   username,
		"task":       taskDetail.Task,
		"taskDetail": taskDetail,
		"title":      fmt.Sprintf("Task Detail - %s", taskID),
	}

	c.HTML(http.StatusOK, "task_detail.html", data)
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

// cancelMaintenanceTask cancels a pending maintenance task
func (as *AdminServer) cancelMaintenanceTask(taskID string) error {
	if as.maintenanceManager == nil {
		return fmt.Errorf("maintenance manager not initialized")
	}

	return as.maintenanceManager.CancelTask(taskID)
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
	// Parse JSON into a generic map first to handle type conversions
	var jsonConfig map[string]interface{}
	if err := c.ShouldBindJSON(&jsonConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Convert JSON map to protobuf configuration
	config, err := convertJSONToMaintenanceConfig(jsonConfig)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse configuration: " + err.Error()})
		return
	}

	err = as.updateMaintenanceConfig(config)
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

// GetMaintenanceQueueStats returns statistics for the maintenance queue (exported for handlers)
func (as *AdminServer) GetMaintenanceQueueStats() (*maintenance.QueueStats, error) {
	return as.getMaintenanceQueueStats()
}

// getMaintenanceQueueStats returns statistics for the maintenance queue
func (as *AdminServer) getMaintenanceQueueStats() (*maintenance.QueueStats, error) {
	if as.maintenanceManager == nil {
		return &maintenance.QueueStats{
			PendingTasks:   0,
			RunningTasks:   0,
			CompletedToday: 0,
			FailedToday:    0,
			TotalTasks:     0,
		}, nil
	}

	// Get real statistics from maintenance manager
	stats := as.maintenanceManager.GetStats()

	// Convert MaintenanceStats to QueueStats
	queueStats := &maintenance.QueueStats{
		PendingTasks:   stats.TasksByStatus[maintenance.TaskStatusPending],
		RunningTasks:   stats.TasksByStatus[maintenance.TaskStatusAssigned] + stats.TasksByStatus[maintenance.TaskStatusInProgress],
		CompletedToday: stats.CompletedToday,
		FailedToday:    stats.FailedToday,
		TotalTasks:     stats.TotalTasks,
	}

	return queueStats, nil
}

// getMaintenanceTasks returns all maintenance tasks
func (as *AdminServer) getMaintenanceTasks() ([]*maintenance.MaintenanceTask, error) {
	if as.maintenanceManager == nil {
		return []*maintenance.MaintenanceTask{}, nil
	}

	// Collect all tasks from memory across all statuses
	allTasks := []*maintenance.MaintenanceTask{}
	statuses := []maintenance.MaintenanceTaskStatus{
		maintenance.TaskStatusPending,
		maintenance.TaskStatusAssigned,
		maintenance.TaskStatusInProgress,
		maintenance.TaskStatusCompleted,
		maintenance.TaskStatusFailed,
		maintenance.TaskStatusCancelled,
	}

	for _, status := range statuses {
		tasks := as.maintenanceManager.GetTasks(status, "", 0)
		allTasks = append(allTasks, tasks...)
	}

	// Also load any persisted tasks that might not be in memory
	if as.configPersistence != nil {
		persistedTasks, err := as.configPersistence.LoadAllTaskStates()
		if err == nil {
			// Add any persisted tasks not already in memory
			for _, persistedTask := range persistedTasks {
				found := false
				for _, memoryTask := range allTasks {
					if memoryTask.ID == persistedTask.ID {
						found = true
						break
					}
				}
				if !found {
					allTasks = append(allTasks, persistedTask)
				}
			}
		}
	}

	return allTasks, nil
}

// getMaintenanceTask returns a specific maintenance task
func (as *AdminServer) getMaintenanceTask(taskID string) (*maintenance.MaintenanceTask, error) {
	if as.maintenanceManager == nil {
		return nil, fmt.Errorf("maintenance manager not initialized")
	}

	// Search for the task across all statuses since we don't know which status it has
	statuses := []maintenance.MaintenanceTaskStatus{
		maintenance.TaskStatusPending,
		maintenance.TaskStatusAssigned,
		maintenance.TaskStatusInProgress,
		maintenance.TaskStatusCompleted,
		maintenance.TaskStatusFailed,
		maintenance.TaskStatusCancelled,
	}

	// First, search for the task in memory across all statuses
	for _, status := range statuses {
		tasks := as.maintenanceManager.GetTasks(status, "", 0) // Get all tasks with this status
		for _, task := range tasks {
			if task.ID == taskID {
				return task, nil
			}
		}
	}

	// If not found in memory, try to load from persistent storage
	if as.configPersistence != nil {
		task, err := as.configPersistence.LoadTaskState(taskID)
		if err == nil {
			glog.V(2).Infof("Loaded task %s from persistent storage", taskID)
			return task, nil
		}
		glog.V(2).Infof("Task %s not found in persistent storage: %v", taskID, err)
	}

	return nil, fmt.Errorf("task %s not found", taskID)
}

// GetMaintenanceTaskDetail returns comprehensive task details including logs and assignment history
func (as *AdminServer) GetMaintenanceTaskDetail(taskID string) (*maintenance.TaskDetailData, error) {
	// Get basic task information
	task, err := as.getMaintenanceTask(taskID)
	if err != nil {
		return nil, err
	}

	// Create task detail structure from the loaded task
	taskDetail := &maintenance.TaskDetailData{
		Task:              task,
		AssignmentHistory: task.AssignmentHistory, // Use assignment history from persisted task
		ExecutionLogs:     []*maintenance.TaskExecutionLog{},
		RelatedTasks:      []*maintenance.MaintenanceTask{},
		LastUpdated:       time.Now(),
	}

	if taskDetail.AssignmentHistory == nil {
		taskDetail.AssignmentHistory = []*maintenance.TaskAssignmentRecord{}
	}

	// Get worker information if task is assigned
	if task.WorkerID != "" {
		workers := as.maintenanceManager.GetWorkers()
		for _, worker := range workers {
			if worker.ID == task.WorkerID {
				taskDetail.WorkerInfo = worker
				break
			}
		}
	}

	// Get execution logs from worker if task is active/completed and worker is connected
	if task.Status == maintenance.TaskStatusInProgress || task.Status == maintenance.TaskStatusCompleted {
		if as.workerGrpcServer != nil && task.WorkerID != "" {
			workerLogs, err := as.workerGrpcServer.RequestTaskLogs(task.WorkerID, taskID, 100, "")
			if err == nil && len(workerLogs) > 0 {
				// Convert worker logs to maintenance logs
				for _, workerLog := range workerLogs {
					maintenanceLog := &maintenance.TaskExecutionLog{
						Timestamp: time.Unix(workerLog.Timestamp, 0),
						Level:     workerLog.Level,
						Message:   workerLog.Message,
						Source:    "worker",
						TaskID:    taskID,
						WorkerID:  task.WorkerID,
					}
					// carry structured fields if present
					if len(workerLog.Fields) > 0 {
						maintenanceLog.Fields = make(map[string]string, len(workerLog.Fields))
						for k, v := range workerLog.Fields {
							maintenanceLog.Fields[k] = v
						}
					}
					// carry optional progress/status
					if workerLog.Progress != 0 {
						p := float64(workerLog.Progress)
						maintenanceLog.Progress = &p
					}
					if workerLog.Status != "" {
						maintenanceLog.Status = workerLog.Status
					}
					taskDetail.ExecutionLogs = append(taskDetail.ExecutionLogs, maintenanceLog)
				}
			} else if err != nil {
				// Add a diagnostic log entry when worker logs cannot be retrieved
				diagnosticLog := &maintenance.TaskExecutionLog{
					Timestamp: time.Now(),
					Level:     "WARNING",
					Message:   fmt.Sprintf("Failed to retrieve worker logs: %v", err),
					Source:    "admin",
					TaskID:    taskID,
					WorkerID:  task.WorkerID,
				}
				taskDetail.ExecutionLogs = append(taskDetail.ExecutionLogs, diagnosticLog)
				glog.V(1).Infof("Failed to get worker logs for task %s from worker %s: %v", taskID, task.WorkerID, err)
			}
		} else {
			// Add diagnostic information when worker is not available
			reason := "worker gRPC server not available"
			if task.WorkerID == "" {
				reason = "no worker assigned to task"
			}
			diagnosticLog := &maintenance.TaskExecutionLog{
				Timestamp: time.Now(),
				Level:     "INFO",
				Message:   fmt.Sprintf("Worker logs not available: %s", reason),
				Source:    "admin",
				TaskID:    taskID,
				WorkerID:  task.WorkerID,
			}
			taskDetail.ExecutionLogs = append(taskDetail.ExecutionLogs, diagnosticLog)
		}
	}

	// Get related tasks (other tasks on same volume/server)
	if task.VolumeID != 0 || task.Server != "" {
		allTasks := as.maintenanceManager.GetTasks("", "", 50) // Get recent tasks
		for _, relatedTask := range allTasks {
			if relatedTask.ID != taskID &&
				(relatedTask.VolumeID == task.VolumeID || relatedTask.Server == task.Server) {
				taskDetail.RelatedTasks = append(taskDetail.RelatedTasks, relatedTask)
			}
		}
	}

	// Save updated task detail to disk
	if err := as.configPersistence.SaveTaskDetail(taskID, taskDetail); err != nil {
		glog.V(1).Infof("Failed to save task detail for %s: %v", taskID, err)
	}

	return taskDetail, nil
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

// GetWorkerLogs fetches logs from a specific worker for a task
func (as *AdminServer) GetWorkerLogs(c *gin.Context) {
	workerID := c.Param("id")
	taskID := c.Query("taskId")
	maxEntriesStr := c.DefaultQuery("maxEntries", "100")
	logLevel := c.DefaultQuery("logLevel", "")

	maxEntries := int32(100)
	if maxEntriesStr != "" {
		if parsed, err := strconv.ParseInt(maxEntriesStr, 10, 32); err == nil {
			maxEntries = int32(parsed)
		}
	}

	if as.workerGrpcServer == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Worker gRPC server not available"})
		return
	}

	logs, err := as.workerGrpcServer.RequestTaskLogs(workerID, taskID, maxEntries, logLevel)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("Failed to get logs from worker: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{"worker_id": workerID, "task_id": taskID, "logs": logs, "count": len(logs)})
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
		// Fallback to default configuration
		config = maintenance.DefaultMaintenanceConfig()
	}

	// Note: Do NOT apply schema defaults to existing config as it overrides saved values
	// Only apply defaults when creating new configs or handling fallback cases
	// The schema defaults should only be used in the UI for new installations

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

	configData := &MaintenanceConfigData{
		Config:       config,
		IsEnabled:    config.Enabled,
		LastScanTime: systemStats.LastScanTime,
		NextScanTime: systemStats.NextScanTime,
		SystemStats:  systemStats,
		MenuItems:    maintenance.BuildMaintenanceMenuItems(),
	}

	return configData, nil
}

// updateMaintenanceConfig updates maintenance configuration
func (as *AdminServer) updateMaintenanceConfig(config *maintenance.MaintenanceConfig) error {
	// Use ConfigField validation instead of standalone validation
	if err := maintenance.ValidateMaintenanceConfigWithSchema(config); err != nil {
		return fmt.Errorf("configuration validation failed: %v", err)
	}

	// Save configuration to persistent storage
	if err := as.configPersistence.SaveMaintenanceConfig(config); err != nil {
		return fmt.Errorf("failed to save maintenance configuration: %w", err)
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

	glog.V(1).Infof("Triggering maintenance scan")
	err := as.maintenanceManager.TriggerScan()
	if err != nil {
		glog.Errorf("Failed to trigger maintenance scan: %v", err)
		return err
	}
	glog.V(1).Infof("Maintenance scan triggered successfully")
	return nil
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
	currentMaster := as.masterClient.GetMaster(context.Background())
	configInfo["master_address"] = string(currentMaster)
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
func (s *AdminServer) StartWorkerGrpcServer(grpcPort int) error {
	if s.workerGrpcServer != nil {
		return fmt.Errorf("worker gRPC server is already running")
	}

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

	// Set up task persistence if config persistence is available
	if s.configPersistence != nil {
		queue := s.maintenanceManager.GetQueue()
		if queue != nil {
			queue.SetPersistence(s.configPersistence)

			// Load tasks from persistence on startup
			if err := queue.LoadTasksFromPersistence(); err != nil {
				glog.Errorf("Failed to load tasks from persistence: %v", err)
			}
		}
	}

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
		return fmt.Errorf("failed to find broker leader: %w", err)
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
		return fmt.Errorf("failed to create topic: %w", err)
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
		return fmt.Errorf("failed to get broker nodes from master: %w", err)
	}

	if brokerAddress == "" {
		return fmt.Errorf("no active brokers found")
	}

	// Create gRPC connection
	conn, err := grpc.NewClient(brokerAddress, s.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
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
		return fmt.Errorf("failed to get current topic configuration: %w", err)
	}

	// Create the topic configuration request, preserving all existing settings
	configRequest := &mq_pb.ConfigureTopicRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      name,
		},
		// Preserve existing partition count - this is critical!
		PartitionCount: currentConfig.PartitionCount,
		// Preserve existing schema if it exists
		MessageRecordType: currentConfig.MessageRecordType,
		KeyColumns:        currentConfig.KeyColumns,
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
		return fmt.Errorf("failed to update topic retention: %w", err)
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

// Function to extract Object Lock information from bucket entry using shared utilities
func extractObjectLockInfoFromEntry(entry *filer_pb.Entry) (bool, string, int32) {
	// Try to load Object Lock configuration using shared utility
	if config, found := s3api.LoadObjectLockConfigurationFromExtended(entry); found {
		return s3api.ExtractObjectLockInfoFromConfig(config)
	}

	return false, "", 0
}

// Function to extract versioning information from bucket entry using shared utilities
func extractVersioningFromEntry(entry *filer_pb.Entry) bool {
	enabled, _ := s3api.LoadVersioningFromExtended(entry)
	return enabled
}

// GetConfigPersistence returns the config persistence manager
func (as *AdminServer) GetConfigPersistence() *ConfigPersistence {
	return as.configPersistence
}

// convertJSONToMaintenanceConfig converts JSON map to protobuf MaintenanceConfig
func convertJSONToMaintenanceConfig(jsonConfig map[string]interface{}) (*maintenance.MaintenanceConfig, error) {
	config := &maintenance.MaintenanceConfig{}

	// Helper function to get int32 from interface{}
	getInt32 := func(key string) (int32, error) {
		if val, ok := jsonConfig[key]; ok {
			switch v := val.(type) {
			case int:
				return int32(v), nil
			case int32:
				return v, nil
			case int64:
				return int32(v), nil
			case float64:
				return int32(v), nil
			default:
				return 0, fmt.Errorf("invalid type for %s: expected number, got %T", key, v)
			}
		}
		return 0, nil
	}

	// Helper function to get bool from interface{}
	getBool := func(key string) bool {
		if val, ok := jsonConfig[key]; ok {
			if b, ok := val.(bool); ok {
				return b
			}
		}
		return false
	}

	var err error

	// Convert basic fields
	config.Enabled = getBool("enabled")

	if config.ScanIntervalSeconds, err = getInt32("scan_interval_seconds"); err != nil {
		return nil, err
	}
	if config.WorkerTimeoutSeconds, err = getInt32("worker_timeout_seconds"); err != nil {
		return nil, err
	}
	if config.TaskTimeoutSeconds, err = getInt32("task_timeout_seconds"); err != nil {
		return nil, err
	}
	if config.RetryDelaySeconds, err = getInt32("retry_delay_seconds"); err != nil {
		return nil, err
	}
	if config.MaxRetries, err = getInt32("max_retries"); err != nil {
		return nil, err
	}
	if config.CleanupIntervalSeconds, err = getInt32("cleanup_interval_seconds"); err != nil {
		return nil, err
	}
	if config.TaskRetentionSeconds, err = getInt32("task_retention_seconds"); err != nil {
		return nil, err
	}

	// Convert policy if present
	if policyData, ok := jsonConfig["policy"]; ok {
		if policyMap, ok := policyData.(map[string]interface{}); ok {
			policy := &maintenance.MaintenancePolicy{}

			if globalMaxConcurrent, err := getInt32FromMap(policyMap, "global_max_concurrent"); err != nil {
				return nil, err
			} else {
				policy.GlobalMaxConcurrent = globalMaxConcurrent
			}

			if defaultRepeatIntervalSeconds, err := getInt32FromMap(policyMap, "default_repeat_interval_seconds"); err != nil {
				return nil, err
			} else {
				policy.DefaultRepeatIntervalSeconds = defaultRepeatIntervalSeconds
			}

			if defaultCheckIntervalSeconds, err := getInt32FromMap(policyMap, "default_check_interval_seconds"); err != nil {
				return nil, err
			} else {
				policy.DefaultCheckIntervalSeconds = defaultCheckIntervalSeconds
			}

			// Convert task policies if present
			if taskPoliciesData, ok := policyMap["task_policies"]; ok {
				if taskPoliciesMap, ok := taskPoliciesData.(map[string]interface{}); ok {
					policy.TaskPolicies = make(map[string]*maintenance.TaskPolicy)

					for taskType, taskPolicyData := range taskPoliciesMap {
						if taskPolicyMap, ok := taskPolicyData.(map[string]interface{}); ok {
							taskPolicy := &maintenance.TaskPolicy{}

							taskPolicy.Enabled = getBoolFromMap(taskPolicyMap, "enabled")

							if maxConcurrent, err := getInt32FromMap(taskPolicyMap, "max_concurrent"); err != nil {
								return nil, err
							} else {
								taskPolicy.MaxConcurrent = maxConcurrent
							}

							if repeatIntervalSeconds, err := getInt32FromMap(taskPolicyMap, "repeat_interval_seconds"); err != nil {
								return nil, err
							} else {
								taskPolicy.RepeatIntervalSeconds = repeatIntervalSeconds
							}

							if checkIntervalSeconds, err := getInt32FromMap(taskPolicyMap, "check_interval_seconds"); err != nil {
								return nil, err
							} else {
								taskPolicy.CheckIntervalSeconds = checkIntervalSeconds
							}

							policy.TaskPolicies[taskType] = taskPolicy
						}
					}
				}
			}

			config.Policy = policy
		}
	}

	return config, nil
}

// Helper functions for map conversion
func getInt32FromMap(m map[string]interface{}, key string) (int32, error) {
	if val, ok := m[key]; ok {
		switch v := val.(type) {
		case int:
			return int32(v), nil
		case int32:
			return v, nil
		case int64:
			return int32(v), nil
		case float64:
			return int32(v), nil
		default:
			return 0, fmt.Errorf("invalid type for %s: expected number, got %T", key, v)
		}
	}
	return 0, nil
}

func getBoolFromMap(m map[string]interface{}, key string) bool {
	if val, ok := m[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}
