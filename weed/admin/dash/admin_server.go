package dash

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"

	_ "github.com/seaweedfs/seaweedfs/weed/credential/grpc" // Register gRPC credential store
)

const (
	defaultCacheTimeout      = 10 * time.Second
	defaultFilerCacheTimeout = 30 * time.Second
	defaultStatsCacheTimeout = 30 * time.Second
)

// FilerConfig holds filer configuration needed for bucket operations
type FilerConfig struct {
	BucketsPath string
	FilerGroup  string
}

// getFilerConfig retrieves the filer configuration (buckets path and filer group)
func (s *AdminServer) getFilerConfig() (*FilerConfig, error) {
	config := &FilerConfig{
		BucketsPath: s3_constants.DefaultBucketsPath,
		FilerGroup:  "",
	}

	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get filer configuration: %w", err)
		}
		if resp.DirBuckets != "" {
			config.BucketsPath = resp.DirBuckets
		}
		config.FilerGroup = resp.FilerGroup
		return nil
	})

	return config, err
}

// getCollectionName returns the collection name for a bucket, prefixed with filer group if configured
func getCollectionName(filerGroup, bucketName string) string {
	if filerGroup != "" {
		return fmt.Sprintf("%s_%s", filerGroup, bucketName)
	}
	return bucketName
}

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
	plugin             *adminplugin.Plugin

	// Topic retention purger
	topicRetentionPurger *TopicRetentionPurger

	// Worker gRPC server
	workerGrpcServer *WorkerGrpcServer

	// Collection statistics caching
	collectionStatsCache          map[string]collectionStats
	lastCollectionStatsUpdate     time.Time
	collectionStatsCacheThreshold time.Duration

	s3TablesManager *s3tables.Manager
	icebergPort     int
}

// Type definitions moved to types.go

func NewAdminServer(masters string, templateFS http.FileSystem, dataDir string, icebergPort int) *AdminServer {
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
		masterClient:                  masterClient,
		templateFS:                    templateFS,
		dataDir:                       dataDir,
		grpcDialOption:                grpcDialOption,
		cacheExpiration:               defaultCacheTimeout,
		filerCacheExpiration:          defaultFilerCacheTimeout,
		configPersistence:             NewConfigPersistence(dataDir),
		collectionStatsCacheThreshold: defaultStatsCacheTimeout,
		s3TablesManager:               newS3TablesManager(),
		icebergPort:                   icebergPort,
	}

	// Initialize topic retention purger
	server.topicRetentionPurger = NewTopicRetentionPurger(server)

	// Initialize credential manager with defaults
	credentialManager, err := credential.NewCredentialManagerWithDefaults(credential.StoreTypeGrpc)
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

	plugin, err := adminplugin.New(adminplugin.Options{
		DataDir: dataDir,
		ClusterContextProvider: func(_ context.Context) (*plugin_pb.ClusterContext, error) {
			return server.buildDefaultPluginClusterContext(), nil
		},
	})
	if err != nil && dataDir != "" {
		glog.Warningf("Failed to initialize plugin with dataDir=%q: %v. Falling back to in-memory plugin state.", dataDir, err)
		plugin, err = adminplugin.New(adminplugin.Options{
			DataDir: "",
			ClusterContextProvider: func(_ context.Context) (*plugin_pb.ClusterContext, error) {
				return server.buildDefaultPluginClusterContext(), nil
			},
		})
	}
	if err != nil {
		glog.Errorf("Failed to initialize plugin: %v", err)
	} else {
		server.plugin = plugin
		glog.V(0).Infof("Plugin enabled")
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

// Filer discovery methods moved to client_management.go

// Client management methods moved to client_management.go

// WithFilerClient and WithVolumeServerClient methods moved to client_management.go

// Cluster topology methods moved to cluster_topology.go

// getTopologyViaGRPC method moved to cluster_topology.go

// InvalidateCache method moved to cluster_topology.go

// GetS3BucketsData retrieves all Object Store buckets and aggregates total storage metrics
func (s *AdminServer) GetS3BucketsData() (S3BucketsData, error) {
	buckets, err := s.GetS3Buckets()
	if err != nil {
		return S3BucketsData{}, err
	}

	var totalSize int64
	for _, bucket := range buckets {
		totalSize += bucket.PhysicalSize
	}

	return S3BucketsData{
		Buckets:      buckets,
		TotalBuckets: len(buckets),
		TotalSize:    totalSize,
		LastUpdated:  time.Now(),
	}, nil
}

// GetS3Buckets retrieves all Object Store buckets from the filer and collects size/object data from collections
func (s *AdminServer) GetS3Buckets() ([]S3Bucket, error) {
	var buckets []S3Bucket

	// Collect volume information by collection with caching
	collectionMap, _ := s.getCollectionStats()

	// Get filer configuration (buckets path and filer group)
	filerConfig, err := s.getFilerConfig()
	if err != nil {
		glog.Warningf("Failed to get filer configuration, using defaults: %v", err)
	}

	// Now list buckets from the filer and match with collection data
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// List buckets by looking at the buckets directory
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory:          filerConfig.BucketsPath,
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
				if strings.HasPrefix(bucketName, ".") {
					// Skip internal/system directories from Object Store bucket listing.
					continue
				}
				if s3tables.IsTableBucketEntry(resp.Entry) || strings.HasSuffix(bucketName, "--table-s3") {
					// Keep table buckets in the S3 Tables pages, not regular Object Store buckets.
					continue
				}

				// Determine collection name for this bucket
				collectionName := getCollectionName(filerConfig.FilerGroup, bucketName)

				// Get size and object count from collection data
				var physicalSize int64
				var logicalSize int64
				var objectCount int64
				if collectionData, exists := collectionMap[collectionName]; exists {
					physicalSize = collectionData.PhysicalSize
					logicalSize = collectionData.LogicalSize
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

				// Get versioning, object lock, and owner information from extended attributes
				versioningStatus := ""
				objectLockEnabled := false
				objectLockMode := ""
				var objectLockDuration int32 = 0
				var owner string

				if resp.Entry.Extended != nil {
					// Use shared utility to extract versioning information
					versioningStatus = extractVersioningFromEntry(resp.Entry)

					// Use shared utility to extract Object Lock information
					objectLockEnabled, objectLockMode, objectLockDuration = extractObjectLockInfoFromEntry(resp.Entry)

					// Extract owner information
					if ownerBytes, ok := resp.Entry.Extended[s3_constants.AmzIdentityId]; ok {
						owner = string(ownerBytes)
					}
				}

				bucket := S3Bucket{
					Name:               bucketName,
					CreatedAt:          time.Unix(resp.Entry.Attributes.Crtime, 0),
					LogicalSize:        logicalSize,
					PhysicalSize:       physicalSize,
					ObjectCount:        objectCount,
					LastModified:       time.Unix(resp.Entry.Attributes.Mtime, 0),
					Quota:              quota,
					QuotaEnabled:       quotaEnabled,
					VersioningStatus:   versioningStatus,
					ObjectLockEnabled:  objectLockEnabled,
					ObjectLockMode:     objectLockMode,
					ObjectLockDuration: objectLockDuration,
					Owner:              owner,
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
// Note: This no longer lists objects for performance reasons. Use GetS3Buckets for size/count data.
func (s *AdminServer) GetBucketDetails(bucketName string) (*BucketDetails, error) {
	// Get filer configuration (buckets path)
	filerConfig, err := s.getFilerConfig()
	if err != nil {
		glog.Warningf("Failed to get filer configuration, using defaults: %v", err)
	}

	details := &BucketDetails{
		Bucket: S3Bucket{
			Name: bucketName,
		},
		UpdatedAt: time.Now(),
	}

	// Get collection data for size and object count with caching
	collectionName := getCollectionName(filerConfig.FilerGroup, bucketName)
	stats, err := s.getCollectionStats()
	if err != nil {
		glog.Warningf("Failed to get collection data: %v", err)
		// Continue without collection data - use zero values
	} else if data, ok := stats[collectionName]; ok {
		details.Bucket.LogicalSize = data.LogicalSize
		details.Bucket.PhysicalSize = data.PhysicalSize
		details.Bucket.ObjectCount = data.FileCount
	}

	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		// Get bucket info
		bucketResp, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
			Directory: filerConfig.BucketsPath,
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

		// Get versioning, object lock, and owner information from extended attributes
		versioningStatus := ""
		objectLockEnabled := false
		objectLockMode := ""
		var objectLockDuration int32 = 0
		var owner string

		if bucketResp.Entry.Extended != nil {
			// Use shared utility to extract versioning information
			versioningStatus = extractVersioningFromEntry(bucketResp.Entry)

			// Use shared utility to extract Object Lock information
			objectLockEnabled, objectLockMode, objectLockDuration = extractObjectLockInfoFromEntry(bucketResp.Entry)

			// Extract owner information
			if ownerBytes, ok := bucketResp.Entry.Extended[s3_constants.AmzIdentityId]; ok {
				owner = string(ownerBytes)
			}
		}

		details.Bucket.VersioningStatus = versioningStatus
		details.Bucket.ObjectLockEnabled = objectLockEnabled
		details.Bucket.ObjectLockMode = objectLockMode
		details.Bucket.ObjectLockDuration = objectLockDuration
		details.Bucket.Owner = owner

		return nil
	})

	if err != nil {
		return nil, err
	}

	return details, nil
}

// CreateS3Bucket creates a new S3 bucket
func (s *AdminServer) CreateS3Bucket(bucketName string) error {
	return s.CreateS3BucketWithQuota(bucketName, 0, false)
}

// DeleteS3Bucket deletes an S3 bucket and all its contents
func (s *AdminServer) DeleteS3Bucket(bucketName string) error {
	ctx := context.Background()

	// Get filer configuration (buckets path and filer group)
	filerConfig, err := s.getFilerConfig()
	if err != nil {
		return fmt.Errorf("failed to get filer configuration: %w", err)
	}

	// Check if bucket has Object Lock enabled and if there are locked objects
	err = s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		return s3api.CheckBucketForLockedObjects(ctx, client, filerConfig.BucketsPath, bucketName)
	})
	if err != nil {
		return err
	}

	// Delete the collection first (same as s3.bucket.delete shell command)
	// This ensures volume data is cleaned up properly
	// Collection name must be prefixed with filer group if configured
	collectionName := getCollectionName(filerConfig.FilerGroup, bucketName)
	err = s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to delete collection %s: %w", collectionName, err)
	}

	// Then delete bucket directory recursively from filer
	// Use same parameters as s3.bucket.delete shell command and S3 API
	return s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
			Directory:            filerConfig.BucketsPath,
			Name:                 bucketName,
			IsDeleteData:         false, // Collection already deleted, just remove metadata
			IsRecursive:          true,
			IgnoreRecursiveError: true, // Same as S3 API and shell command
		})
		if err != nil {
			return fmt.Errorf("failed to delete bucket: %w", err)
		}

		return nil
	})
}

// GetObjectStoreUsers retrieves object store users from identity.json
func (s *AdminServer) GetObjectStoreUsers(ctx context.Context) ([]ObjectStoreUser, error) {
	if s.credentialManager == nil {
		return []ObjectStoreUser{}, nil
	}

	s3cfg, err := s.credentialManager.LoadConfiguration(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load IAM configuration: %w", err)
	}

	var users []ObjectStoreUser

	// Convert IAM identities to ObjectStoreUser format
	for _, identity := range s3cfg.Identities {
		// Skip anonymous identity
		if identity.Name == "anonymous" {
			continue
		}

		// Skip service accounts - they should not be parent users
		if strings.HasPrefix(identity.Name, serviceAccountPrefix) {
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
			Address:  pb.ServerAddress(master.Address).ToHttpAddress(),
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
			httpAddress := pb.ServerAddress(address).ToHttpAddress()

			// Update existing master info or create new one
			if masterInfo, exists := masterMap[address]; exists {
				// Update existing master with raft data
				masterInfo.IsLeader = server.IsLeader
				masterInfo.Suffrage = server.Suffrage
			} else {
				// Create new master info from raft data
				masterInfo := &MasterInfo{
					Address:  httpAddress,
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

	// Sort masters by address for consistent ordering on page refresh
	sort.Slice(masters, func(i, j int) bool {
		return masters[i].Address < masters[j].Address
	})

	// If no masters found at all, add the current master as fallback
	if len(masters) == 0 {
		currentMaster := s.masterClient.GetMaster(context.Background())
		if currentMaster != "" {
			masters = append(masters, MasterInfo{
				Address:  pb.ServerAddress(currentMaster).ToHttpAddress(),
				IsLeader: true,
				Suffrage: "Voter",
			})
			leaderCount = 1
		}
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
				Address:    pb.ServerAddress(node.Address).ToHttpAddress(),
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

	// Sort filers by address for consistent ordering on page refresh
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
				Address:    pb.ServerAddress(node.Address).ToHttpAddress(),
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

	// Sort brokers by address for consistent ordering on page refresh
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

// TriggerTopicRetentionPurgeAPI triggers topic retention purge via HTTP API
func (as *AdminServer) TriggerTopicRetentionPurgeAPI(w http.ResponseWriter, r *http.Request) {
	err := as.TriggerTopicRetentionPurge()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"message": "Topic retention purge triggered successfully"})
}

// GetConfigInfo returns information about the admin configuration
func (as *AdminServer) GetConfigInfo(w http.ResponseWriter, r *http.Request) {
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

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"config_info": configInfo,
		"title":       "Configuration Information",
	})
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

// GetWorkerGrpcPort returns the worker gRPC listen port, or 0 when unavailable.
func (s *AdminServer) GetWorkerGrpcPort() int {
	if s.workerGrpcServer == nil {
		return 0
	}
	return s.workerGrpcServer.ListenPort()
}

// GetPlugin returns the plugin instance when enabled.
func (s *AdminServer) GetPlugin() *adminplugin.Plugin {
	return s.plugin
}

// RequestPluginJobTypeDescriptor asks one worker for job type schema and returns the descriptor.
func (s *AdminServer) RequestPluginJobTypeDescriptor(ctx context.Context, jobType string, forceRefresh bool) (*plugin_pb.JobTypeDescriptor, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.RequestConfigSchema(ctx, jobType, forceRefresh)
}

// LoadPluginJobTypeDescriptor loads persisted descriptor for one job type.
func (s *AdminServer) LoadPluginJobTypeDescriptor(jobType string) (*plugin_pb.JobTypeDescriptor, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.LoadDescriptor(jobType)
}

// SavePluginJobTypeConfig persists plugin job type config in admin data dir.
func (s *AdminServer) SavePluginJobTypeConfig(config *plugin_pb.PersistedJobTypeConfig) error {
	if s.plugin == nil {
		return fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.SaveJobTypeConfig(config)
}

// LoadPluginJobTypeConfig loads plugin job type config from persistence.
func (s *AdminServer) LoadPluginJobTypeConfig(jobType string) (*plugin_pb.PersistedJobTypeConfig, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.LoadJobTypeConfig(jobType)
}

// RunPluginDetection triggers one detection pass for a job type and returns proposed jobs.
func (s *AdminServer) RunPluginDetection(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
) ([]*plugin_pb.JobProposal, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.RunDetection(ctx, jobType, clusterContext, maxResults)
}

// FilterPluginProposalsWithActiveJobs drops proposals already represented by assigned/running jobs.
func (s *AdminServer) FilterPluginProposalsWithActiveJobs(
	jobType string,
	proposals []*plugin_pb.JobProposal,
) ([]*plugin_pb.JobProposal, int, error) {
	if s.plugin == nil {
		return nil, 0, fmt.Errorf("plugin is not enabled")
	}
	filtered, skipped := s.plugin.FilterProposalsWithActiveJobs(jobType, proposals)
	return filtered, skipped, nil
}

// RunPluginDetectionWithReport triggers one detection pass and returns request metadata and proposals.
func (s *AdminServer) RunPluginDetectionWithReport(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
) (*adminplugin.DetectionReport, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.RunDetectionWithReport(ctx, jobType, clusterContext, maxResults)
}

// ExecutePluginJob dispatches one job to a capable worker and waits for completion.
func (s *AdminServer) ExecutePluginJob(
	ctx context.Context,
	job *plugin_pb.JobSpec,
	clusterContext *plugin_pb.ClusterContext,
	attempt int32,
) (*plugin_pb.JobCompleted, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.ExecuteJob(ctx, job, clusterContext, attempt)
}

// GetPluginRunHistory returns the bounded run history (last 10 success + last 10 error).
func (s *AdminServer) GetPluginRunHistory(jobType string) (*adminplugin.JobTypeRunHistory, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.LoadRunHistory(jobType)
}

// ListPluginJobTypes returns known plugin job types from connected worker registry and persisted data.
func (s *AdminServer) ListPluginJobTypes() ([]string, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.ListKnownJobTypes()
}

// GetPluginWorkers returns currently connected plugin workers.
func (s *AdminServer) GetPluginWorkers() []*adminplugin.WorkerSession {
	if s.plugin == nil {
		return nil
	}
	return s.plugin.ListWorkers()
}

// ListPluginJobs returns tracked plugin jobs for monitoring.
func (s *AdminServer) ListPluginJobs(jobType, state string, limit int) []adminplugin.TrackedJob {
	if s.plugin == nil {
		return nil
	}
	return s.plugin.ListTrackedJobs(jobType, state, limit)
}

// GetPluginJob returns one tracked plugin job by ID.
func (s *AdminServer) GetPluginJob(jobID string) (*adminplugin.TrackedJob, bool) {
	if s.plugin == nil {
		return nil, false
	}
	return s.plugin.GetTrackedJob(jobID)
}

// GetPluginJobDetail returns detailed plugin job information with activity timeline.
func (s *AdminServer) GetPluginJobDetail(jobID string, activityLimit, relatedLimit int) (*adminplugin.JobDetail, bool, error) {
	if s.plugin == nil {
		return nil, false, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.BuildJobDetail(jobID, activityLimit, relatedLimit)
}

// ListPluginActivities returns plugin job activities for monitoring.
func (s *AdminServer) ListPluginActivities(jobType string, limit int) []adminplugin.JobActivity {
	if s.plugin == nil {
		return nil
	}
	return s.plugin.ListActivities(jobType, limit)
}

// ListPluginSchedulerStates returns per-job-type scheduler state.
func (s *AdminServer) ListPluginSchedulerStates() ([]adminplugin.SchedulerJobTypeState, error) {
	if s.plugin == nil {
		return nil, fmt.Errorf("plugin is not enabled")
	}
	return s.plugin.ListSchedulerStates()
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

	if s.plugin != nil {
		s.plugin.Shutdown()
	}

	// Stop worker gRPC server
	if err := s.StopWorkerGrpcServer(); err != nil {
		glog.Errorf("Failed to stop worker gRPC server: %v", err)
	}

	// Shutdown credential manager
	if s.credentialManager != nil {
		s.credentialManager.Shutdown()
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
func extractVersioningFromEntry(entry *filer_pb.Entry) string {
	return s3api.GetVersioningStatus(entry)
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

type collectionStats struct {
	PhysicalSize int64
	LogicalSize  int64
	FileCount    int64
}

func collectCollectionStats(topologyInfo *master_pb.TopologyInfo) map[string]collectionStats {
	collectionMap := make(map[string]collectionStats)
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				for _, diskInfo := range node.DiskInfos {
					for _, volInfo := range diskInfo.VolumeInfos {
						collection := volInfo.Collection
						if collection == "" {
							collection = "default"
						}

						data := collectionMap[collection]
						data.PhysicalSize += int64(volInfo.Size)
						rp, _ := super_block.NewReplicaPlacementFromByte(byte(volInfo.ReplicaPlacement))
						// NewReplicaPlacementFromByte never returns a nil rp. If there's an error,
						// it returns a zero-valued ReplicaPlacement, for which GetCopyCount() is 1.
						// This provides a safe fallback, so we can ignore the error.
						replicaCount := int64(rp.GetCopyCount())
						if volInfo.Size >= volInfo.DeletedByteCount {
							data.LogicalSize += int64(volInfo.Size-volInfo.DeletedByteCount) / replicaCount
						}
						if volInfo.FileCount >= volInfo.DeleteCount {
							data.FileCount += int64(volInfo.FileCount-volInfo.DeleteCount) / replicaCount
						}
						collectionMap[collection] = data
					}
				}
			}
		}
	}
	return collectionMap
}

// getCollectionStats returns current collection statistics with caching
func (s *AdminServer) getCollectionStats() (map[string]collectionStats, error) {
	now := time.Now()
	if s.collectionStatsCache != nil && now.Sub(s.lastCollectionStatsUpdate) < s.collectionStatsCacheThreshold {
		return s.collectionStatsCache, nil
	}

	err := s.WithMasterClient(func(client master_pb.SeaweedClient) error {
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		if resp.TopologyInfo != nil {
			s.collectionStatsCache = collectCollectionStats(resp.TopologyInfo)
			s.lastCollectionStatsUpdate = now
		}
		return nil
	})

	return s.collectionStatsCache, err
}
