package dash

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// Core cluster topology structures
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

	// EC shard information
	EcVolumes      int                  `json:"ec_volumes"`       // Number of EC volumes this server has shards for
	EcShards       int                  `json:"ec_shards"`        // Total number of EC shards on this server
	EcShardDetails []VolumeServerEcInfo `json:"ec_shard_details"` // Detailed EC shard information
}

func (vs *VolumeServer) GetDisplayAddress() string {
	if vs.PublicURL != "" {
		return vs.PublicURL
	}
	return vs.Address
}

// VolumeServerEcInfo represents EC shard information for a specific volume on a server
type VolumeServerEcInfo struct {
	VolumeID     uint32        `json:"volume_id"`
	Collection   string        `json:"collection"`
	ShardCount   int           `json:"shard_count"`   // Number of shards this server has for this volume
	EcIndexBits  uint32        `json:"ec_index_bits"` // Bitmap of which shards this server has
	ShardNumbers []int         `json:"shard_numbers"` // List of actual shard numbers this server has
	ShardSizes   map[int]int64 `json:"shard_sizes"`   // Map from shard number to size in bytes
	TotalSize    int64         `json:"total_size"`    // Total size of all shards on this server for this volume
}

// S3 Bucket management structures
type S3Bucket struct {
	Name               string    `json:"name"`
	CreatedAt          time.Time `json:"created_at"`
	Size               int64     `json:"size"`
	ObjectCount        int64     `json:"object_count"`
	LastModified       time.Time `json:"last_modified"`
	Quota              int64     `json:"quota"`                // Quota in bytes, 0 means no quota
	QuotaEnabled       bool      `json:"quota_enabled"`        // Whether quota is enabled
	VersioningStatus   string    `json:"versioning_status"`    // Versioning status: "" (never enabled), "Enabled", or "Suspended"
	ObjectLockEnabled  bool      `json:"object_lock_enabled"`  // Whether object lock is enabled
	ObjectLockMode     string    `json:"object_lock_mode"`     // Object lock mode: "GOVERNANCE" or "COMPLIANCE"
	ObjectLockDuration int32     `json:"object_lock_duration"` // Default retention duration in days
	Owner              string    `json:"owner,omitempty"`      // Bucket owner identity; empty means admin-only access
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

// ObjectStoreUser is defined in admin_data.go

// Volume management structures
type VolumeWithTopology struct {
	*master_pb.VolumeInformationMessage
	Server     string `json:"server"`
	DataCenter string `json:"datacenter"`
	Rack       string `json:"rack"`
}

type ClusterVolumesData struct {
	Username        string               `json:"username"`
	Volumes         []VolumeWithTopology `json:"volumes"`
	TotalVolumes    int                  `json:"total_volumes"`
	TotalSize       int64                `json:"total_size"`
	VolumeSizeLimit uint64               `json:"volume_size_limit"`
	LastUpdated     time.Time            `json:"last_updated"`

	// Pagination
	CurrentPage int `json:"current_page"`
	TotalPages  int `json:"total_pages"`
	PageSize    int `json:"page_size"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`

	// Statistics
	DataCenterCount int `json:"datacenter_count"`
	RackCount       int `json:"rack_count"`
	DiskTypeCount   int `json:"disk_type_count"`
	CollectionCount int `json:"collection_count"`
	VersionCount    int `json:"version_count"`

	// Conditional display flags
	ShowDataCenterColumn bool `json:"show_datacenter_column"`
	ShowRackColumn       bool `json:"show_rack_column"`
	ShowDiskTypeColumn   bool `json:"show_disk_type_column"`
	ShowCollectionColumn bool `json:"show_collection_column"`
	ShowVersionColumn    bool `json:"show_version_column"`

	// Single values when only one exists
	SingleDataCenter string `json:"single_datacenter"`
	SingleRack       string `json:"single_rack"`
	SingleDiskType   string `json:"single_disk_type"`
	SingleCollection string `json:"single_collection"`
	SingleVersion    string `json:"single_version"`

	// All versions when multiple exist
	AllVersions []string `json:"all_versions"`

	// All disk types when multiple exist
	AllDiskTypes []string `json:"all_disk_types"`

	// Filtering
	FilterCollection string `json:"filter_collection"`
}

// ClusterEcShardsData represents the data for the cluster EC shards page
type ClusterEcShardsData struct {
	Username     string            `json:"username"`
	EcShards     []EcShardWithInfo `json:"ec_shards"`
	TotalShards  int               `json:"total_shards"`
	TotalVolumes int               `json:"total_volumes"`
	LastUpdated  time.Time         `json:"last_updated"`

	// Pagination
	CurrentPage int `json:"current_page"`
	TotalPages  int `json:"total_pages"`
	PageSize    int `json:"page_size"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`

	// Statistics
	DataCenterCount int `json:"datacenter_count"`
	RackCount       int `json:"rack_count"`
	CollectionCount int `json:"collection_count"`

	// Conditional display flags
	ShowDataCenterColumn bool `json:"show_datacenter_column"`
	ShowRackColumn       bool `json:"show_rack_column"`
	ShowCollectionColumn bool `json:"show_collection_column"`

	// Single values when only one exists
	SingleDataCenter string `json:"single_datacenter"`
	SingleRack       string `json:"single_rack"`
	SingleCollection string `json:"single_collection"`

	// Filtering
	FilterCollection string `json:"filter_collection"`

	// EC specific statistics
	ShardsPerVolume          map[uint32]int `json:"shards_per_volume"`           // VolumeID -> shard count
	VolumesWithAllShards     int            `json:"volumes_with_all_shards"`     // Volumes with all 14 shards
	VolumesWithMissingShards int            `json:"volumes_with_missing_shards"` // Volumes missing shards
}

// EcShardWithInfo represents an EC shard with its topology information
type EcShardWithInfo struct {
	VolumeID     uint32 `json:"volume_id"`
	ShardID      uint32 `json:"shard_id"`
	Collection   string `json:"collection"`
	Size         uint64 `json:"size"`
	Server       string `json:"server"`
	DataCenter   string `json:"datacenter"`
	Rack         string `json:"rack"`
	DiskType     string `json:"disk_type"`
	ModifiedTime int64  `json:"modified_time"`

	// EC specific fields
	EcIndexBits   uint32 `json:"ec_index_bits"`  // Bitmap of which shards this server has
	ShardCount    int    `json:"shard_count"`    // Number of shards this server has for this volume
	IsComplete    bool   `json:"is_complete"`    // True if this volume has all 14 shards
	MissingShards []int  `json:"missing_shards"` // List of missing shard IDs
}

// EcVolumeDetailsData represents the data for the EC volume details page
type EcVolumeDetailsData struct {
	Username      string            `json:"username"`
	VolumeID      uint32            `json:"volume_id"`
	Collection    string            `json:"collection"`
	Shards        []EcShardWithInfo `json:"shards"`
	TotalShards   int               `json:"total_shards"`
	IsComplete    bool              `json:"is_complete"`
	MissingShards []int             `json:"missing_shards"`
	DataCenters   []string          `json:"datacenters"`
	Servers       []string          `json:"servers"`
	LastUpdated   time.Time         `json:"last_updated"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`
}

type VolumeDetailsData struct {
	Volume           VolumeWithTopology   `json:"volume"`
	Replicas         []VolumeWithTopology `json:"replicas"`
	VolumeSizeLimit  uint64               `json:"volume_size_limit"`
	ReplicationCount int                  `json:"replication_count"`
	LastUpdated      time.Time            `json:"last_updated"`
}

// Collection management structures
type CollectionInfo struct {
	Name          string   `json:"name"`
	DataCenter    string   `json:"datacenter"`
	VolumeCount   int      `json:"volume_count"`
	EcVolumeCount int      `json:"ec_volume_count"`
	FileCount     int64    `json:"file_count"`
	TotalSize     int64    `json:"total_size"`
	DiskTypes     []string `json:"disk_types"`
}

type ClusterCollectionsData struct {
	Username         string           `json:"username"`
	Collections      []CollectionInfo `json:"collections"`
	TotalCollections int              `json:"total_collections"`
	TotalVolumes     int              `json:"total_volumes"`
	TotalEcVolumes   int              `json:"total_ec_volumes"`
	TotalFiles       int64            `json:"total_files"`
	TotalSize        int64            `json:"total_size"`
	LastUpdated      time.Time        `json:"last_updated"`
}

// Master and Filer management structures
type MasterInfo struct {
	Address  string `json:"address"`
	IsLeader bool   `json:"is_leader"`
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
}

type ClusterFilersData struct {
	Username    string      `json:"username"`
	Filers      []FilerInfo `json:"filers"`
	TotalFilers int         `json:"total_filers"`
	LastUpdated time.Time   `json:"last_updated"`
}

type MessageBrokerInfo struct {
	Address    string    `json:"address"`
	DataCenter string    `json:"datacenter"`
	Rack       string    `json:"rack"`
	Version    string    `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
}

type ClusterBrokersData struct {
	Username     string              `json:"username"`
	Brokers      []MessageBrokerInfo `json:"brokers"`
	TotalBrokers int                 `json:"total_brokers"`
	LastUpdated  time.Time           `json:"last_updated"`
}

type TopicInfo struct {
	Name         string             `json:"name"`
	Partitions   int                `json:"partitions"`
	Subscribers  int                `json:"subscribers"`
	MessageCount int64              `json:"message_count"`
	TotalSize    int64              `json:"total_size"`
	LastMessage  time.Time          `json:"last_message"`
	CreatedAt    time.Time          `json:"created_at"`
	Retention    TopicRetentionInfo `json:"retention"`
}

type TopicsData struct {
	Username      string      `json:"username"`
	Topics        []TopicInfo `json:"topics"`
	TotalTopics   int         `json:"total_topics"`
	TotalMessages int64       `json:"total_messages"`
	TotalSize     int64       `json:"total_size"`
	LastUpdated   time.Time   `json:"last_updated"`
}

type SubscriberInfo struct {
	Name          string    `json:"name"`
	Topic         string    `json:"topic"`
	ConsumerGroup string    `json:"consumer_group"`
	Status        string    `json:"status"`
	LastSeen      time.Time `json:"last_seen"`
	MessageCount  int64     `json:"message_count"`
	CreatedAt     time.Time `json:"created_at"`
}

type SubscribersData struct {
	Username          string           `json:"username"`
	Subscribers       []SubscriberInfo `json:"subscribers"`
	TotalSubscribers  int              `json:"total_subscribers"`
	ActiveSubscribers int              `json:"active_subscribers"`
	LastUpdated       time.Time        `json:"last_updated"`
}

// Topic Details structures
type PartitionInfo struct {
	ID             int32     `json:"id"`
	LeaderBroker   string    `json:"leader_broker"`
	FollowerBroker string    `json:"follower_broker"`
	MessageCount   int64     `json:"message_count"`
	TotalSize      int64     `json:"total_size"`
	LastDataTime   time.Time `json:"last_data_time"`
	CreatedAt      time.Time `json:"created_at"`
}

type SchemaFieldInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type PublisherInfo struct {
	PublisherName       string    `json:"publisher_name"`
	ClientID            string    `json:"client_id"`
	PartitionID         int32     `json:"partition_id"`
	Broker              string    `json:"broker"`
	ConnectTime         time.Time `json:"connect_time"`
	LastSeenTime        time.Time `json:"last_seen_time"`
	IsActive            bool      `json:"is_active"`
	LastPublishedOffset int64     `json:"last_published_offset"`
	LastAckedOffset     int64     `json:"last_acked_offset"`
}

type TopicSubscriberInfo struct {
	ConsumerGroup      string    `json:"consumer_group"`
	ConsumerID         string    `json:"consumer_id"`
	ClientID           string    `json:"client_id"`
	PartitionID        int32     `json:"partition_id"`
	Broker             string    `json:"broker"`
	ConnectTime        time.Time `json:"connect_time"`
	LastSeenTime       time.Time `json:"last_seen_time"`
	IsActive           bool      `json:"is_active"`
	CurrentOffset      int64     `json:"current_offset"`       // last acknowledged offset
	LastReceivedOffset int64     `json:"last_received_offset"` // last received offset
}

type ConsumerGroupOffsetInfo struct {
	ConsumerGroup string    `json:"consumer_group"`
	PartitionID   int32     `json:"partition_id"`
	Offset        int64     `json:"offset"`
	LastUpdated   time.Time `json:"last_updated"`
}

type TopicRetentionInfo struct {
	Enabled          bool   `json:"enabled"`
	RetentionSeconds int64  `json:"retention_seconds"`
	DisplayValue     int32  `json:"display_value"` // for UI rendering
	DisplayUnit      string `json:"display_unit"`  // for UI rendering
}

type TopicDetailsData struct {
	Username             string                    `json:"username"`
	TopicName            string                    `json:"topic_name"`
	Namespace            string                    `json:"namespace"`
	Name                 string                    `json:"name"`
	Partitions           []PartitionInfo           `json:"partitions"`
	KeySchema            []SchemaFieldInfo         `json:"key_schema"`   // Schema fields for keys
	ValueSchema          []SchemaFieldInfo         `json:"value_schema"` // Schema fields for values
	Publishers           []PublisherInfo           `json:"publishers"`
	Subscribers          []TopicSubscriberInfo     `json:"subscribers"`
	ConsumerGroupOffsets []ConsumerGroupOffsetInfo `json:"consumer_group_offsets"`
	Retention            TopicRetentionInfo        `json:"retention"`
	MessageCount         int64                     `json:"message_count"`
	TotalSize            int64                     `json:"total_size"`
	CreatedAt            time.Time                 `json:"created_at"`
	LastUpdated          time.Time                 `json:"last_updated"`
}

// Volume server management structures
type ClusterVolumeServersData struct {
	Username           string         `json:"username"`
	VolumeServers      []VolumeServer `json:"volume_servers"`
	TotalVolumeServers int            `json:"total_volume_servers"`
	TotalVolumes       int            `json:"total_volumes"`
	TotalCapacity      int64          `json:"total_capacity"`
	LastUpdated        time.Time      `json:"last_updated"`
}

// Type aliases for maintenance package types to support existing code
type MaintenanceTask = maintenance.MaintenanceTask
type MaintenanceTaskType = maintenance.MaintenanceTaskType
type MaintenanceTaskStatus = maintenance.MaintenanceTaskStatus
type MaintenanceTaskPriority = maintenance.MaintenanceTaskPriority
type MaintenanceWorker = maintenance.MaintenanceWorker
type MaintenanceConfig = maintenance.MaintenanceConfig
type MaintenanceStats = maintenance.MaintenanceStats
type MaintenanceConfigData = maintenance.MaintenanceConfigData
type MaintenanceQueueData = maintenance.MaintenanceQueueData
type QueueStats = maintenance.QueueStats
type WorkerDetailsData = maintenance.WorkerDetailsData
type WorkerPerformance = maintenance.WorkerPerformance

// GetTaskIcon returns the icon CSS class for a task type from its UI provider
func GetTaskIcon(taskType MaintenanceTaskType) string {
	return maintenance.GetTaskIcon(taskType)
}

// Status constants (these are still static)
const (
	TaskStatusPending    = maintenance.TaskStatusPending
	TaskStatusAssigned   = maintenance.TaskStatusAssigned
	TaskStatusInProgress = maintenance.TaskStatusInProgress
	TaskStatusCompleted  = maintenance.TaskStatusCompleted
	TaskStatusFailed     = maintenance.TaskStatusFailed
	TaskStatusCancelled  = maintenance.TaskStatusCancelled

	PriorityLow      = maintenance.PriorityLow
	PriorityNormal   = maintenance.PriorityNormal
	PriorityHigh     = maintenance.PriorityHigh
	PriorityCritical = maintenance.PriorityCritical
)

// Helper functions from maintenance package
var DefaultMaintenanceConfig = maintenance.DefaultMaintenanceConfig

// MaintenanceWorkersData represents the data for the maintenance workers page
type MaintenanceWorkersData struct {
	Workers       []*WorkerDetailsData `json:"workers"`
	ActiveWorkers int                  `json:"active_workers"`
	BusyWorkers   int                  `json:"busy_workers"`
	TotalLoad     int                  `json:"total_load"`
	LastUpdated   time.Time            `json:"last_updated"`
}

// Maintenance system types are now in weed/admin/maintenance package

// EcVolumeWithShards represents an EC volume with its shard distribution
type EcVolumeWithShards struct {
	VolumeID       uint32         `json:"volume_id"`
	Collection     string         `json:"collection"`
	TotalShards    int            `json:"total_shards"`
	IsComplete     bool           `json:"is_complete"`
	MissingShards  []int          `json:"missing_shards"`
	ShardLocations map[int]string `json:"shard_locations"` // shardId -> server
	ShardSizes     map[int]int64  `json:"shard_sizes"`     // shardId -> size in bytes
	DataCenters    []string       `json:"data_centers"`
	Servers        []string       `json:"servers"`
	Racks          []string       `json:"racks"`
	ModifiedTime   int64          `json:"modified_time"`
}

// ClusterEcVolumesData represents the response for clustered EC volumes view
type ClusterEcVolumesData struct {
	EcVolumes    []EcVolumeWithShards `json:"ec_volumes"`
	TotalVolumes int                  `json:"total_volumes"`
	LastUpdated  time.Time            `json:"last_updated"`

	// Pagination
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalPages int `json:"total_pages"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`

	// Filtering
	Collection string `json:"collection"`

	// Conditional display flags
	ShowDataCenterColumn bool `json:"show_datacenter_column"`
	ShowRackColumn       bool `json:"show_rack_column"`
	ShowCollectionColumn bool `json:"show_collection_column"`

	// Statistics
	CompleteVolumes   int `json:"complete_volumes"`
	IncompleteVolumes int `json:"incomplete_volumes"`
	TotalShards       int `json:"total_shards"`

	// User context
	Username string `json:"username"`
}

// Collection detail page structures
type CollectionDetailsData struct {
	Username       string               `json:"username"`
	CollectionName string               `json:"collection_name"`
	RegularVolumes []VolumeWithTopology `json:"regular_volumes"`
	EcVolumes      []EcVolumeWithShards `json:"ec_volumes"`
	TotalVolumes   int                  `json:"total_volumes"`
	TotalEcVolumes int                  `json:"total_ec_volumes"`
	TotalFiles     int64                `json:"total_files"`
	TotalSize      int64                `json:"total_size"`
	DataCenters    []string             `json:"data_centers"`
	DiskTypes      []string             `json:"disk_types"`
	LastUpdated    time.Time            `json:"last_updated"`

	// Pagination
	Page       int `json:"page"`
	PageSize   int `json:"page_size"`
	TotalPages int `json:"total_pages"`

	// Sorting
	SortBy    string `json:"sort_by"`
	SortOrder string `json:"sort_order"`
}

// Service Account management structures
type ServiceAccount struct {
	ID                string    `json:"id"`
	ParentUser        string    `json:"parent_user"`
	Description       string    `json:"description,omitempty"`
	AccessKeyId       string    `json:"access_key_id,omitempty"`
	SecretAccessKey   string    `json:"secret_access_key,omitempty"` // Only returned on creation
	Status            string    `json:"status"`
	CreateDate        time.Time `json:"create_date"`
	Expiration        time.Time `json:"expiration,omitempty"`
	ServiceAccountId  string    `json:"service_account_id,omitempty"`
	ServiceAccountIds []string  `json:"service_account_ids,omitempty"`
}

type ServiceAccountsData struct {
	Username        string           `json:"username"`
	ServiceAccounts []ServiceAccount `json:"service_accounts"`
	TotalAccounts   int              `json:"total_accounts"`
	ActiveAccounts  int              `json:"active_accounts"`
	AvailableUsers  []string         `json:"available_users"` // For parent user dropdown
	LastUpdated     time.Time        `json:"last_updated"`
}

type CreateServiceAccountRequest struct {
	ParentUser  string `json:"parent_user"`
	Description string `json:"description,omitempty"`
	Expiration  string `json:"expiration,omitempty"` // RFC3339 format
}

type UpdateServiceAccountRequest struct {
	Status      string `json:"status,omitempty"` // Active, Inactive
	Description string `json:"description,omitempty"`
	Expiration  string `json:"expiration,omitempty"`
}

// STS Configuration display types
type STSConfigData struct {
	Enabled       bool      `json:"enabled"`
	Issuer        string    `json:"issuer,omitempty"`
	TokenDuration string    `json:"token_duration,omitempty"`
	Providers     []string  `json:"providers,omitempty"`
	LastUpdated   time.Time `json:"last_updated"`
}
