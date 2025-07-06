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
}

// S3 Bucket management structures
type S3Bucket struct {
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	Size         int64     `json:"size"`
	ObjectCount  int64     `json:"object_count"`
	LastModified time.Time `json:"last_modified"`
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

type VolumeDetailsData struct {
	Volume           VolumeWithTopology   `json:"volume"`
	Replicas         []VolumeWithTopology `json:"replicas"`
	VolumeSizeLimit  uint64               `json:"volume_size_limit"`
	ReplicationCount int                  `json:"replication_count"`
	LastUpdated      time.Time            `json:"last_updated"`
}

// Collection management structures
type CollectionInfo struct {
	Name        string   `json:"name"`
	DataCenter  string   `json:"datacenter"`
	VolumeCount int      `json:"volume_count"`
	FileCount   int64    `json:"file_count"`
	TotalSize   int64    `json:"total_size"`
	DiskTypes   []string `json:"disk_types"`
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
