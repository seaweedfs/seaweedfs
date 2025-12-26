package types

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
)

// ClusterInfo contains cluster information for task detection
type ClusterInfo struct {
	Servers        []*VolumeServerInfo
	TotalVolumes   int
	TotalServers   int
	LastUpdated    time.Time
	ActiveTopology *topology.ActiveTopology // Added for destination planning in detection
}

// VolumeHealthMetrics contains health information about a volume (simplified)
type VolumeHealthMetrics struct {
	VolumeID         uint32
	Server           string // Volume server ID
	ServerAddress    string // Volume server address (ip:port)
	DiskType         string // Disk type (e.g., "hdd", "ssd") or disk path (e.g., "/data1")
	DiskId           uint32 // ID of the disk in Store.Locations array
	DataCenter       string // Data center of the server
	Rack             string // Rack of the server
	Collection       string
	Size             uint64
	DeletedBytes     uint64
	GarbageRatio     float64
	LastModified     time.Time
	Age              time.Duration
	ReplicaCount     int
	ExpectedReplicas int
	IsReadOnly       bool
	HasRemoteCopy    bool
	IsECVolume       bool
	FullnessRatio    float64
}

// VolumeServerInfo contains information about a volume server (simplified)
type VolumeServerInfo struct {
	Address   string
	Volumes   int
	UsedSpace uint64
	FreeSpace uint64
	IsActive  bool
}
