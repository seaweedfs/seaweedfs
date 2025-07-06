package types

import (
	"time"
)

// ClusterInfo contains cluster information for task detection
type ClusterInfo struct {
	Servers      []*VolumeServerInfo
	TotalVolumes int
	TotalServers int
	LastUpdated  time.Time
}

// VolumeHealthMetrics contains health information about a volume (simplified)
type VolumeHealthMetrics struct {
	VolumeID         uint32
	Server           string
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
