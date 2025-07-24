package task

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// TaskDiscoveryEngine discovers volumes that need maintenance tasks
type TaskDiscoveryEngine struct {
	masterClient   *wdclient.MasterClient
	scanInterval   time.Duration
	ecDetector     *ECDetector
	vacuumDetector *VacuumDetector
}

// NewTaskDiscoveryEngine creates a new task discovery engine
func NewTaskDiscoveryEngine(masterClient *wdclient.MasterClient, scanInterval time.Duration) *TaskDiscoveryEngine {
	return &TaskDiscoveryEngine{
		masterClient:   masterClient,
		scanInterval:   scanInterval,
		ecDetector:     NewECDetector(),
		vacuumDetector: NewVacuumDetector(),
	}
}

// ScanForTasks scans for volumes that need maintenance tasks
func (tde *TaskDiscoveryEngine) ScanForTasks() ([]*VolumeCandidate, error) {
	var candidates []*VolumeCandidate

	// Get cluster topology and volume information
	volumeInfos, err := tde.getVolumeInformation()
	if err != nil {
		return nil, err
	}

	// Scan for EC candidates
	ecCandidates, err := tde.ecDetector.DetectECCandidates(volumeInfos)
	if err != nil {
		glog.Errorf("EC detection failed: %v", err)
	} else {
		candidates = append(candidates, ecCandidates...)
	}

	// Scan for vacuum candidates
	vacuumCandidates, err := tde.vacuumDetector.DetectVacuumCandidates(volumeInfos)
	if err != nil {
		glog.Errorf("Vacuum detection failed: %v", err)
	} else {
		candidates = append(candidates, vacuumCandidates...)
	}

	glog.V(1).Infof("Task discovery found %d candidates (%d EC, %d vacuum)",
		len(candidates), len(ecCandidates), len(vacuumCandidates))

	return candidates, nil
}

// getVolumeInformation retrieves volume information from master
func (tde *TaskDiscoveryEngine) getVolumeInformation() ([]*VolumeInfo, error) {
	var volumeInfos []*VolumeInfo

	err := tde.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
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
								volumeInfo := &VolumeInfo{
									ID:               volInfo.Id,
									Size:             volInfo.Size,
									Collection:       volInfo.Collection,
									FileCount:        volInfo.FileCount,
									DeleteCount:      volInfo.DeleteCount,
									DeletedByteCount: volInfo.DeletedByteCount,
									ReadOnly:         volInfo.ReadOnly,
									Server:           node.Id,
									DataCenter:       dc.Id,
									Rack:             rack.Id,
									DiskType:         volInfo.DiskType,
									ModifiedAtSecond: volInfo.ModifiedAtSecond,
									RemoteStorageKey: volInfo.RemoteStorageKey,
								}
								volumeInfos = append(volumeInfos, volumeInfo)
							}
						}
					}
				}
			}
		}

		return nil
	})

	return volumeInfos, err
}

// VolumeInfo contains detailed volume information
type VolumeInfo struct {
	ID               uint32
	Size             uint64
	Collection       string
	FileCount        uint64
	DeleteCount      uint64
	DeletedByteCount uint64
	ReadOnly         bool
	Server           string
	DataCenter       string
	Rack             string
	DiskType         string
	ModifiedAtSecond int64
	RemoteStorageKey string
}

// GetUtilization calculates volume utilization percentage
func (vi *VolumeInfo) GetUtilization() float64 {
	if vi.Size == 0 {
		return 0.0
	}
	// Assuming max volume size of 30GB
	maxSize := uint64(30 * 1024 * 1024 * 1024)
	return float64(vi.Size) / float64(maxSize) * 100.0
}

// GetGarbageRatio calculates the garbage ratio
func (vi *VolumeInfo) GetGarbageRatio() float64 {
	if vi.Size == 0 {
		return 0.0
	}
	return float64(vi.DeletedByteCount) / float64(vi.Size)
}

// GetIdleTime calculates how long the volume has been idle
func (vi *VolumeInfo) GetIdleTime() time.Duration {
	lastModified := time.Unix(vi.ModifiedAtSecond, 0)
	return time.Since(lastModified)
}

// IsECCandidate checks if volume is a candidate for EC
func (vi *VolumeInfo) IsECCandidate() bool {
	return !vi.ReadOnly &&
		vi.GetUtilization() >= 95.0 &&
		vi.GetIdleTime() > time.Hour &&
		vi.RemoteStorageKey == "" // Not already EC'd
}

// IsVacuumCandidate checks if volume is a candidate for vacuum
func (vi *VolumeInfo) IsVacuumCandidate() bool {
	return !vi.ReadOnly &&
		vi.GetGarbageRatio() >= 0.3 &&
		vi.DeleteCount > 0
}
