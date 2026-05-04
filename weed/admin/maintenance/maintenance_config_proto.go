package maintenance

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// DefaultMaintenanceConfigProto returns default configuration as protobuf
func DefaultMaintenanceConfigProto() *worker_pb.MaintenanceConfig {
	return &worker_pb.MaintenanceConfig{
		Enabled:                true,
		ScanIntervalSeconds:    30 * 60,     // 30 minutes
		WorkerTimeoutSeconds:   5 * 60,      // 5 minutes
		TaskTimeoutSeconds:     2 * 60 * 60, // 2 hours
		RetryDelaySeconds:      15 * 60,     // 15 minutes
		MaxRetries:             3,
		CleanupIntervalSeconds: 24 * 60 * 60,     // 24 hours
		TaskRetentionSeconds:   7 * 24 * 60 * 60, // 7 days
		// Policy field will be populated dynamically from separate task configuration files
		Policy: nil,
	}
}
