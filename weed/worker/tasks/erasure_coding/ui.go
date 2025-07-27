package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ErasureCodingConfig represents the erasure coding configuration matching the schema
type ErasureCodingConfig struct {
	Enabled             bool    `json:"enabled"`
	QuietForSeconds     int     `json:"quiet_for_seconds"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	FullnessRatio       float64 `json:"fullness_ratio"`
	CollectionFilter    string  `json:"collection_filter"`
}

// ErasureCodingUILogic contains the business logic for erasure coding UI operations
type ErasureCodingUILogic struct {
	detector  *EcDetector
	scheduler *Scheduler
}

// NewErasureCodingUILogic creates new erasure coding UI logic
func NewErasureCodingUILogic(detector *EcDetector, scheduler *Scheduler) *ErasureCodingUILogic {
	return &ErasureCodingUILogic{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetCurrentConfig returns the current erasure coding configuration
func (logic *ErasureCodingUILogic) GetCurrentConfig() interface{} {
	config := ErasureCodingConfig{
		// Default values from schema (matching task_config_schema.go)
		Enabled:             true,
		QuietForSeconds:     7 * 24 * 60 * 60, // 7 days
		ScanIntervalSeconds: 12 * 60 * 60,     // 12 hours
		MaxConcurrent:       1,
		FullnessRatio:       0.9, // 90%
		CollectionFilter:    "",
	}

	// Get current values from detector
	if logic.detector != nil {
		config.Enabled = logic.detector.IsEnabled()
		config.QuietForSeconds = logic.detector.GetQuietForSeconds()
		config.FullnessRatio = logic.detector.GetFullnessRatio()
		config.CollectionFilter = logic.detector.GetCollectionFilter()
		config.ScanIntervalSeconds = int(logic.detector.ScanInterval().Seconds())
	}

	// Get current values from scheduler
	if logic.scheduler != nil {
		config.MaxConcurrent = logic.scheduler.GetMaxConcurrent()
	}

	return config
}

// ApplyConfig applies the erasure coding configuration
func (logic *ErasureCodingUILogic) ApplyConfig(config interface{}) error {
	ecConfig, ok := config.(ErasureCodingConfig)
	if !ok {
		return nil // Will be handled by base provider fallback
	}

	// Apply to detector
	if logic.detector != nil {
		logic.detector.SetEnabled(ecConfig.Enabled)
		logic.detector.SetQuietForSeconds(ecConfig.QuietForSeconds)
		logic.detector.SetFullnessRatio(ecConfig.FullnessRatio)
		logic.detector.SetCollectionFilter(ecConfig.CollectionFilter)
		logic.detector.SetScanInterval(time.Duration(ecConfig.ScanIntervalSeconds) * time.Second)
	}

	// Apply to scheduler
	if logic.scheduler != nil {
		logic.scheduler.SetEnabled(ecConfig.Enabled)
		logic.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, quiet_for=%v seconds, max_concurrent=%d, fullness_ratio=%f, collection_filter=%s",
		ecConfig.Enabled, ecConfig.QuietForSeconds, ecConfig.MaxConcurrent, ecConfig.FullnessRatio, ecConfig.CollectionFilter)

	return nil
}

// RegisterUI registers the erasure coding UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *EcDetector, scheduler *Scheduler) {
	logic := NewErasureCodingUILogic(detector, scheduler)

	tasks.CommonRegisterUI(
		types.TaskTypeErasureCoding,
		"Erasure Coding",
		uiRegistry,
		detector,
		scheduler,
		tasks.GetErasureCodingTaskConfigSchema,
		logic.GetCurrentConfig,
		logic.ApplyConfig,
	)
}
