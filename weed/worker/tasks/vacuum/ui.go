package vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VacuumConfig represents the vacuum configuration matching the schema
type VacuumConfig struct {
	Enabled             bool    `json:"enabled"`
	GarbageThreshold    float64 `json:"garbage_threshold"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
}

// VacuumUILogic contains the business logic for vacuum UI operations
type VacuumUILogic struct {
	detector  *VacuumDetector
	scheduler *VacuumScheduler
}

// NewVacuumUILogic creates new vacuum UI logic
func NewVacuumUILogic(detector *VacuumDetector, scheduler *VacuumScheduler) *VacuumUILogic {
	return &VacuumUILogic{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetCurrentConfig returns the current vacuum configuration
func (logic *VacuumUILogic) GetCurrentConfig() interface{} {
	config := &VacuumConfig{
		// Default values from schema (matching task_config_schema.go)
		Enabled:             true,
		GarbageThreshold:    0.3,         // 30%
		ScanIntervalSeconds: 2 * 60 * 60, // 2 hours
		MaxConcurrent:       2,
		MinVolumeAgeSeconds: 24 * 60 * 60,     // 24 hours
		MinIntervalSeconds:  7 * 24 * 60 * 60, // 7 days
	}

	// Get current values from detector
	if logic.detector != nil {
		config.Enabled = logic.detector.IsEnabled()
		config.GarbageThreshold = logic.detector.GetGarbageThreshold()
		config.ScanIntervalSeconds = int(logic.detector.ScanInterval().Seconds())
		config.MinVolumeAgeSeconds = int(logic.detector.GetMinVolumeAge().Seconds())
	}

	// Get current values from scheduler
	if logic.scheduler != nil {
		config.MaxConcurrent = logic.scheduler.GetMaxConcurrent()
		config.MinIntervalSeconds = int(logic.scheduler.GetMinInterval().Seconds())
	}

	return config
}

// ApplyConfig applies the vacuum configuration
func (logic *VacuumUILogic) ApplyConfig(config interface{}) error {
	vacuumConfig, ok := config.(*VacuumConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type for vacuum")
	}

	// Apply to detector
	if logic.detector != nil {
		logic.detector.SetEnabled(vacuumConfig.Enabled)
		logic.detector.SetGarbageThreshold(vacuumConfig.GarbageThreshold)
		logic.detector.SetScanInterval(time.Duration(vacuumConfig.ScanIntervalSeconds) * time.Second)
		logic.detector.SetMinVolumeAge(time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second)
	}

	// Apply to scheduler
	if logic.scheduler != nil {
		logic.scheduler.SetEnabled(vacuumConfig.Enabled)
		logic.scheduler.SetMaxConcurrent(vacuumConfig.MaxConcurrent)
		logic.scheduler.SetMinInterval(time.Duration(vacuumConfig.MinIntervalSeconds) * time.Second)
	}

	glog.V(1).Infof("Applied vacuum configuration: enabled=%v, threshold=%.1f%%, scan_interval=%ds, max_concurrent=%d",
		vacuumConfig.Enabled, vacuumConfig.GarbageThreshold*100, vacuumConfig.ScanIntervalSeconds, vacuumConfig.MaxConcurrent)

	return nil
}

// RegisterUI registers the vacuum UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *VacuumDetector, scheduler *VacuumScheduler) {
	logic := NewVacuumUILogic(detector, scheduler)

	tasks.CommonRegisterUI(
		types.TaskTypeVacuum,
		"Volume Vacuum",
		uiRegistry,
		detector,
		scheduler,
		GetConfigSchema,
		logic.GetCurrentConfig,
		logic.ApplyConfig,
	)
}

// GetUIProvider returns the UI provider for external use
func GetUIProvider(uiRegistry *types.UIRegistry) types.TaskUIProvider {
	return uiRegistry.GetProvider(types.TaskTypeVacuum)
}
