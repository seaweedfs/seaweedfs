package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UIProvider provides the UI for erasure coding task configuration
type UIProvider struct {
	detector  *EcDetector
	scheduler *Scheduler
}

// NewUIProvider creates a new erasure coding UI provider
func NewUIProvider(detector *EcDetector, scheduler *Scheduler) *UIProvider {
	return &UIProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetTaskType returns the task type
func (ui *UIProvider) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// GetDisplayName returns the human-readable name
func (ui *UIProvider) GetDisplayName() string {
	return "Erasure Coding"
}

// GetDescription returns a description of what this task does
func (ui *UIProvider) GetDescription() string {
	return "Converts volumes to erasure coded format for improved data durability and fault tolerance"
}

// GetIcon returns the icon CSS class for this task type
func (ui *UIProvider) GetIcon() string {
	return "fas fa-shield-alt text-info"
}

// ErasureCodingConfig represents the erasure coding configuration matching the schema
type ErasureCodingConfig struct {
	Enabled             bool    `json:"enabled"`
	QuietForSeconds     int     `json:"quiet_for_seconds"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	FullnessRatio       float64 `json:"fullness_ratio"`
	CollectionFilter    string  `json:"collection_filter"`
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
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
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.QuietForSeconds = ui.detector.GetQuietForSeconds()
		config.FullnessRatio = ui.detector.GetFullnessRatio()
		config.CollectionFilter = ui.detector.GetCollectionFilter()
		config.ScanIntervalSeconds = int(ui.detector.ScanInterval().Seconds())
	}

	// Get current values from scheduler
	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
	}

	return config
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	ecConfig, ok := config.(ErasureCodingConfig)
	if !ok {
		// Try to get the configuration from the schema-based system
		schema := tasks.GetErasureCodingTaskConfigSchema()
		if schema != nil {
			// Apply defaults to ensure we have a complete config
			if err := schema.ApplyDefaults(config); err != nil {
				return err
			}

			// Use reflection to convert to ErasureCodingConfig - simplified approach
			if ec, ok := config.(ErasureCodingConfig); ok {
				ecConfig = ec
			} else {
				glog.Warningf("Config type conversion failed, using current config")
				ecConfig = ui.GetCurrentConfig().(ErasureCodingConfig)
			}
		}
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(ecConfig.Enabled)
		ui.detector.SetQuietForSeconds(ecConfig.QuietForSeconds)
		ui.detector.SetFullnessRatio(ecConfig.FullnessRatio)
		ui.detector.SetCollectionFilter(ecConfig.CollectionFilter)
		ui.detector.SetScanInterval(time.Duration(ecConfig.ScanIntervalSeconds) * time.Second)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(ecConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, quiet_for=%v seconds, max_concurrent=%d, fullness_ratio=%f, collection_filter=%s",
		ecConfig.Enabled, ecConfig.QuietForSeconds, ecConfig.MaxConcurrent, ecConfig.FullnessRatio, ecConfig.CollectionFilter)

	return nil
}

// RegisterUI registers the erasure coding UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *EcDetector, scheduler *Scheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered erasure coding task UI provider")
}
