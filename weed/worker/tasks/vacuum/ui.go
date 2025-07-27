package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UIProvider provides the UI for vacuum task configuration
type UIProvider struct {
	detector  *VacuumDetector
	scheduler *VacuumScheduler
}

// NewUIProvider creates a new vacuum UI provider
func NewUIProvider(detector *VacuumDetector, scheduler *VacuumScheduler) *UIProvider {
	return &UIProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetTaskType returns the task type
func (ui *UIProvider) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// GetDisplayName returns the human-readable name
func (ui *UIProvider) GetDisplayName() string {
	return "Volume Vacuum"
}

// GetDescription returns a description of what this task does
func (ui *UIProvider) GetDescription() string {
	return "Reclaims disk space by removing deleted files from volumes"
}

// GetIcon returns the icon CSS class for this task type
func (ui *UIProvider) GetIcon() string {
	return "fas fa-broom text-primary"
}

// VacuumConfig represents the vacuum configuration matching the schema
type VacuumConfig struct {
	Enabled             bool    `json:"enabled"`
	GarbageThreshold    float64 `json:"garbage_threshold"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
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
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.GarbageThreshold = ui.detector.GetGarbageThreshold()
		config.ScanIntervalSeconds = int(ui.detector.ScanInterval().Seconds())
		config.MinVolumeAgeSeconds = int(ui.detector.GetMinVolumeAge().Seconds())
	}

	// Get current values from scheduler
	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
		config.MinIntervalSeconds = int(ui.scheduler.GetMinInterval().Seconds())
	}

	return config
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	vacuumConfig, ok := config.(*VacuumConfig)
	if !ok {
		// Try to get the configuration from the schema-based system
		schema := tasks.GetVacuumTaskConfigSchema()
		if schema != nil {
			// Apply defaults to ensure we have a complete config
			if err := schema.ApplyDefaults(config); err != nil {
				return err
			}

			// Use reflection to convert to VacuumConfig - simplified approach
			if vc, ok := config.(*VacuumConfig); ok {
				vacuumConfig = vc
			} else {
				glog.Warningf("Config type conversion failed, using current config")
				vacuumConfig = ui.GetCurrentConfig().(*VacuumConfig)
			}
		}
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(vacuumConfig.Enabled)
		ui.detector.SetGarbageThreshold(vacuumConfig.GarbageThreshold)
		ui.detector.SetScanInterval(time.Duration(vacuumConfig.ScanIntervalSeconds) * time.Second)
		ui.detector.SetMinVolumeAge(time.Duration(vacuumConfig.MinVolumeAgeSeconds) * time.Second)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(vacuumConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(vacuumConfig.MaxConcurrent)
		ui.scheduler.SetMinInterval(time.Duration(vacuumConfig.MinIntervalSeconds) * time.Second)
	}

	glog.V(1).Infof("Applied vacuum configuration: enabled=%v, threshold=%.1f%%, scan_interval=%ds, max_concurrent=%d",
		vacuumConfig.Enabled, vacuumConfig.GarbageThreshold*100, vacuumConfig.ScanIntervalSeconds, vacuumConfig.MaxConcurrent)

	return nil
}

// RegisterUI registers the vacuum UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *VacuumDetector, scheduler *VacuumScheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered vacuum task UI provider")
}

// GetUIProvider returns the UI provider for external use
func GetUIProvider(uiRegistry *types.UIRegistry) *UIProvider {
	provider := uiRegistry.GetProvider(types.TaskTypeVacuum)
	if provider == nil {
		return nil
	}

	if vacuumProvider, ok := provider.(*UIProvider); ok {
		return vacuumProvider
	}

	return nil
}
