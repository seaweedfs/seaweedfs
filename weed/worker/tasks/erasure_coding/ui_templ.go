package erasure_coding

import (
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/view/components"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UITemplProvider provides the templ-based UI for erasure coding task configuration
type UITemplProvider struct {
	detector  *ECDetector
	scheduler *ECScheduler
}

// NewUITemplProvider creates a new erasure coding templ UI provider
func NewUITemplProvider(detector *ECDetector, scheduler *ECScheduler) *UITemplProvider {
	return &UITemplProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// ECConfig is defined in ui.go - we reuse it

// GetTaskType returns the task type
func (ui *UITemplProvider) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// GetDisplayName returns the human-readable name
func (ui *UITemplProvider) GetDisplayName() string {
	return "Erasure Coding"
}

// GetDescription returns a description of what this task does
func (ui *UITemplProvider) GetDescription() string {
	return "Converts replicated volumes to erasure-coded format for efficient storage"
}

// GetIcon returns the icon CSS class for this task type
func (ui *UITemplProvider) GetIcon() string {
	return "fas fa-shield-alt text-info"
}

// RenderConfigSections renders the configuration as templ section data
func (ui *UITemplProvider) RenderConfigSections(currentConfig interface{}) ([]components.ConfigSectionData, error) {
	config := ui.getCurrentECConfig()

	// Detection settings section
	detectionSection := components.ConfigSectionData{
		Title:       "Detection Settings",
		Icon:        "fas fa-search",
		Description: "Configure when erasure coding tasks should be triggered",
		Fields: []interface{}{
			components.CheckboxFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "enabled",
					Label:       "Enable Erasure Coding Tasks",
					Description: "Whether erasure coding tasks should be automatically created",
				},
				Checked: config.Enabled,
			},
			components.DurationFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "scan_interval",
					Label:       "Scan Interval",
					Description: "How often to scan for volumes needing erasure coding",
					Required:    true,
				},
				Value: config.ScanInterval.String(),
			},
			components.DurationFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "volume_age_threshold",
					Label:       "Volume Age Threshold",
					Description: "Only apply erasure coding to volumes older than this age",
					Required:    true,
				},
				Value: config.VolumeAgeThreshold.String(),
			},
		},
	}

	// Erasure coding parameters section
	paramsSection := components.ConfigSectionData{
		Title:       "Erasure Coding Parameters",
		Icon:        "fas fa-cogs",
		Description: "Configure erasure coding scheme and performance",
		Fields: []interface{}{
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "data_shards",
					Label:       "Data Shards",
					Description: "Number of data shards in the erasure coding scheme",
					Required:    true,
				},
				Value: float64(config.DataShards),
				Step:  "1",
				Min:   floatPtr(1),
				Max:   floatPtr(16),
			},
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "parity_shards",
					Label:       "Parity Shards",
					Description: "Number of parity shards (determines fault tolerance)",
					Required:    true,
				},
				Value: float64(config.ParityShards),
				Step:  "1",
				Min:   floatPtr(1),
				Max:   floatPtr(16),
			},
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "max_concurrent",
					Label:       "Max Concurrent Tasks",
					Description: "Maximum number of erasure coding tasks that can run simultaneously",
					Required:    true,
				},
				Value: float64(config.MaxConcurrent),
				Step:  "1",
				Min:   floatPtr(1),
			},
		},
	}

	// Performance impact info section
	infoSection := components.ConfigSectionData{
		Title:       "Performance Impact",
		Icon:        "fas fa-info-circle",
		Description: "Important information about erasure coding operations",
		Fields: []interface{}{
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:  "durability_info",
					Label: "Durability",
					Description: fmt.Sprintf("With %d+%d configuration, can tolerate up to %d shard failures",
						config.DataShards, config.ParityShards, config.ParityShards),
				},
				Value: "High durability with space efficiency",
			},
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "performance_info",
					Label:       "Performance Note",
					Description: "Erasure coding is CPU and I/O intensive. Consider running during off-peak hours",
				},
				Value: "Schedule during low-traffic periods",
			},
		},
	}

	return []components.ConfigSectionData{detectionSection, paramsSection, infoSection}, nil
}

// ParseConfigForm parses form data into configuration
func (ui *UITemplProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &ECConfig{}

	// Parse enabled checkbox
	config.Enabled = len(formData["enabled"]) > 0 && formData["enabled"][0] == "on"

	// Parse volume age threshold
	if ageStr := formData["volume_age_threshold"]; len(ageStr) > 0 {
		if age, err := time.ParseDuration(ageStr[0]); err != nil {
			return nil, fmt.Errorf("invalid volume age threshold: %v", err)
		} else {
			config.VolumeAgeThreshold = age
		}
	}

	// Parse scan interval
	if intervalStr := formData["scan_interval"]; len(intervalStr) > 0 {
		if interval, err := time.ParseDuration(intervalStr[0]); err != nil {
			return nil, fmt.Errorf("invalid scan interval: %v", err)
		} else {
			config.ScanInterval = interval
		}
	}

	// Parse data shards
	if shardsStr := formData["data_shards"]; len(shardsStr) > 0 {
		if shards, err := strconv.Atoi(shardsStr[0]); err != nil {
			return nil, fmt.Errorf("invalid data shards: %v", err)
		} else if shards < 1 || shards > 16 {
			return nil, fmt.Errorf("data shards must be between 1 and 16")
		} else {
			config.DataShards = shards
		}
	}

	// Parse parity shards
	if shardsStr := formData["parity_shards"]; len(shardsStr) > 0 {
		if shards, err := strconv.Atoi(shardsStr[0]); err != nil {
			return nil, fmt.Errorf("invalid parity shards: %v", err)
		} else if shards < 1 || shards > 16 {
			return nil, fmt.Errorf("parity shards must be between 1 and 16")
		} else {
			config.ParityShards = shards
		}
	}

	// Parse max concurrent
	if concurrentStr := formData["max_concurrent"]; len(concurrentStr) > 0 {
		if concurrent, err := strconv.Atoi(concurrentStr[0]); err != nil {
			return nil, fmt.Errorf("invalid max concurrent: %v", err)
		} else if concurrent < 1 {
			return nil, fmt.Errorf("max concurrent must be at least 1")
		} else {
			config.MaxConcurrent = concurrent
		}
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UITemplProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentECConfig()
}

// ApplyConfig applies the new configuration
func (ui *UITemplProvider) ApplyConfig(config interface{}) error {
	ecConfig, ok := config.(*ECConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *ECConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(ecConfig.Enabled)
		ui.detector.SetVolumeAgeHours(int(ecConfig.VolumeAgeThreshold.Hours()))
		ui.detector.SetScanInterval(ecConfig.ScanInterval)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
		ui.scheduler.SetEnabled(ecConfig.Enabled)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, age_threshold=%v, max_concurrent=%d",
		ecConfig.Enabled, ecConfig.VolumeAgeThreshold, ecConfig.MaxConcurrent)

	return nil
}

// getCurrentECConfig gets the current configuration from detector and scheduler
func (ui *UITemplProvider) getCurrentECConfig() *ECConfig {
	config := &ECConfig{
		// Default values
		Enabled:            true,
		VolumeAgeThreshold: 24 * time.Hour,
		ScanInterval:       2 * time.Hour,
		MaxConcurrent:      1,
		DataShards:         10,
		ParityShards:       4,
	}

	// Get current values from detector and scheduler
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.ScanInterval = ui.detector.ScanInterval()
	}

	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
	}

	return config
}

// floatPtr is a helper function to create float64 pointers
func floatPtr(f float64) *float64 {
	return &f
}

// RegisterUITempl registers the erasure coding templ UI provider with the UI registry
func RegisterUITempl(uiRegistry *types.UITemplRegistry, detector *ECDetector, scheduler *ECScheduler) {
	uiProvider := NewUITemplProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered erasure coding task templ UI provider")
}
