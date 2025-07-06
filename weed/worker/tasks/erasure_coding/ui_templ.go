package erasure_coding

import (
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/view/components"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Helper function to format seconds as duration string
func formatDurationFromSeconds(seconds int) string {
	d := time.Duration(seconds) * time.Second
	return d.String()
}

// Helper function to convert value and unit to seconds
func valueAndUnitToSeconds(value float64, unit string) int {
	switch unit {
	case "days":
		return int(value * 24 * 60 * 60)
	case "hours":
		return int(value * 60 * 60)
	case "minutes":
		return int(value * 60)
	default:
		return int(value * 60) // Default to minutes
	}
}

// UITemplProvider provides the templ-based UI for erasure coding task configuration
type UITemplProvider struct {
	detector  *EcDetector
	scheduler *Scheduler
}

// NewUITemplProvider creates a new erasure coding templ UI provider
func NewUITemplProvider(detector *EcDetector, scheduler *Scheduler) *UITemplProvider {
	return &UITemplProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// ErasureCodingConfig is defined in ui.go - we reuse it

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
			components.DurationInputFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "scan_interval",
					Label:       "Scan Interval",
					Description: "How often to scan for volumes needing erasure coding",
					Required:    true,
				},
				Seconds: config.ScanIntervalSeconds,
			},
			components.DurationInputFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "volume_age_threshold",
					Label:       "Volume Age Threshold",
					Description: "Only apply erasure coding to volumes older than this age",
					Required:    true,
				},
				Seconds: config.VolumeAgeHoursSeconds,
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
				Value: float64(config.ShardCount),
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
				Value: float64(config.ParityCount),
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
						config.ShardCount, config.ParityCount, config.ParityCount),
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
	config := &ErasureCodingConfig{}

	// Parse enabled checkbox
	config.Enabled = len(formData["enabled"]) > 0 && formData["enabled"][0] == "on"

	// Parse volume age threshold
	if valueStr := formData["volume_age_threshold"]; len(valueStr) > 0 {
		if value, err := strconv.ParseFloat(valueStr[0], 64); err != nil {
			return nil, fmt.Errorf("invalid volume age threshold value: %v", err)
		} else {
			unit := "hours" // default
			if unitStr := formData["volume_age_threshold_unit"]; len(unitStr) > 0 {
				unit = unitStr[0]
			}
			config.VolumeAgeHoursSeconds = valueAndUnitToSeconds(value, unit)
		}
	}

	// Parse scan interval
	if valueStr := formData["scan_interval"]; len(valueStr) > 0 {
		if value, err := strconv.ParseFloat(valueStr[0], 64); err != nil {
			return nil, fmt.Errorf("invalid scan interval value: %v", err)
		} else {
			unit := "hours" // default
			if unitStr := formData["scan_interval_unit"]; len(unitStr) > 0 {
				unit = unitStr[0]
			}
			config.ScanIntervalSeconds = valueAndUnitToSeconds(value, unit)
		}
	}

	// Parse data shards
	if shardsStr := formData["data_shards"]; len(shardsStr) > 0 {
		if shards, err := strconv.Atoi(shardsStr[0]); err != nil {
			return nil, fmt.Errorf("invalid data shards: %v", err)
		} else if shards < 1 || shards > 16 {
			return nil, fmt.Errorf("data shards must be between 1 and 16")
		} else {
			config.ShardCount = shards
		}
	}

	// Parse parity shards
	if shardsStr := formData["parity_shards"]; len(shardsStr) > 0 {
		if shards, err := strconv.Atoi(shardsStr[0]); err != nil {
			return nil, fmt.Errorf("invalid parity shards: %v", err)
		} else if shards < 1 || shards > 16 {
			return nil, fmt.Errorf("parity shards must be between 1 and 16")
		} else {
			config.ParityCount = shards
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
	ecConfig, ok := config.(*ErasureCodingConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *ErasureCodingConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(ecConfig.Enabled)
		ui.detector.SetVolumeAgeHours(ecConfig.VolumeAgeHoursSeconds)
		ui.detector.SetScanInterval(time.Duration(ecConfig.ScanIntervalSeconds) * time.Second)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
		ui.scheduler.SetEnabled(ecConfig.Enabled)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, age_threshold=%ds, max_concurrent=%d",
		ecConfig.Enabled, ecConfig.VolumeAgeHoursSeconds, ecConfig.MaxConcurrent)

	return nil
}

// getCurrentECConfig gets the current configuration from detector and scheduler
func (ui *UITemplProvider) getCurrentECConfig() *ErasureCodingConfig {
	config := &ErasureCodingConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:               true,
		VolumeAgeHoursSeconds: int((24 * time.Hour).Seconds()),
		ScanIntervalSeconds:   int((2 * time.Hour).Seconds()),
		MaxConcurrent:         1,
		ShardCount:            10,
		ParityCount:           4,
	}

	// Get current values from detector
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.VolumeAgeHoursSeconds = ui.detector.GetVolumeAgeHours()
		config.ScanIntervalSeconds = int(ui.detector.ScanInterval().Seconds())
	}

	// Get current values from scheduler
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
func RegisterUITempl(uiRegistry *types.UITemplRegistry, detector *EcDetector, scheduler *Scheduler) {
	uiProvider := NewUITemplProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered erasure coding task templ UI provider")
}
