package vacuum

import (
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/view/components"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UITemplProvider provides the templ-based UI for vacuum task configuration
type UITemplProvider struct {
	detector  *VacuumDetector
	scheduler *VacuumScheduler
}

// NewUITemplProvider creates a new vacuum templ UI provider
func NewUITemplProvider(detector *VacuumDetector, scheduler *VacuumScheduler) *UITemplProvider {
	return &UITemplProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetTaskType returns the task type
func (ui *UITemplProvider) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// GetDisplayName returns the human-readable name
func (ui *UITemplProvider) GetDisplayName() string {
	return "Volume Vacuum"
}

// GetDescription returns a description of what this task does
func (ui *UITemplProvider) GetDescription() string {
	return "Reclaims disk space by removing deleted files from volumes"
}

// GetIcon returns the icon CSS class for this task type
func (ui *UITemplProvider) GetIcon() string {
	return "fas fa-broom text-primary"
}

// RenderConfigSections renders the configuration as templ section data
func (ui *UITemplProvider) RenderConfigSections(currentConfig interface{}) ([]components.ConfigSectionData, error) {
	config := ui.getCurrentVacuumConfig()

	// Detection settings section
	detectionSection := components.ConfigSectionData{
		Title:       "Detection Settings",
		Icon:        "fas fa-search",
		Description: "Configure when vacuum tasks should be triggered",
		Fields: []interface{}{
			components.CheckboxFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "enabled",
					Label:       "Enable Vacuum Tasks",
					Description: "Whether vacuum tasks should be automatically created",
				},
				Checked: config.Enabled,
			},
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "garbage_threshold",
					Label:       "Garbage Threshold",
					Description: "Trigger vacuum when garbage ratio exceeds this percentage (0.0-1.0)",
					Required:    true,
				},
				Value: config.GarbageThreshold,
				Step:  "0.01",
				Min:   floatPtr(0.0),
				Max:   floatPtr(1.0),
			},
			components.DurationFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "scan_interval",
					Label:       "Scan Interval",
					Description: "How often to scan for volumes needing vacuum",
					Required:    true,
				},
				Value: config.ScanInterval.String(),
			},
			components.DurationFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "min_volume_age",
					Label:       "Minimum Volume Age",
					Description: "Only vacuum volumes older than this duration",
					Required:    true,
				},
				Value: config.MinVolumeAge.String(),
			},
		},
	}

	// Scheduling settings section
	schedulingSection := components.ConfigSectionData{
		Title:       "Scheduling Settings",
		Icon:        "fas fa-clock",
		Description: "Configure task scheduling and concurrency",
		Fields: []interface{}{
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "max_concurrent",
					Label:       "Max Concurrent Tasks",
					Description: "Maximum number of vacuum tasks that can run simultaneously",
					Required:    true,
				},
				Value: float64(config.MaxConcurrent),
				Step:  "1",
				Min:   floatPtr(1),
			},
			components.DurationFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "min_interval",
					Label:       "Minimum Interval",
					Description: "Minimum time between vacuum operations on the same volume",
					Required:    true,
				},
				Value: config.MinInterval.String(),
			},
		},
	}

	// Performance impact info section
	performanceSection := components.ConfigSectionData{
		Title:       "Performance Impact",
		Icon:        "fas fa-exclamation-triangle",
		Description: "Important information about vacuum operations",
		Fields: []interface{}{
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "info_impact",
					Label:       "Impact",
					Description: "Volume vacuum operations are I/O intensive and should be scheduled appropriately",
				},
				Value: "Configure thresholds and intervals based on your storage usage patterns",
			},
		},
	}

	return []components.ConfigSectionData{detectionSection, schedulingSection, performanceSection}, nil
}

// ParseConfigForm parses form data into configuration
func (ui *UITemplProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &VacuumConfig{}

	// Parse enabled checkbox
	config.Enabled = len(formData["enabled"]) > 0 && formData["enabled"][0] == "on"

	// Parse garbage threshold
	if thresholdStr := formData["garbage_threshold"]; len(thresholdStr) > 0 {
		if threshold, err := strconv.ParseFloat(thresholdStr[0], 64); err != nil {
			return nil, fmt.Errorf("invalid garbage threshold: %v", err)
		} else if threshold < 0 || threshold > 1 {
			return nil, fmt.Errorf("garbage threshold must be between 0.0 and 1.0")
		} else {
			config.GarbageThreshold = threshold
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

	// Parse min volume age
	if ageStr := formData["min_volume_age"]; len(ageStr) > 0 {
		if age, err := time.ParseDuration(ageStr[0]); err != nil {
			return nil, fmt.Errorf("invalid min volume age: %v", err)
		} else {
			config.MinVolumeAge = age
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

	// Parse min interval
	if intervalStr := formData["min_interval"]; len(intervalStr) > 0 {
		if interval, err := time.ParseDuration(intervalStr[0]); err != nil {
			return nil, fmt.Errorf("invalid min interval: %v", err)
		} else {
			config.MinInterval = interval
		}
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UITemplProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentVacuumConfig()
}

// ApplyConfig applies the new configuration
func (ui *UITemplProvider) ApplyConfig(config interface{}) error {
	vacuumConfig, ok := config.(*VacuumConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *VacuumConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(vacuumConfig.Enabled)
		ui.detector.SetGarbageThreshold(vacuumConfig.GarbageThreshold)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetMaxConcurrent(vacuumConfig.MaxConcurrent)
		ui.scheduler.SetMinInterval(vacuumConfig.MinInterval)
	}

	glog.V(1).Infof("Applied vacuum configuration: enabled=%v, threshold=%.1f%%, max_concurrent=%d",
		vacuumConfig.Enabled, vacuumConfig.GarbageThreshold*100, vacuumConfig.MaxConcurrent)

	return nil
}

// getCurrentVacuumConfig gets the current configuration from detector and scheduler
func (ui *UITemplProvider) getCurrentVacuumConfig() *VacuumConfig {
	config := &VacuumConfig{
		// Default values
		Enabled:          true,
		GarbageThreshold: 0.3,
		ScanInterval:     30 * time.Minute,
		MinVolumeAge:     1 * time.Hour,
		MaxConcurrent:    2,
		MinInterval:      6 * time.Hour,
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

// RegisterUITempl registers the vacuum templ UI provider with the UI registry
func RegisterUITempl(uiRegistry *types.UITemplRegistry, detector *VacuumDetector, scheduler *VacuumScheduler) {
	uiProvider := NewUITemplProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered vacuum task templ UI provider")
}
