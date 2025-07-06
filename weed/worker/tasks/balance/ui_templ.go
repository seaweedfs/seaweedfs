package balance

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

// Helper functions to convert between seconds and value+unit format
func secondsToValueAndUnit(seconds int) (float64, string) {
	if seconds == 0 {
		return 0, "minutes"
	}

	// Try days first
	if seconds%(24*3600) == 0 && seconds >= 24*3600 {
		return float64(seconds / (24 * 3600)), "days"
	}

	// Try hours
	if seconds%3600 == 0 && seconds >= 3600 {
		return float64(seconds / 3600), "hours"
	}

	// Default to minutes
	return float64(seconds / 60), "minutes"
}

func valueAndUnitToSeconds(value float64, unit string) int {
	switch unit {
	case "days":
		return int(value * 24 * 3600)
	case "hours":
		return int(value * 3600)
	case "minutes":
		return int(value * 60)
	default:
		return int(value * 60) // Default to minutes
	}
}

// UITemplProvider provides the templ-based UI for balance task configuration
type UITemplProvider struct {
	detector  *BalanceDetector
	scheduler *BalanceScheduler
}

// NewUITemplProvider creates a new balance templ UI provider
func NewUITemplProvider(detector *BalanceDetector, scheduler *BalanceScheduler) *UITemplProvider {
	return &UITemplProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetTaskType returns the task type
func (ui *UITemplProvider) GetTaskType() types.TaskType {
	return types.TaskTypeBalance
}

// GetDisplayName returns the human-readable name
func (ui *UITemplProvider) GetDisplayName() string {
	return "Volume Balance"
}

// GetDescription returns a description of what this task does
func (ui *UITemplProvider) GetDescription() string {
	return "Redistributes volumes across volume servers to optimize storage utilization and performance"
}

// GetIcon returns the icon CSS class for this task type
func (ui *UITemplProvider) GetIcon() string {
	return "fas fa-balance-scale text-secondary"
}

// RenderConfigSections renders the configuration as templ section data
func (ui *UITemplProvider) RenderConfigSections(currentConfig interface{}) ([]components.ConfigSectionData, error) {
	config := ui.getCurrentBalanceConfig()

	// Detection settings section
	detectionSection := components.ConfigSectionData{
		Title:       "Detection Settings",
		Icon:        "fas fa-search",
		Description: "Configure when balance tasks should be triggered",
		Fields: []interface{}{
			components.CheckboxFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "enabled",
					Label:       "Enable Balance Tasks",
					Description: "Whether balance tasks should be automatically created",
				},
				Checked: config.Enabled,
			},
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "imbalance_threshold",
					Label:       "Imbalance Threshold",
					Description: "Trigger balance when storage imbalance exceeds this percentage (0.0-1.0)",
					Required:    true,
				},
				Value: config.ImbalanceThreshold,
				Step:  "0.01",
				Min:   floatPtr(0.0),
				Max:   floatPtr(1.0),
			},
			components.DurationInputFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "scan_interval",
					Label:       "Scan Interval",
					Description: "How often to scan for imbalanced volumes",
					Required:    true,
				},
				Seconds: config.ScanIntervalSeconds,
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
					Description: "Maximum number of balance tasks that can run simultaneously",
					Required:    true,
				},
				Value: float64(config.MaxConcurrent),
				Step:  "1",
				Min:   floatPtr(1),
			},
			components.NumberFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "min_server_count",
					Label:       "Minimum Server Count",
					Description: "Only balance when at least this many servers are available",
					Required:    true,
				},
				Value: float64(config.MinServerCount),
				Step:  "1",
				Min:   floatPtr(1),
			},
		},
	}

	// Timing constraints section
	timingSection := components.ConfigSectionData{
		Title:       "Timing Constraints",
		Icon:        "fas fa-calendar-clock",
		Description: "Configure when balance operations are allowed",
		Fields: []interface{}{
			components.CheckboxFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "move_during_off_hours",
					Label:       "Restrict to Off-Hours",
					Description: "Only perform balance operations during off-peak hours",
				},
				Checked: config.MoveDuringOffHours,
			},
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "off_hours_start",
					Label:       "Off-Hours Start Time",
					Description: "Start time for off-hours window (e.g., 23:00)",
				},
				Value: config.OffHoursStart,
			},
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "off_hours_end",
					Label:       "Off-Hours End Time",
					Description: "End time for off-hours window (e.g., 06:00)",
				},
				Value: config.OffHoursEnd,
			},
		},
	}

	// Performance impact info section
	performanceSection := components.ConfigSectionData{
		Title:       "Performance Considerations",
		Icon:        "fas fa-exclamation-triangle",
		Description: "Important information about balance operations",
		Fields: []interface{}{
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "performance_info",
					Label:       "Performance Impact",
					Description: "Volume balancing involves data movement and can impact cluster performance",
				},
				Value: "Enable off-hours restriction to minimize impact on production workloads",
			},
			components.TextFieldData{
				FormFieldData: components.FormFieldData{
					Name:        "safety_info",
					Label:       "Safety Requirements",
					Description: fmt.Sprintf("Requires at least %d servers to ensure data safety during moves", config.MinServerCount),
				},
				Value: "Maintains data safety during volume moves between servers",
			},
		},
	}

	return []components.ConfigSectionData{detectionSection, schedulingSection, timingSection, performanceSection}, nil
}

// ParseConfigForm parses form data into configuration
func (ui *UITemplProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &BalanceConfig{}

	// Parse enabled checkbox
	config.Enabled = len(formData["enabled"]) > 0 && formData["enabled"][0] == "on"

	// Parse imbalance threshold
	if thresholdStr := formData["imbalance_threshold"]; len(thresholdStr) > 0 {
		if threshold, err := strconv.ParseFloat(thresholdStr[0], 64); err != nil {
			return nil, fmt.Errorf("invalid imbalance threshold: %v", err)
		} else if threshold < 0 || threshold > 1 {
			return nil, fmt.Errorf("imbalance threshold must be between 0.0 and 1.0")
		} else {
			config.ImbalanceThreshold = threshold
		}
	}

	// Parse scan interval
	if valueStr := formData["scan_interval"]; len(valueStr) > 0 {
		if value, err := strconv.ParseFloat(valueStr[0], 64); err != nil {
			return nil, fmt.Errorf("invalid scan interval value: %v", err)
		} else {
			unit := "minutes" // default
			if unitStr := formData["scan_interval_unit"]; len(unitStr) > 0 {
				unit = unitStr[0]
			}
			config.ScanIntervalSeconds = valueAndUnitToSeconds(value, unit)
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

	// Parse min server count
	if serverCountStr := formData["min_server_count"]; len(serverCountStr) > 0 {
		if serverCount, err := strconv.Atoi(serverCountStr[0]); err != nil {
			return nil, fmt.Errorf("invalid min server count: %v", err)
		} else if serverCount < 1 {
			return nil, fmt.Errorf("min server count must be at least 1")
		} else {
			config.MinServerCount = serverCount
		}
	}

	// Parse move during off hours
	config.MoveDuringOffHours = len(formData["move_during_off_hours"]) > 0 && formData["move_during_off_hours"][0] == "on"

	// Parse off hours start time
	if startStr := formData["off_hours_start"]; len(startStr) > 0 {
		config.OffHoursStart = startStr[0]
	}

	// Parse off hours end time
	if endStr := formData["off_hours_end"]; len(endStr) > 0 {
		config.OffHoursEnd = endStr[0]
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UITemplProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentBalanceConfig()
}

// ApplyConfig applies the new configuration
func (ui *UITemplProvider) ApplyConfig(config interface{}) error {
	balanceConfig, ok := config.(*BalanceConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *BalanceConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(balanceConfig.Enabled)
		ui.detector.SetThreshold(balanceConfig.ImbalanceThreshold)
		ui.detector.SetMinCheckInterval(time.Duration(balanceConfig.ScanIntervalSeconds) * time.Second)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(balanceConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(balanceConfig.MaxConcurrent)
		ui.scheduler.SetMinServerCount(balanceConfig.MinServerCount)
		ui.scheduler.SetMoveDuringOffHours(balanceConfig.MoveDuringOffHours)
		ui.scheduler.SetOffHoursStart(balanceConfig.OffHoursStart)
		ui.scheduler.SetOffHoursEnd(balanceConfig.OffHoursEnd)
	}

	glog.V(1).Infof("Applied balance configuration: enabled=%v, threshold=%.1f%%, max_concurrent=%d, min_servers=%d, off_hours=%v",
		balanceConfig.Enabled, balanceConfig.ImbalanceThreshold*100, balanceConfig.MaxConcurrent,
		balanceConfig.MinServerCount, balanceConfig.MoveDuringOffHours)

	return nil
}

// getCurrentBalanceConfig gets the current configuration from detector and scheduler
func (ui *UITemplProvider) getCurrentBalanceConfig() *BalanceConfig {
	config := &BalanceConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:             true,
		ImbalanceThreshold:  0.1, // 10% imbalance
		ScanIntervalSeconds: int((4 * time.Hour).Seconds()),
		MaxConcurrent:       1,
		MinServerCount:      3,
		MoveDuringOffHours:  true,
		OffHoursStart:       "23:00",
		OffHoursEnd:         "06:00",
	}

	// Get current values from detector
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.ImbalanceThreshold = ui.detector.GetThreshold()
		config.ScanIntervalSeconds = int(ui.detector.ScanInterval().Seconds())
	}

	// Get current values from scheduler
	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
		config.MinServerCount = ui.scheduler.GetMinServerCount()
		config.MoveDuringOffHours = ui.scheduler.GetMoveDuringOffHours()
		config.OffHoursStart = ui.scheduler.GetOffHoursStart()
		config.OffHoursEnd = ui.scheduler.GetOffHoursEnd()
	}

	return config
}

// floatPtr is a helper function to create float64 pointers
func floatPtr(f float64) *float64 {
	return &f
}

// RegisterUITempl registers the balance templ UI provider with the UI registry
func RegisterUITempl(uiRegistry *types.UITemplRegistry, detector *BalanceDetector, scheduler *BalanceScheduler) {
	uiProvider := NewUITemplProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered balance task templ UI provider")
}
