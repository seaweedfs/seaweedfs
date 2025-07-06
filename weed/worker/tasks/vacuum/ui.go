package vacuum

import (
	"fmt"
	"html/template"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// VacuumConfig represents the vacuum configuration
type VacuumConfig struct {
	Enabled          bool          `json:"enabled"`
	GarbageThreshold float64       `json:"garbage_threshold"`
	ScanInterval     time.Duration `json:"scan_interval"`
	MinVolumeAge     time.Duration `json:"min_volume_age"`
	MaxConcurrent    int           `json:"max_concurrent"`
	MinInterval      time.Duration `json:"min_interval"`
}

// RenderConfigForm renders the configuration form HTML
func (ui *UIProvider) RenderConfigForm(currentConfig interface{}) (template.HTML, error) {
	config := ui.getCurrentVacuumConfig()

	// Build form using the FormBuilder helper
	form := types.NewFormBuilder()

	// Detection Settings
	form.AddCheckboxField(
		"enabled",
		"Enable Vacuum Tasks",
		"Whether vacuum tasks should be automatically created",
		config.Enabled,
	)

	form.AddNumberField(
		"garbage_threshold",
		"Garbage Threshold (%)",
		"Trigger vacuum when garbage ratio exceeds this percentage (0.0-1.0)",
		config.GarbageThreshold,
		true,
	)

	form.AddDurationField(
		"scan_interval",
		"Scan Interval",
		"How often to scan for volumes needing vacuum",
		config.ScanInterval,
		true,
	)

	form.AddDurationField(
		"min_volume_age",
		"Minimum Volume Age",
		"Only vacuum volumes older than this duration",
		config.MinVolumeAge,
		true,
	)

	// Scheduling Settings
	form.AddNumberField(
		"max_concurrent",
		"Max Concurrent Tasks",
		"Maximum number of vacuum tasks that can run simultaneously",
		float64(config.MaxConcurrent),
		true,
	)

	form.AddDurationField(
		"min_interval",
		"Minimum Interval",
		"Minimum time between vacuum operations on the same volume",
		config.MinInterval,
		true,
	)

	// Generate organized form sections using Bootstrap components
	html := `
<div class="row">
	<div class="col-12">
		<div class="card mb-4">
			<div class="card-header">
				<h5 class="mb-0">
					<i class="fas fa-search me-2"></i>
					Detection Settings
				</h5>
			</div>
			<div class="card-body">
` + string(form.Build()) + `
			</div>
		</div>
	</div>
</div>

<script>
function resetForm() {
	if (confirm('Reset all vacuum settings to defaults?')) {
		// Reset to default values
		document.querySelector('input[name="enabled"]').checked = true;
		document.querySelector('input[name="garbage_threshold"]').value = '0.3';
		document.querySelector('input[name="scan_interval"]').value = '30m';
		document.querySelector('input[name="min_volume_age"]').value = '1h';
		document.querySelector('input[name="max_concurrent"]').value = '2';
		document.querySelector('input[name="min_interval"]').value = '6h';
	}
}
</script>
`

	return template.HTML(html), nil
}

// ParseConfigForm parses form data into configuration
func (ui *UIProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
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
func (ui *UIProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentVacuumConfig()
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	vacuumConfig, ok := config.(*VacuumConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *VacuumConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(vacuumConfig.Enabled)
		ui.detector.SetGarbageThreshold(vacuumConfig.GarbageThreshold)
		ui.detector.SetScanInterval(vacuumConfig.ScanInterval)
		ui.detector.SetMinVolumeAge(vacuumConfig.MinVolumeAge)
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(vacuumConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(vacuumConfig.MaxConcurrent)
		ui.scheduler.SetMinInterval(vacuumConfig.MinInterval)
	}

	glog.V(1).Infof("Applied vacuum configuration: enabled=%v, threshold=%.1f%%, scan_interval=%v, max_concurrent=%d",
		vacuumConfig.Enabled, vacuumConfig.GarbageThreshold*100, vacuumConfig.ScanInterval, vacuumConfig.MaxConcurrent)

	return nil
}

// getCurrentVacuumConfig gets the current configuration from detector and scheduler
func (ui *UIProvider) getCurrentVacuumConfig() *VacuumConfig {
	config := &VacuumConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:          true,
		GarbageThreshold: 0.3,
		ScanInterval:     30 * time.Minute,
		MinVolumeAge:     1 * time.Hour,
		MaxConcurrent:    2,
		MinInterval:      6 * time.Hour,
	}

	// Get current values from detector
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.GarbageThreshold = ui.detector.GetGarbageThreshold()
		config.ScanInterval = ui.detector.ScanInterval()
		config.MinVolumeAge = ui.detector.GetMinVolumeAge()
	}

	// Get current values from scheduler
	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
		config.MinInterval = ui.scheduler.GetMinInterval()
	}

	return config
}

// RegisterUI registers the vacuum UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *VacuumDetector, scheduler *VacuumScheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered vacuum task UI provider")
}

// Example: How to get the UI provider for external use
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
