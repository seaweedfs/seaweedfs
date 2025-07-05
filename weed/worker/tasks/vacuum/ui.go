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
	detector  *SimpleDetector
	scheduler *SimpleScheduler
}

// NewUIProvider creates a new vacuum UI provider
func NewUIProvider(detector *SimpleDetector, scheduler *SimpleScheduler) *UIProvider {
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

	// Wrap in a form with proper sections
	html := `
<div class="task-config-container">
	<h3>Vacuum Task Configuration</h3>
	<p class="task-description">` + ui.GetDescription() + `</p>
	
	<form method="POST" action="/admin/maintenance/config/vacuum" class="task-config-form">
		<fieldset class="config-section">
			<legend>Detection Settings</legend>
			<div class="config-fields">
` + string(form.Build()) + `
			</div>
		</fieldset>
		
		<div class="form-actions">
			<button type="submit" class="btn-primary">Update Vacuum Configuration</button>
			<button type="button" class="btn-secondary" onclick="resetForm()">Reset</button>
		</div>
	</form>
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

<style>
.task-config-container {
	max-width: 600px;
	margin: 0 auto;
	padding: 20px;
}

.config-section {
	border: 1px solid #ddd;
	border-radius: 6px;
	padding: 20px;
	margin-bottom: 20px;
}

.config-section legend {
	font-weight: bold;
	padding: 0 10px;
	color: #333;
}

.form-field {
	margin-bottom: 15px;
}

.form-field label {
	display: block;
	font-weight: 500;
	margin-bottom: 5px;
	color: #555;
}

.form-field input, .form-field select {
	width: 100%;
	padding: 8px 12px;
	border: 1px solid #ccc;
	border-radius: 4px;
	font-size: 14px;
}

.form-field input[type="checkbox"] {
	width: auto;
	margin-right: 8px;
}

.field-description {
	display: block;
	color: #666;
	font-size: 12px;
	margin-top: 4px;
	font-style: italic;
}

.form-actions {
	text-align: right;
	padding-top: 20px;
	border-top: 1px solid #eee;
}

.btn-primary, .btn-secondary {
	padding: 10px 20px;
	border: none;
	border-radius: 4px;
	cursor: pointer;
	margin-left: 10px;
	font-size: 14px;
}

.btn-primary {
	background-color: #007bff;
	color: white;
}

.btn-secondary {
	background-color: #6c757d;
	color: white;
}

.btn-primary:hover {
	background-color: #0056b3;
}

.btn-secondary:hover {
	background-color: #545b62;
}
</style>
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
		// Note: scan interval and min volume age would need setters
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
func (ui *UIProvider) getCurrentVacuumConfig() *VacuumConfig {
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
		// Note: would need getters for threshold and min age
	}

	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
		// Note: would need getter for min interval
	}

	return config
}

// RegisterUI registers the vacuum UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *SimpleDetector, scheduler *SimpleScheduler) {
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
