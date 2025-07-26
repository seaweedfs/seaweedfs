package erasure_coding

import (
	"fmt"
	"html/template"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// ErasureCodingConfig represents the erasure coding configuration
type ErasureCodingConfig struct {
	Enabled             bool    `json:"enabled"`
	QuietForSeconds     int     `json:"quiet_for_seconds"`
	FullnessRatio       float64 `json:"fullness_ratio"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	CollectionFilter    string  `json:"collection_filter"`
}

// Helper functions for duration conversion
func secondsToDuration(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}

func durationToSeconds(d time.Duration) int {
	return int(d.Seconds())
}

// formatDurationForUser formats seconds as a user-friendly duration string
func formatDurationForUser(seconds int) string {
	d := secondsToDuration(seconds)
	if d < time.Minute {
		return fmt.Sprintf("%ds", seconds)
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

// RenderConfigForm renders the configuration form HTML
func (ui *UIProvider) RenderConfigForm(currentConfig interface{}) (template.HTML, error) {
	config := ui.getCurrentECConfig()

	// Build form using the FormBuilder helper
	form := types.NewFormBuilder()

	// Detection Settings
	form.AddCheckboxField(
		"enabled",
		"Enable Erasure Coding Tasks",
		"Whether erasure coding tasks should be automatically created",
		config.Enabled,
	)

	form.AddIntervalField(
		"quiet_for_seconds",
		"Quiet For Duration",
		"Only apply erasure coding to volumes that have not been modified for this duration",
		config.QuietForSeconds,
		true,
	)

	form.AddIntervalField(
		"scan_interval_seconds",
		"Scan Interval",
		"How often to scan for volumes needing erasure coding",
		config.ScanIntervalSeconds,
		true,
	)

	// Scheduling Settings
	form.AddNumberField(
		"max_concurrent",
		"Max Concurrent Tasks",
		"Maximum number of erasure coding tasks that can run simultaneously",
		float64(config.MaxConcurrent),
		true,
	)

	// Detection Parameters
	form.AddNumberField(
		"fullness_ratio",
		"Fullness Ratio (0.0-1.0)",
		"Only apply erasure coding to volumes with fullness ratio above this threshold (e.g., 0.90 for 90%)",
		config.FullnessRatio,
		true,
	)

	form.AddTextField(
		"collection_filter",
		"Collection Filter",
		"Only apply erasure coding to volumes in these collections (comma-separated, leave empty for all collections)",
		config.CollectionFilter,
		false,
	)

	// Generate organized form sections using Bootstrap components
	html := `
<div class="row">
	<div class="col-12">
		<div class="card mb-4">
			<div class="card-header">
				<h5 class="mb-0">
					<i class="fas fa-shield-alt me-2"></i>
					Erasure Coding Configuration
				</h5>
			</div>
			<div class="card-body">
` + string(form.Build()) + `
			</div>
		</div>
	</div>
</div>

<div class="row">
	<div class="col-12">
		<div class="card mb-3">
			<div class="card-header">
				<h5 class="mb-0">
					<i class="fas fa-info-circle me-2"></i>
					Performance Impact
				</h5>
			</div>
			<div class="card-body">
				<div class="alert alert-info" role="alert">
					<h6 class="alert-heading">Important Notes:</h6>
					<p class="mb-2"><strong>Performance:</strong> Erasure coding is CPU and I/O intensive. Consider running during off-peak hours.</p>
					<p class="mb-2"><strong>Durability:</strong> With 10+4 configuration, can tolerate up to 4 shard failures.</p>
					<p class="mb-0"><strong>Configuration:</strong> Use the dropdown to select time units (days, hours, minutes). Fullness ratio should be between 0.0 and 1.0 (e.g., 0.90 for 90%).</p>
				</div>
			</div>
		</div>
	</div>
</div>`

	return template.HTML(html), nil
}

// ParseConfigForm parses form data into configuration
func (ui *UIProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := ErasureCodingConfig{}

	// Parse enabled
	config.Enabled = len(formData["enabled"]) > 0

	// Parse quiet for duration
	if values, ok := formData["quiet_for_seconds_value"]; ok && len(values) > 0 {
		value, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid quiet for duration value: %w", err)
		}

		unit := "minute" // default
		if units, ok := formData["quiet_for_seconds_unit"]; ok && len(units) > 0 {
			unit = units[0]
		}

		// Convert to seconds using the helper function from types package
		config.QuietForSeconds = types.IntervalValueUnitToSeconds(value, unit)
	}

	// Parse scan interval
	if values, ok := formData["scan_interval_seconds_value"]; ok && len(values) > 0 {
		value, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid scan interval value: %w", err)
		}

		unit := "minute" // default
		if units, ok := formData["scan_interval_seconds_unit"]; ok && len(units) > 0 {
			unit = units[0]
		}

		// Convert to seconds
		config.ScanIntervalSeconds = types.IntervalValueUnitToSeconds(value, unit)
	}

	// Parse max concurrent
	if values, ok := formData["max_concurrent"]; ok && len(values) > 0 {
		maxConcurrent, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid max concurrent: %w", err)
		}
		if maxConcurrent < 1 {
			return nil, fmt.Errorf("max concurrent must be at least 1")
		}
		config.MaxConcurrent = maxConcurrent
	}

	// Parse fullness ratio
	if values, ok := formData["fullness_ratio"]; ok && len(values) > 0 {
		fullnessRatio, err := strconv.ParseFloat(values[0], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid fullness ratio: %w", err)
		}
		if fullnessRatio < 0 || fullnessRatio > 1 {
			return nil, fmt.Errorf("fullness ratio must be between 0.0 and 1.0")
		}
		config.FullnessRatio = fullnessRatio
	}

	// Parse collection filter
	if values, ok := formData["collection_filter"]; ok && len(values) > 0 {
		config.CollectionFilter = values[0]
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentECConfig()
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	ecConfig, ok := config.(ErasureCodingConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected ErasureCodingConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(ecConfig.Enabled)
		ui.detector.SetQuietForSeconds(ecConfig.QuietForSeconds)
		ui.detector.SetFullnessRatio(ecConfig.FullnessRatio)
		ui.detector.SetCollectionFilter(ecConfig.CollectionFilter)
		ui.detector.SetScanInterval(secondsToDuration(ecConfig.ScanIntervalSeconds))
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(ecConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, quiet_for=%v seconds, max_concurrent=%d, fullness_ratio=%f, collection_filter=%s, shards=10+4",
		ecConfig.Enabled, ecConfig.QuietForSeconds, ecConfig.MaxConcurrent, ecConfig.FullnessRatio, ecConfig.CollectionFilter)

	return nil
}

// getCurrentECConfig gets the current configuration from detector and scheduler
func (ui *UIProvider) getCurrentECConfig() ErasureCodingConfig {
	config := ErasureCodingConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:             true,
		QuietForSeconds:     24 * 3600, // Default to 24 hours in seconds
		ScanIntervalSeconds: 2 * 3600,  // 2 hours in seconds
		MaxConcurrent:       1,
		FullnessRatio:       0.90, // Default fullness ratio
		CollectionFilter:    "",
	}

	// Get current values from detector
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.QuietForSeconds = ui.detector.GetQuietForSeconds()
		config.FullnessRatio = ui.detector.GetFullnessRatio()
		config.CollectionFilter = ui.detector.GetCollectionFilter()
		config.ScanIntervalSeconds = durationToSeconds(ui.detector.ScanInterval())
	}

	// Get current values from scheduler
	if ui.scheduler != nil {
		config.MaxConcurrent = ui.scheduler.GetMaxConcurrent()
	}

	return config
}

// RegisterUI registers the erasure coding UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *EcDetector, scheduler *Scheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered erasure coding task UI provider")
}
