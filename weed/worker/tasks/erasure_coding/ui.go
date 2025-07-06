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

// ECConfig represents the erasure coding configuration
type ECConfig struct {
	Enabled            bool          `json:"enabled"`
	VolumeAgeThreshold time.Duration `json:"volume_age_threshold"`
	ScanInterval       time.Duration `json:"scan_interval"`
	MaxConcurrent      int           `json:"max_concurrent"`
	DataShards         int           `json:"data_shards"`
	ParityShards       int           `json:"parity_shards"`
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

	form.AddDurationField(
		"volume_age_threshold",
		"Volume Age Threshold",
		"Only apply erasure coding to volumes older than this duration",
		config.VolumeAgeThreshold,
		true,
	)

	form.AddDurationField(
		"scan_interval",
		"Scan Interval",
		"How often to scan for volumes needing erasure coding",
		config.ScanInterval,
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

	// Erasure Coding Parameters
	form.AddNumberField(
		"data_shards",
		"Data Shards",
		"Number of data shards for erasure coding (recommended: 10)",
		float64(config.DataShards),
		true,
	)

	form.AddNumberField(
		"parity_shards",
		"Parity Shards",
		"Number of parity shards for erasure coding (recommended: 4)",
		float64(config.ParityShards),
		true,
	)

	// Wrap in a form with proper sections
	html := `<div class="erasure-coding-config">
	<div class="config-section">
		<h3>Detection Settings</h3>
		<p>Configure when erasure coding tasks should be triggered.</p>
	</div>
	` + string(form.Build()) + `
	<div class="config-section">
		<h3>Performance Impact</h3>
		<div class="info-box">
			<p><strong>Note:</strong> Erasure coding is CPU and I/O intensive. Consider running during off-peak hours.</p>
			<p><strong>Durability:</strong> With ` + fmt.Sprintf("%d+%d", config.DataShards, config.ParityShards) + ` configuration, can tolerate up to ` + fmt.Sprintf("%d", config.ParityShards) + ` shard failures.</p>
		</div>
	</div>
	</div>`

	return template.HTML(html), nil
}

// ParseConfigForm parses form data into configuration
func (ui *UIProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &ECConfig{}

	// Parse enabled
	config.Enabled = len(formData["enabled"]) > 0

	// Parse volume age threshold
	if values, ok := formData["volume_age_threshold"]; ok && len(values) > 0 {
		duration, err := time.ParseDuration(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid volume age threshold: %v", err)
		}
		config.VolumeAgeThreshold = duration
	}

	// Parse scan interval
	if values, ok := formData["scan_interval"]; ok && len(values) > 0 {
		duration, err := time.ParseDuration(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid scan interval: %v", err)
		}
		config.ScanInterval = duration
	}

	// Parse max concurrent
	if values, ok := formData["max_concurrent"]; ok && len(values) > 0 {
		maxConcurrent, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid max concurrent: %v", err)
		}
		if maxConcurrent < 1 {
			return nil, fmt.Errorf("max concurrent must be at least 1")
		}
		config.MaxConcurrent = maxConcurrent
	}

	// Parse data shards
	if values, ok := formData["data_shards"]; ok && len(values) > 0 {
		dataShards, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid data shards: %v", err)
		}
		if dataShards < 1 {
			return nil, fmt.Errorf("data shards must be at least 1")
		}
		config.DataShards = dataShards
	}

	// Parse parity shards
	if values, ok := formData["parity_shards"]; ok && len(values) > 0 {
		parityShards, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid parity shards: %v", err)
		}
		if parityShards < 1 {
			return nil, fmt.Errorf("parity shards must be at least 1")
		}
		config.ParityShards = parityShards
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentECConfig()
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	ecConfig, ok := config.(*ECConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *ECConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(ecConfig.Enabled)
		// Note: would need setters for volume age threshold and scan interval
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, max_concurrent=%d, shards=%d+%d",
		ecConfig.Enabled, ecConfig.MaxConcurrent, ecConfig.DataShards, ecConfig.ParityShards)

	return nil
}

// getCurrentECConfig gets the current configuration from detector and scheduler
func (ui *UIProvider) getCurrentECConfig() *ECConfig {
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

// RegisterUI registers the erasure coding UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *EcDetector, scheduler *Scheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered erasure coding task UI provider")
}
