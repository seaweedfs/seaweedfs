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
	Enabled               bool    `json:"enabled"`
	VolumeAgeHoursSeconds int     `json:"volume_age_hours_seconds"`
	FullnessRatio         float64 `json:"fullness_ratio"`
	ScanIntervalSeconds   int     `json:"scan_interval_seconds"`
	MaxConcurrent         int     `json:"max_concurrent"`
	ShardCount            int     `json:"shard_count"`
	ParityCount           int     `json:"parity_count"`
	CollectionFilter      string  `json:"collection_filter"`
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

	form.AddNumberField(
		"volume_age_hours_seconds",
		"Volume Age Threshold",
		"Only apply erasure coding to volumes older than this duration",
		float64(config.VolumeAgeHoursSeconds),
		true,
	)

	form.AddNumberField(
		"scan_interval_seconds",
		"Scan Interval",
		"How often to scan for volumes needing erasure coding",
		float64(config.ScanIntervalSeconds),
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
		"shard_count",
		"Data Shards",
		"Number of data shards for erasure coding (recommended: 10)",
		float64(config.ShardCount),
		true,
	)

	form.AddNumberField(
		"parity_count",
		"Parity Shards",
		"Number of parity shards for erasure coding (recommended: 4)",
		float64(config.ParityCount),
		true,
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
					<p class="mb-0"><strong>Durability:</strong> With ` + fmt.Sprintf("%d+%d", config.ShardCount, config.ParityCount) + ` configuration, can tolerate up to ` + fmt.Sprintf("%d", config.ParityCount) + ` shard failures.</p>
				</div>
			</div>
		</div>
	</div>
</div>`

	return template.HTML(html), nil
}

// ParseConfigForm parses form data into configuration
func (ui *UIProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &ErasureCodingConfig{}

	// Parse enabled
	config.Enabled = len(formData["enabled"]) > 0

	// Parse volume age hours
	if values, ok := formData["volume_age_hours_seconds"]; ok && len(values) > 0 {
		hours, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid volume age hours: %v", err)
		}
		config.VolumeAgeHoursSeconds = hours
	}

	// Parse scan interval
	if values, ok := formData["scan_interval_seconds"]; ok && len(values) > 0 {
		interval, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid scan interval: %v", err)
		}
		config.ScanIntervalSeconds = interval
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

	// Parse shard count
	if values, ok := formData["shard_count"]; ok && len(values) > 0 {
		shardCount, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid shard count: %v", err)
		}
		if shardCount < 1 {
			return nil, fmt.Errorf("shard count must be at least 1")
		}
		config.ShardCount = shardCount
	}

	// Parse parity count
	if values, ok := formData["parity_count"]; ok && len(values) > 0 {
		parityCount, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid parity count: %v", err)
		}
		if parityCount < 1 {
			return nil, fmt.Errorf("parity count must be at least 1")
		}
		config.ParityCount = parityCount
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
		ui.detector.SetVolumeAgeHours(ecConfig.VolumeAgeHoursSeconds)
		ui.detector.SetScanInterval(secondsToDuration(ecConfig.ScanIntervalSeconds))
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetEnabled(ecConfig.Enabled)
		ui.scheduler.SetMaxConcurrent(ecConfig.MaxConcurrent)
	}

	glog.V(1).Infof("Applied erasure coding configuration: enabled=%v, age_threshold=%v, max_concurrent=%d, shards=%d+%d",
		ecConfig.Enabled, ecConfig.VolumeAgeHoursSeconds, ecConfig.MaxConcurrent, ecConfig.ShardCount, ecConfig.ParityCount)

	return nil
}

// getCurrentECConfig gets the current configuration from detector and scheduler
func (ui *UIProvider) getCurrentECConfig() ErasureCodingConfig {
	config := ErasureCodingConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:               true,
		VolumeAgeHoursSeconds: 24 * 3600, // 24 hours in seconds
		ScanIntervalSeconds:   2 * 3600,  // 2 hours in seconds
		MaxConcurrent:         1,
		ShardCount:            10,
		ParityCount:           4,
	}

	// Get current values from detector
	if ui.detector != nil {
		config.Enabled = ui.detector.IsEnabled()
		config.VolumeAgeHoursSeconds = ui.detector.GetVolumeAgeHours()
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
