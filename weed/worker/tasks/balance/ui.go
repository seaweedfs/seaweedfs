package balance

import (
	"fmt"
	"html/template"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UIProvider provides the UI for balance task configuration
type UIProvider struct {
	detector  *BalanceDetector
	scheduler *BalanceScheduler
}

// NewUIProvider creates a new balance UI provider
func NewUIProvider(detector *BalanceDetector, scheduler *BalanceScheduler) *UIProvider {
	return &UIProvider{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetTaskType returns the task type
func (ui *UIProvider) GetTaskType() types.TaskType {
	return types.TaskTypeBalance
}

// GetDisplayName returns the human-readable name
func (ui *UIProvider) GetDisplayName() string {
	return "Volume Balance"
}

// GetDescription returns a description of what this task does
func (ui *UIProvider) GetDescription() string {
	return "Redistributes volumes across volume servers to optimize storage utilization and performance"
}

// BalanceConfig represents the balance configuration
type BalanceConfig struct {
	Enabled            bool          `json:"enabled"`
	ImbalanceThreshold float64       `json:"imbalance_threshold"`
	ScanInterval       time.Duration `json:"scan_interval"`
	MaxConcurrent      int           `json:"max_concurrent"`
	MinServerCount     int           `json:"min_server_count"`
	MoveDuringOffHours bool          `json:"move_during_off_hours"`
	OffHoursStart      string        `json:"off_hours_start"`
	OffHoursEnd        string        `json:"off_hours_end"`
}

// RenderConfigForm renders the configuration form HTML
func (ui *UIProvider) RenderConfigForm(currentConfig interface{}) (template.HTML, error) {
	config := ui.getCurrentBalanceConfig()

	// Build form using the FormBuilder helper
	form := types.NewFormBuilder()

	// Detection Settings
	form.AddCheckboxField(
		"enabled",
		"Enable Balance Tasks",
		"Whether balance tasks should be automatically created",
		config.Enabled,
	)

	form.AddNumberField(
		"imbalance_threshold",
		"Imbalance Threshold (%)",
		"Trigger balance when storage imbalance exceeds this percentage (0.0-1.0)",
		config.ImbalanceThreshold,
		true,
	)

	form.AddDurationField(
		"scan_interval",
		"Scan Interval",
		"How often to scan for imbalanced volumes",
		config.ScanInterval,
		true,
	)

	// Scheduling Settings
	form.AddNumberField(
		"max_concurrent",
		"Max Concurrent Tasks",
		"Maximum number of balance tasks that can run simultaneously",
		float64(config.MaxConcurrent),
		true,
	)

	form.AddNumberField(
		"min_server_count",
		"Minimum Server Count",
		"Only balance when at least this many servers are available",
		float64(config.MinServerCount),
		true,
	)

	// Timing Settings
	form.AddCheckboxField(
		"move_during_off_hours",
		"Restrict to Off-Hours",
		"Only perform balance operations during off-peak hours",
		config.MoveDuringOffHours,
	)

	form.AddTextField(
		"off_hours_start",
		"Off-Hours Start Time",
		"Start time for off-hours window (e.g., 23:00)",
		config.OffHoursStart,
		false,
	)

	form.AddTextField(
		"off_hours_end",
		"Off-Hours End Time",
		"End time for off-hours window (e.g., 06:00)",
		config.OffHoursEnd,
		false,
	)

	// Wrap in a form with proper sections
	html := `<div class="balance-config">
	<div class="config-section">
		<h3>Detection Settings</h3>
		<p>Configure when balance tasks should be triggered based on storage imbalance.</p>
	</div>
	` + string(form.Build()) + `
	<div class="config-section">
		<h3>Performance Impact</h3>
		<div class="info-box">
			<p><strong>Note:</strong> Volume balancing involves data movement and can impact cluster performance.</p>
			<p><strong>Recommendation:</strong> Enable off-hours restriction to minimize impact on production workloads.</p>
			<p><strong>Safety:</strong> Requires at least ` + fmt.Sprintf("%d", config.MinServerCount) + ` servers to ensure data safety during moves.</p>
		</div>
	</div>
	</div>`

	return template.HTML(html), nil
}

// ParseConfigForm parses form data into configuration
func (ui *UIProvider) ParseConfigForm(formData map[string][]string) (interface{}, error) {
	config := &BalanceConfig{}

	// Parse enabled
	config.Enabled = len(formData["enabled"]) > 0

	// Parse imbalance threshold
	if values, ok := formData["imbalance_threshold"]; ok && len(values) > 0 {
		threshold, err := strconv.ParseFloat(values[0], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid imbalance threshold: %v", err)
		}
		if threshold < 0 || threshold > 1 {
			return nil, fmt.Errorf("imbalance threshold must be between 0.0 and 1.0")
		}
		config.ImbalanceThreshold = threshold
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

	// Parse min server count
	if values, ok := formData["min_server_count"]; ok && len(values) > 0 {
		minServerCount, err := strconv.Atoi(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid min server count: %v", err)
		}
		if minServerCount < 2 {
			return nil, fmt.Errorf("min server count must be at least 2")
		}
		config.MinServerCount = minServerCount
	}

	// Parse off-hours settings
	config.MoveDuringOffHours = len(formData["move_during_off_hours"]) > 0

	if values, ok := formData["off_hours_start"]; ok && len(values) > 0 {
		config.OffHoursStart = values[0]
	}

	if values, ok := formData["off_hours_end"]; ok && len(values) > 0 {
		config.OffHoursEnd = values[0]
	}

	return config, nil
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
	return ui.getCurrentBalanceConfig()
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	balanceConfig, ok := config.(*BalanceConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *BalanceConfig")
	}

	// Apply to detector
	if ui.detector != nil {
		ui.detector.SetEnabled(balanceConfig.Enabled)
		// Note: would need setters for imbalance threshold and scan interval
	}

	// Apply to scheduler
	if ui.scheduler != nil {
		ui.scheduler.SetMaxConcurrent(balanceConfig.MaxConcurrent)
		// Note: would need setters for min server count and off-hours settings
	}

	glog.V(1).Infof("Applied balance configuration: enabled=%v, threshold=%.1f%%, max_concurrent=%d, min_servers=%d",
		balanceConfig.Enabled, balanceConfig.ImbalanceThreshold*100, balanceConfig.MaxConcurrent, balanceConfig.MinServerCount)

	return nil
}

// getCurrentBalanceConfig gets the current configuration from detector and scheduler
func (ui *UIProvider) getCurrentBalanceConfig() *BalanceConfig {
	config := &BalanceConfig{
		// Default values
		Enabled:            true,
		ImbalanceThreshold: 0.1, // 10% imbalance
		ScanInterval:       4 * time.Hour,
		MaxConcurrent:      1,
		MinServerCount:     3,
		MoveDuringOffHours: true,
		OffHoursStart:      "23:00",
		OffHoursEnd:        "06:00",
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

// RegisterUI registers the balance UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *BalanceDetector, scheduler *BalanceScheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered balance task UI provider")
}
