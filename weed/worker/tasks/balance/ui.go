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

// GetIcon returns the icon CSS class for this task type
func (ui *UIProvider) GetIcon() string {
	return "fas fa-balance-scale text-secondary"
}

// BalanceConfig represents the balance configuration
type BalanceConfig struct {
	Enabled             bool    `json:"enabled"`
	ImbalanceThreshold  float64 `json:"imbalance_threshold"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	MinServerCount      int     `json:"min_server_count"`
	MoveDuringOffHours  bool    `json:"move_during_off_hours"`
	OffHoursStart       string  `json:"off_hours_start"`
	OffHoursEnd         string  `json:"off_hours_end"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
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

	form.AddDurationField("scan_interval", "Scan Interval", "How often to scan for imbalanced volumes", secondsToDuration(config.ScanIntervalSeconds), true)

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

	// Timing constraints
	form.AddDurationField("min_interval", "Min Interval", "Minimum time between balance operations", secondsToDuration(config.MinIntervalSeconds), true)

	// Generate organized form sections using Bootstrap components
	html := `
<div class="row">
	<div class="col-12">
		<div class="card mb-4">
			<div class="card-header">
				<h5 class="mb-0">
					<i class="fas fa-balance-scale me-2"></i>
					Balance Configuration
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
					<i class="fas fa-exclamation-triangle me-2"></i>
					Performance Considerations
				</h5>
			</div>
			<div class="card-body">
				<div class="alert alert-warning" role="alert">
					<h6 class="alert-heading">Important Considerations:</h6>
					<p class="mb-2"><strong>Performance:</strong> Volume balancing involves data movement and can impact cluster performance.</p>
					<p class="mb-2"><strong>Recommendation:</strong> Enable off-hours restriction to minimize impact on production workloads.</p>
					<p class="mb-0"><strong>Safety:</strong> Requires at least ` + fmt.Sprintf("%d", config.MinServerCount) + ` servers to ensure data safety during moves.</p>
				</div>
			</div>
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
		config.ScanIntervalSeconds = int(duration.Seconds())
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

	// Parse min interval
	if values, ok := formData["min_interval"]; ok && len(values) > 0 {
		duration, err := time.ParseDuration(values[0])
		if err != nil {
			return nil, fmt.Errorf("invalid min interval: %v", err)
		}
		config.MinIntervalSeconds = int(duration.Seconds())
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
		ui.detector.SetThreshold(balanceConfig.ImbalanceThreshold)
		ui.detector.SetMinCheckInterval(secondsToDuration(balanceConfig.ScanIntervalSeconds))
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
func (ui *UIProvider) getCurrentBalanceConfig() *BalanceConfig {
	config := &BalanceConfig{
		// Default values (fallback if detectors/schedulers are nil)
		Enabled:             true,
		ImbalanceThreshold:  0.1, // 10% imbalance
		ScanIntervalSeconds: durationToSeconds(4 * time.Hour),
		MaxConcurrent:       1,
		MinServerCount:      3,
		MoveDuringOffHours:  true,
		OffHoursStart:       "23:00",
		OffHoursEnd:         "06:00",
		MinIntervalSeconds:  durationToSeconds(1 * time.Hour),
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

// RegisterUI registers the balance UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *BalanceDetector, scheduler *BalanceScheduler) {
	uiProvider := NewUIProvider(detector, scheduler)
	uiRegistry.RegisterUI(uiProvider)

	glog.V(1).Infof("✅ Registered balance task UI provider")
}

// DefaultBalanceConfig returns default balance configuration
func DefaultBalanceConfig() *BalanceConfig {
	return &BalanceConfig{
		Enabled:             false,
		ImbalanceThreshold:  0.3,
		ScanIntervalSeconds: durationToSeconds(4 * time.Hour),
		MaxConcurrent:       1,
		MinServerCount:      3,
		MoveDuringOffHours:  false,
		OffHoursStart:       "22:00",
		OffHoursEnd:         "06:00",
		MinIntervalSeconds:  durationToSeconds(1 * time.Hour),
	}
}
