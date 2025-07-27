package balance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
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

// BalanceConfig represents the balance configuration matching the schema
type BalanceConfig struct {
	Enabled             bool    `json:"enabled"`
	ImbalanceThreshold  float64 `json:"imbalance_threshold"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	MinServerCount      int     `json:"min_server_count"`
}

// GetCurrentConfig returns the current configuration
func (ui *UIProvider) GetCurrentConfig() interface{} {
	config := &BalanceConfig{
		// Default values from schema (matching task_config_schema.go)
		Enabled:             true,
		ImbalanceThreshold:  0.1,         // 10%
		ScanIntervalSeconds: 6 * 60 * 60, // 6 hours
		MaxConcurrent:       2,
		MinServerCount:      3,
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
	}

	return config
}

// ApplyConfig applies the new configuration
func (ui *UIProvider) ApplyConfig(config interface{}) error {
	balanceConfig, ok := config.(*BalanceConfig)
	if !ok {
		// Try to get the configuration from the schema-based system
		schema := tasks.GetBalanceTaskConfigSchema()
		if schema != nil {
			// Apply defaults to ensure we have a complete config
			if err := schema.ApplyDefaults(config); err != nil {
				return err
			}

			// Use reflection to convert to BalanceConfig - simplified approach
			if bc, ok := config.(*BalanceConfig); ok {
				balanceConfig = bc
			} else {
				glog.Warningf("Config type conversion failed, using current config")
				balanceConfig = ui.GetCurrentConfig().(*BalanceConfig)
			}
		}
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
	}

	glog.V(1).Infof("Applied balance configuration: enabled=%v, threshold=%.1f%%, max_concurrent=%d, min_servers=%d",
		balanceConfig.Enabled, balanceConfig.ImbalanceThreshold*100, balanceConfig.MaxConcurrent,
		balanceConfig.MinServerCount)

	return nil
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
		ImbalanceThreshold:  0.1,         // 10%
		ScanIntervalSeconds: 6 * 60 * 60, // 6 hours
		MaxConcurrent:       2,
		MinServerCount:      3,
	}
}
