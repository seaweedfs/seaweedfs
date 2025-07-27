package balance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BalanceConfig represents the balance configuration matching the schema
type BalanceConfig struct {
	Enabled             bool    `json:"enabled"`
	ImbalanceThreshold  float64 `json:"imbalance_threshold"`
	ScanIntervalSeconds int     `json:"scan_interval_seconds"`
	MaxConcurrent       int     `json:"max_concurrent"`
	MinServerCount      int     `json:"min_server_count"`
}

// BalanceUILogic contains the business logic for balance UI operations
type BalanceUILogic struct {
	detector  *BalanceDetector
	scheduler *BalanceScheduler
}

// NewBalanceUILogic creates new balance UI logic
func NewBalanceUILogic(detector *BalanceDetector, scheduler *BalanceScheduler) *BalanceUILogic {
	return &BalanceUILogic{
		detector:  detector,
		scheduler: scheduler,
	}
}

// GetCurrentConfig returns the current balance configuration
func (logic *BalanceUILogic) GetCurrentConfig() interface{} {
	config := &BalanceConfig{
		// Default values from schema (matching task_config_schema.go)
		Enabled:             true,
		ImbalanceThreshold:  0.1,         // 10%
		ScanIntervalSeconds: 6 * 60 * 60, // 6 hours
		MaxConcurrent:       2,
		MinServerCount:      3,
	}

	// Get current values from detector
	if logic.detector != nil {
		config.Enabled = logic.detector.IsEnabled()
		config.ImbalanceThreshold = logic.detector.GetThreshold()
		config.ScanIntervalSeconds = int(logic.detector.ScanInterval().Seconds())
	}

	// Get current values from scheduler
	if logic.scheduler != nil {
		config.MaxConcurrent = logic.scheduler.GetMaxConcurrent()
		config.MinServerCount = logic.scheduler.GetMinServerCount()
	}

	return config
}

// ApplyConfig applies the balance configuration
func (logic *BalanceUILogic) ApplyConfig(config interface{}) error {
	balanceConfig, ok := config.(*BalanceConfig)
	if !ok {
		return nil // Will be handled by base provider fallback
	}

	// Apply to detector
	if logic.detector != nil {
		logic.detector.SetEnabled(balanceConfig.Enabled)
		logic.detector.SetThreshold(balanceConfig.ImbalanceThreshold)
		logic.detector.SetMinCheckInterval(time.Duration(balanceConfig.ScanIntervalSeconds) * time.Second)
	}

	// Apply to scheduler
	if logic.scheduler != nil {
		logic.scheduler.SetEnabled(balanceConfig.Enabled)
		logic.scheduler.SetMaxConcurrent(balanceConfig.MaxConcurrent)
		logic.scheduler.SetMinServerCount(balanceConfig.MinServerCount)
	}

	glog.V(1).Infof("Applied balance configuration: enabled=%v, threshold=%.1f%%, max_concurrent=%d, min_servers=%d",
		balanceConfig.Enabled, balanceConfig.ImbalanceThreshold*100, balanceConfig.MaxConcurrent,
		balanceConfig.MinServerCount)

	return nil
}

// RegisterUI registers the balance UI provider with the UI registry
func RegisterUI(uiRegistry *types.UIRegistry, detector *BalanceDetector, scheduler *BalanceScheduler) {
	logic := NewBalanceUILogic(detector, scheduler)

	tasks.CommonRegisterUI(
		types.TaskTypeBalance,
		"Volume Balance",
		uiRegistry,
		detector,
		scheduler,
		GetConfigSchema,
		logic.GetCurrentConfig,
		logic.ApplyConfig,
	)
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
