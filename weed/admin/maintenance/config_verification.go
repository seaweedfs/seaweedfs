package maintenance

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// VerifyProtobufConfig demonstrates that the protobuf configuration system is working
func VerifyProtobufConfig() error {
	// Create configuration manager
	configManager := NewMaintenanceConfigManager()
	config := configManager.GetConfig()

	// Verify basic configuration
	if !config.Enabled {
		return fmt.Errorf("expected config to be enabled by default")
	}

	if config.ScanIntervalSeconds != 30*60 {
		return fmt.Errorf("expected scan interval to be 1800 seconds, got %d", config.ScanIntervalSeconds)
	}

	// Verify policy configuration
	if config.Policy == nil {
		return fmt.Errorf("expected policy to be configured")
	}

	if config.Policy.GlobalMaxConcurrent != 4 {
		return fmt.Errorf("expected global max concurrent to be 4, got %d", config.Policy.GlobalMaxConcurrent)
	}

	// Note: Task policies are now generic - each task manages its own configuration
	// The maintenance system no longer knows about specific task types

	return nil
}

// GetProtobufConfigSummary returns a summary of the current protobuf configuration
func GetProtobufConfigSummary() string {
	configManager := NewMaintenanceConfigManager()
	config := configManager.GetConfig()

	summary := fmt.Sprintf("SeaweedFS Protobuf Maintenance Configuration:\n")
	summary += fmt.Sprintf("  Enabled: %v\n", config.Enabled)
	summary += fmt.Sprintf("  Scan Interval: %d seconds\n", config.ScanIntervalSeconds)
	summary += fmt.Sprintf("  Max Retries: %d\n", config.MaxRetries)
	summary += fmt.Sprintf("  Global Max Concurrent: %d\n", config.Policy.GlobalMaxConcurrent)
	summary += fmt.Sprintf("  Task Policies: %d configured\n", len(config.Policy.TaskPolicies))

	for taskType, policy := range config.Policy.TaskPolicies {
		summary += fmt.Sprintf("    %s: enabled=%v, max_concurrent=%d\n",
			taskType, policy.Enabled, policy.MaxConcurrent)
	}

	return summary
}

// CreateCustomConfig demonstrates creating a custom protobuf configuration
func CreateCustomConfig() *worker_pb.MaintenanceConfig {
	return &worker_pb.MaintenanceConfig{
		Enabled:             true,
		ScanIntervalSeconds: 60 * 60, // 1 hour
		MaxRetries:          5,
		Policy: &worker_pb.MaintenancePolicy{
			GlobalMaxConcurrent:          8,
			DefaultRepeatIntervalSeconds: 7200, // 2 hours
			DefaultCheckIntervalSeconds:  1800, // 30 minutes
			TaskPolicies:                 make(map[string]*worker_pb.TaskPolicy),
		},
	}
}
