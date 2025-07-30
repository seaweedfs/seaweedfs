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

	// Verify task policies
	vacuumPolicy := config.Policy.TaskPolicies["vacuum"]
	if vacuumPolicy == nil {
		return fmt.Errorf("expected vacuum policy to be configured")
	}

	if !vacuumPolicy.Enabled {
		return fmt.Errorf("expected vacuum policy to be enabled")
	}

	// Verify typed configuration access
	vacuumConfig := vacuumPolicy.GetVacuumConfig()
	if vacuumConfig == nil {
		return fmt.Errorf("expected vacuum config to be accessible")
	}

	if vacuumConfig.GarbageThreshold != 0.3 {
		return fmt.Errorf("expected garbage threshold to be 0.3, got %f", vacuumConfig.GarbageThreshold)
	}

	// Verify helper functions work
	if !IsTaskEnabled(config.Policy, "vacuum") {
		return fmt.Errorf("expected vacuum task to be enabled via helper function")
	}

	maxConcurrent := GetMaxConcurrent(config.Policy, "vacuum")
	if maxConcurrent != 2 {
		return fmt.Errorf("expected vacuum max concurrent to be 2, got %d", maxConcurrent)
	}

	// Verify erasure coding configuration
	ecPolicy := config.Policy.TaskPolicies["erasure_coding"]
	if ecPolicy == nil {
		return fmt.Errorf("expected EC policy to be configured")
	}

	ecConfig := ecPolicy.GetErasureCodingConfig()
	if ecConfig == nil {
		return fmt.Errorf("expected EC config to be accessible")
	}

	// Verify configurable EC fields only
	if ecConfig.FullnessRatio <= 0 || ecConfig.FullnessRatio > 1 {
		return fmt.Errorf("expected EC config to have valid fullness ratio (0-1), got %f", ecConfig.FullnessRatio)
	}

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
			GlobalMaxConcurrent: 8,
			TaskPolicies: map[string]*worker_pb.TaskPolicy{
				"custom_vacuum": {
					Enabled:       true,
					MaxConcurrent: 4,
					TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
						VacuumConfig: &worker_pb.VacuumTaskConfig{
							GarbageThreshold:  0.5,
							MinVolumeAgeHours: 48,
						},
					},
				},
			},
		},
	}
}
