package ml

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"gopkg.in/yaml.v3"
)

// OptimizationConfigManager manages optimization configuration loading and validation
type OptimizationConfigManager struct {
	sync.RWMutex
	
	configDir        string
	loadedConfigs    map[string]*OptimizationConfig
	watchEnabled     bool
	validationRules  map[string]ValidationRule
}

// OptimizationConfig represents a complete optimization configuration
type OptimizationConfig struct {
	Version     string                   `json:"version" yaml:"version"`
	Name        string                   `json:"name" yaml:"name"`
	Description string                   `json:"description" yaml:"description"`
	Author      string                   `json:"author,omitempty" yaml:"author,omitempty"`
	Tags        []string                 `json:"tags,omitempty" yaml:"tags,omitempty"`
	
	// Core configuration
	Rules       []*OptimizationRule      `json:"rules" yaml:"rules"`
	Templates   []*OptimizationTemplate  `json:"templates" yaml:"templates"`
	Strategies  map[string]interface{}   `json:"strategies,omitempty" yaml:"strategies,omitempty"`
	
	// Framework-specific settings
	Frameworks  map[string]FrameworkConfig `json:"frameworks,omitempty" yaml:"frameworks,omitempty"`
	
	// Global settings
	Settings    GlobalOptimizationSettings `json:"settings" yaml:"settings"`
	
	// Metadata
	Metadata    map[string]interface{}   `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// FrameworkConfig holds framework-specific configuration
type FrameworkConfig struct {
	Enabled     bool                     `json:"enabled" yaml:"enabled"`
	Version     string                   `json:"version,omitempty" yaml:"version,omitempty"`
	Rules       []string                 `json:"rules,omitempty" yaml:"rules,omitempty"`
	Templates   []string                 `json:"templates,omitempty" yaml:"templates,omitempty"`
	Parameters  map[string]interface{}   `json:"parameters,omitempty" yaml:"parameters,omitempty"`
}

// GlobalOptimizationSettings contains global optimization settings
type GlobalOptimizationSettings struct {
	DefaultStrategy      string                 `json:"default_strategy" yaml:"default_strategy"`
	MaxConcurrentRules   int                    `json:"max_concurrent_rules" yaml:"max_concurrent_rules"`
	ConfidenceThreshold  float64                `json:"confidence_threshold" yaml:"confidence_threshold"`
	AdaptiveLearning     bool                   `json:"adaptive_learning" yaml:"adaptive_learning"`
	MetricsCollection    bool                   `json:"metrics_collection" yaml:"metrics_collection"`
	Debug                bool                   `json:"debug" yaml:"debug"`
	
	// Resource limits
	MemoryLimitMB        int                    `json:"memory_limit_mb,omitempty" yaml:"memory_limit_mb,omitempty"`
	CPULimitPercent      int                    `json:"cpu_limit_percent,omitempty" yaml:"cpu_limit_percent,omitempty"`
	
	// Advanced settings
	ExperimentalFeatures map[string]bool        `json:"experimental_features,omitempty" yaml:"experimental_features,omitempty"`
	CustomProperties     map[string]interface{} `json:"custom_properties,omitempty" yaml:"custom_properties,omitempty"`
}

// ValidationRule defines validation rules for configurations
type ValidationRule struct {
	Field       string   `json:"field"`
	Required    bool     `json:"required"`
	Type        string   `json:"type"`        // string, int, float, bool, array, object
	MinValue    *float64 `json:"min_value,omitempty"`
	MaxValue    *float64 `json:"max_value,omitempty"`
	AllowedValues []string `json:"allowed_values,omitempty"`
	Pattern     string   `json:"pattern,omitempty"` // regex pattern
}

// NewOptimizationConfigManager creates a new configuration manager
func NewOptimizationConfigManager(configDir string) *OptimizationConfigManager {
	return &OptimizationConfigManager{
		configDir:       configDir,
		loadedConfigs:   make(map[string]*OptimizationConfig),
		watchEnabled:    false,
		validationRules: getDefaultValidationRules(),
	}
}

// LoadConfiguration loads optimization configuration from file
func (ocm *OptimizationConfigManager) LoadConfiguration(filePath string) (*OptimizationConfig, error) {
	ocm.Lock()
	defer ocm.Unlock()
	
	// Check if already loaded
	if config, exists := ocm.loadedConfigs[filePath]; exists {
		return config, nil
	}
	
	// Read file
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}
	
	// Parse based on file extension
	config := &OptimizationConfig{}
	ext := strings.ToLower(filepath.Ext(filePath))
	
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config %s: %w", filePath, err)
		}
	case ".json":
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config %s: %w", filePath, err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}
	
	// Validate configuration
	if err := ocm.validateConfiguration(config); err != nil {
		return nil, fmt.Errorf("configuration validation failed for %s: %w", filePath, err)
	}
	
	// Process and enhance configuration
	ocm.processConfiguration(config)
	
	// Cache the configuration
	ocm.loadedConfigs[filePath] = config
	
	glog.V(1).Infof("Loaded optimization configuration: %s (%d rules, %d templates)", 
		config.Name, len(config.Rules), len(config.Templates))
	
	return config, nil
}

// LoadConfigurationDirectory loads all configuration files from a directory
func (ocm *OptimizationConfigManager) LoadConfigurationDirectory(dirPath string) ([]*OptimizationConfig, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration directory does not exist: %s", dirPath)
	}
	
	configs := make([]*OptimizationConfig, 0)
	
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if info.IsDir() {
			return nil
		}
		
		// Check if it's a config file
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yaml" && ext != ".yml" && ext != ".json" {
			return nil
		}
		
		config, loadErr := ocm.LoadConfiguration(path)
		if loadErr != nil {
			glog.Warningf("Failed to load configuration %s: %v", path, loadErr)
			return nil // Continue loading other files
		}
		
		configs = append(configs, config)
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to walk configuration directory: %w", err)
	}
	
	glog.V(1).Infof("Loaded %d optimization configurations from directory: %s", len(configs), dirPath)
	return configs, nil
}

// SaveConfiguration saves an optimization configuration to file
func (ocm *OptimizationConfigManager) SaveConfiguration(config *OptimizationConfig, filePath string) error {
	// Validate configuration before saving
	if err := ocm.validateConfiguration(config); err != nil {
		return fmt.Errorf("cannot save invalid configuration: %w", err)
	}
	
	// Serialize based on file extension
	ext := strings.ToLower(filepath.Ext(filePath))
	var data []byte
	var err error
	
	switch ext {
	case ".yaml", ".yml":
		data, err = yaml.Marshal(config)
		if err != nil {
			return fmt.Errorf("failed to marshal YAML: %w", err)
		}
	case ".json":
		data, err = json.MarshalIndent(config, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config file format: %s", ext)
	}
	
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}
	
	// Write file
	if err := ioutil.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	
	// Update cache
	ocm.Lock()
	ocm.loadedConfigs[filePath] = config
	ocm.Unlock()
	
	glog.V(1).Infof("Saved optimization configuration: %s", filePath)
	return nil
}

// GenerateDefaultConfiguration generates a comprehensive default configuration
func (ocm *OptimizationConfigManager) GenerateDefaultConfiguration() *OptimizationConfig {
	return &OptimizationConfig{
		Version:     "1.0.0",
		Name:        "Default ML Optimization Configuration",
		Description: "Comprehensive default optimization rules and templates for ML workloads",
		Author:      "SeaweedFS ML Optimization System",
		Tags:        []string{"default", "ml", "comprehensive"},
		
		Rules: []*OptimizationRule{
			{
				ID:          "smart_sequential_prefetch",
				Name:        "Smart Sequential Prefetching",
				Description: "Intelligent prefetching based on access patterns and file characteristics",
				Priority:    100,
				Conditions: []RuleCondition{
					{
						Type:     "access_pattern",
						Property: "pattern_type",
						Operator: "equals",
						Value:    "sequential",
						Weight:   1.0,
					},
					{
						Type:     "file_context",
						Property: "size",
						Operator: "greater_than",
						Value:    5 * 1024 * 1024, // 5MB
						Weight:   0.7,
					},
				},
				Actions: []RuleAction{
					{
						Type:   "prefetch",
						Target: "file",
						Parameters: map[string]interface{}{
							"strategy":         "adaptive",
							"initial_size":     8,
							"max_size":         32,
							"growth_factor":    1.5,
							"confidence_based": true,
						},
					},
				},
			},
			{
				ID:          "ml_file_type_optimization",
				Name:        "ML File Type Optimization",
				Description: "Optimizations based on detected ML file types",
				Priority:    95,
				Conditions: []RuleCondition{
					{
						Type:     "file_context",
						Property: "type",
						Operator: "in",
						Value:    []string{"model", "dataset", "checkpoint"},
						Weight:   1.0,
					},
				},
				Actions: []RuleAction{
					{
						Type:   "smart_cache",
						Target: "file",
						Parameters: map[string]interface{}{
							"strategy":        "ml_aware",
							"priority_boost":  2.0,
							"retention_time":  "extended",
						},
					},
				},
			},
			{
				ID:          "workload_aware_coordination",
				Name:        "Workload-Aware Coordination",
				Description: "Coordinate optimizations based on workload characteristics",
				Priority:    85,
				Conditions: []RuleCondition{
					{
						Type:     "workload_context",
						Property: "workload_type",
						Operator: "in",
						Value:    []string{"training", "inference", "preprocessing"},
						Weight:   0.9,
					},
					{
						Type:     "system_context",
						Property: "gpu_count",
						Operator: "greater_than",
						Value:    0,
						Weight:   0.6,
					},
				},
				Actions: []RuleAction{
					{
						Type:   "coordinate",
						Target: "workload",
						Parameters: map[string]interface{}{
							"resource_aware": true,
							"priority_scheduling": true,
							"gpu_coordination": true,
						},
					},
				},
			},
		},
		
		Templates: []*OptimizationTemplate{
			{
				ID:          "universal_ml_training",
				Name:        "Universal ML Training Template",
				Description: "Framework-agnostic optimization template for ML training",
				Category:    "training",
				Rules:       []string{"smart_sequential_prefetch", "ml_file_type_optimization", "workload_aware_coordination"},
				Parameters: map[string]interface{}{
					"optimization_level": "balanced",
					"resource_usage":     "moderate",
					"adaptivity":         true,
				},
			},
			{
				ID:          "inference_optimized",
				Name:        "Inference Optimization Template",
				Description: "Low-latency optimization template for ML inference",
				Category:    "inference",
				Rules:       []string{"ml_file_type_optimization"},
				Parameters: map[string]interface{}{
					"optimization_level": "latency",
					"preload_models":     true,
					"batch_processing":   false,
				},
			},
		},
		
		Frameworks: map[string]FrameworkConfig{
			"pytorch": {
				Enabled: true,
				Rules:   []string{"smart_sequential_prefetch", "ml_file_type_optimization"},
				Parameters: map[string]interface{}{
					"dataloader_optimization": true,
					"tensor_prefetch":         true,
				},
			},
			"tensorflow": {
				Enabled: true,
				Rules:   []string{"smart_sequential_prefetch", "workload_aware_coordination"},
				Parameters: map[string]interface{}{
					"dataset_optimization": true,
					"savedmodel_caching":   true,
				},
			},
		},
		
		Settings: GlobalOptimizationSettings{
			DefaultStrategy:      "adaptive",
			MaxConcurrentRules:   5,
			ConfidenceThreshold:  0.6,
			AdaptiveLearning:     true,
			MetricsCollection:    true,
			Debug:                false,
			MemoryLimitMB:        512,
			CPULimitPercent:      20,
			ExperimentalFeatures: map[string]bool{
				"neural_optimization": false,
				"quantum_prefetch":    false,
				"blockchain_cache":    false, // Just kidding :)
			},
		},
		
		Metadata: map[string]interface{}{
			"generated_at":    "auto",
			"config_version":  "1.0.0",
			"compatible_with": []string{"seaweedfs-ml-v1"},
		},
	}
}

// validateConfiguration validates an optimization configuration
func (ocm *OptimizationConfigManager) validateConfiguration(config *OptimizationConfig) error {
	if config == nil {
		return fmt.Errorf("configuration is nil")
	}
	
	// Basic validation
	if config.Name == "" {
		return fmt.Errorf("configuration name is required")
	}
	
	if config.Version == "" {
		return fmt.Errorf("configuration version is required")
	}
	
	// Validate rules
	ruleIDs := make(map[string]bool)
	for i, rule := range config.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule at index %d is missing ID", i)
		}
		
		if ruleIDs[rule.ID] {
			return fmt.Errorf("duplicate rule ID: %s", rule.ID)
		}
		ruleIDs[rule.ID] = true
		
		// Validate rule structure
		if err := ocm.validateRule(rule); err != nil {
			return fmt.Errorf("rule '%s' validation failed: %w", rule.ID, err)
		}
	}
	
	// Validate templates
	templateIDs := make(map[string]bool)
	for i, template := range config.Templates {
		if template.ID == "" {
			return fmt.Errorf("template at index %d is missing ID", i)
		}
		
		if templateIDs[template.ID] {
			return fmt.Errorf("duplicate template ID: %s", template.ID)
		}
		templateIDs[template.ID] = true
		
		// Validate template references
		for _, ruleID := range template.Rules {
			if !ruleIDs[ruleID] {
				return fmt.Errorf("template '%s' references unknown rule: %s", template.ID, ruleID)
			}
		}
	}
	
	// Validate settings
	if config.Settings.ConfidenceThreshold < 0.0 || config.Settings.ConfidenceThreshold > 1.0 {
		return fmt.Errorf("confidence threshold must be between 0.0 and 1.0")
	}
	
	if config.Settings.MaxConcurrentRules < 1 {
		return fmt.Errorf("max concurrent rules must be at least 1")
	}
	
	return nil
}

// validateRule validates a single optimization rule
func (ocm *OptimizationConfigManager) validateRule(rule *OptimizationRule) error {
	if rule.Name == "" {
		return fmt.Errorf("rule name is required")
	}
	
	if rule.Priority < 0 {
		return fmt.Errorf("rule priority must be non-negative")
	}
	
	// Validate conditions
	for i, condition := range rule.Conditions {
		if condition.Type == "" {
			return fmt.Errorf("condition %d is missing type", i)
		}
		
		if condition.Property == "" {
			return fmt.Errorf("condition %d is missing property", i)
		}
		
		if condition.Operator == "" {
			return fmt.Errorf("condition %d is missing operator", i)
		}
		
		if condition.Weight < 0.0 || condition.Weight > 1.0 {
			return fmt.Errorf("condition %d weight must be between 0.0 and 1.0", i)
		}
	}
	
	// Validate actions
	if len(rule.Actions) == 0 {
		return fmt.Errorf("rule must have at least one action")
	}
	
	for i, action := range rule.Actions {
		if action.Type == "" {
			return fmt.Errorf("action %d is missing type", i)
		}
		
		if action.Target == "" {
			return fmt.Errorf("action %d is missing target", i)
		}
	}
	
	return nil
}

// processConfiguration processes and enhances a configuration after loading
func (ocm *OptimizationConfigManager) processConfiguration(config *OptimizationConfig) {
	// Set default values
	if config.Settings.DefaultStrategy == "" {
		config.Settings.DefaultStrategy = "adaptive"
	}
	
	if config.Settings.MaxConcurrentRules == 0 {
		config.Settings.MaxConcurrentRules = 3
	}
	
	if config.Settings.ConfidenceThreshold == 0.0 {
		config.Settings.ConfidenceThreshold = 0.5
	}
	
	// Process metadata
	if config.Metadata == nil {
		config.Metadata = make(map[string]interface{})
	}
	
	config.Metadata["processed_at"] = "runtime"
	config.Metadata["rule_count"] = len(config.Rules)
	config.Metadata["template_count"] = len(config.Templates)
}

// getDefaultValidationRules returns default validation rules
func getDefaultValidationRules() map[string]ValidationRule {
	return map[string]ValidationRule{
		"confidence_threshold": {
			Field:    "confidence_threshold",
			Required: true,
			Type:     "float",
			MinValue: &[]float64{0.0}[0],
			MaxValue: &[]float64{1.0}[0],
		},
		"max_concurrent_rules": {
			Field:    "max_concurrent_rules", 
			Required: true,
			Type:     "int",
			MinValue: &[]float64{1.0}[0],
			MaxValue: &[]float64{100.0}[0],
		},
	}
}

// ExportConfiguration exports configuration to different formats
func (ocm *OptimizationConfigManager) ExportConfiguration(config *OptimizationConfig, format string) ([]byte, error) {
	switch strings.ToLower(format) {
	case "json":
		return json.MarshalIndent(config, "", "  ")
	case "yaml", "yml":
		return yaml.Marshal(config)
	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}

// GetLoadedConfigurations returns all currently loaded configurations
func (ocm *OptimizationConfigManager) GetLoadedConfigurations() map[string]*OptimizationConfig {
	ocm.RLock()
	defer ocm.RUnlock()
	
	// Return a copy to prevent external modification
	result := make(map[string]*OptimizationConfig)
	for k, v := range ocm.loadedConfigs {
		result[k] = v
	}
	return result
}

// ClearCache clears the configuration cache
func (ocm *OptimizationConfigManager) ClearCache() {
	ocm.Lock()
	defer ocm.Unlock()
	
	ocm.loadedConfigs = make(map[string]*OptimizationConfig)
	glog.V(1).Infof("Configuration cache cleared")
}

// ValidateConfigurationFile validates a configuration file without loading it
func (ocm *OptimizationConfigManager) ValidateConfigurationFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	
	config := &OptimizationConfig{}
	ext := strings.ToLower(filepath.Ext(filePath))
	
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, config); err != nil {
			return fmt.Errorf("YAML parsing error: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, config); err != nil {
			return fmt.Errorf("JSON parsing error: %w", err)
		}
	default:
		return fmt.Errorf("unsupported file format: %s", ext)
	}
	
	return ocm.validateConfiguration(config)
}
