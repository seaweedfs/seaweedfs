package ml

import (
	"testing"
)

// TestOptimizationEngine_Basic tests the basic functionality of the optimization engine
func TestOptimizationEngine_Basic(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	if engine == nil {
		t.Fatal("Should create optimization engine")
	}

	if !engine.enabled {
		t.Error("Engine should be enabled")
	}

	// Check that default rules and strategies are loaded
	if len(engine.rules) == 0 {
		t.Error("Should have default rules loaded")
	}

	if len(engine.strategies) == 0 {
		t.Error("Should have default strategies loaded")
	}

	t.Logf("Engine initialized with %d rules, %d strategies", len(engine.rules), len(engine.strategies))
}

// TestOptimizationEngine_RuleEvaluation tests rule evaluation
func TestOptimizationEngine_RuleEvaluation(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	// Create test context for sequential access of a large model file
	context := &OptimizationContext{
		FilePath:        "/models/large_model.pth",
		FileSize:        2 * 1024 * 1024 * 1024, // 2GB
		FileType:        "model",
		AccessPattern:   SequentialAccess,
		AccessFrequency: 10,
		Framework:       "pytorch",
		WorkloadType:    "training",
	}

	// Apply optimizations
	result := engine.OptimizeAccess(context)

	if result == nil {
		t.Fatal("Should return optimization result")
	}

	if !result.Applied {
		t.Error("Should apply optimizations for large model file with sequential access")
	}

	if result.Confidence < 0.5 {
		t.Errorf("Expected confidence >= 0.5, got %.2f", result.Confidence)
	}

	if len(result.Optimizations) == 0 {
		t.Error("Should have applied optimizations")
	}

	t.Logf("Applied %d optimizations with confidence %.2f",
		len(result.Optimizations), result.Confidence)

	for i, opt := range result.Optimizations {
		t.Logf("Optimization %d: type=%s, target=%s", i+1, opt.Type, opt.Target)
	}
}

// TestOptimizationEngine_FrameworkDetection tests framework detection
func TestOptimizationEngine_FrameworkDetection(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	testCases := []struct {
		filePath          string
		expectedFramework string
	}{
		{"/models/model.pth", "pytorch"},
		{"/models/model.pt", "pytorch"},
		{"/models/saved_model.pb", "tensorflow"},
		{"/models/model.h5", "tensorflow"},
		{"/models/checkpoint.ckpt", "tensorflow"},
		{"/data/dataset.tfrecord", "tensorflow"},
		{"/unknown/file.bin", ""},
	}

	for _, tc := range testCases {
		framework := engine.detectFramework(tc.filePath, nil)

		if tc.expectedFramework == "" {
			if framework != "" {
				t.Errorf("File %s: expected no framework detection, got %s", tc.filePath, framework)
			}
		} else {
			if framework != tc.expectedFramework {
				t.Errorf("File %s: expected framework %s, got %s",
					tc.filePath, tc.expectedFramework, framework)
			}
		}
	}
}

// TestOptimizationEngine_FileTypeDetection tests file type detection
func TestOptimizationEngine_FileTypeDetection(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	testCases := []struct {
		filePath     string
		expectedType string
	}{
		{"/models/model.pth", "model"},
		{"/data/dataset.csv", "dataset"},
		{"/configs/config.yaml", "config"},
		{"/logs/training.log", "log"},
		{"/unknown/file.bin", "unknown"},
	}

	for _, tc := range testCases {
		fileType := engine.detectFileType(tc.filePath)

		if fileType != tc.expectedType {
			t.Errorf("File %s: expected type %s, got %s",
				tc.filePath, tc.expectedType, fileType)
		}
	}
}

// TestOptimizationEngine_ConditionEvaluation tests condition evaluation
func TestOptimizationEngine_ConditionEvaluation(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	context := &OptimizationContext{
		FilePath:      "/models/test.pth",
		FileSize:      5 * 1024 * 1024, // 5MB
		FileType:      "model",
		AccessPattern: SequentialAccess,
		Framework:     "pytorch",
	}

	// Test various condition types
	testConditions := []struct {
		condition RuleCondition
		expected  bool
	}{
		{
			condition: RuleCondition{
				Type:     "file_pattern",
				Property: "extension",
				Operator: "equals",
				Value:    ".pth",
			},
			expected: true,
		},
		{
			condition: RuleCondition{
				Type:     "file_context",
				Property: "size",
				Operator: "greater_than",
				Value:    1024 * 1024, // 1MB
			},
			expected: true,
		},
		{
			condition: RuleCondition{
				Type:     "access_pattern",
				Property: "pattern_type",
				Operator: "equals",
				Value:    "sequential",
			},
			expected: true,
		},
		{
			condition: RuleCondition{
				Type:     "workload_context",
				Property: "framework",
				Operator: "equals",
				Value:    "tensorflow",
			},
			expected: false,
		},
	}

	for i, tc := range testConditions {
		result := engine.evaluateCondition(tc.condition, context)
		if result != tc.expected {
			t.Errorf("Condition %d: expected %v, got %v", i+1, tc.expected, result)
		}
	}
}

// TestOptimizationEngine_PluginSystem tests the plugin system
func TestOptimizationEngine_PluginSystem(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	// Register a test plugin
	plugin := NewPyTorchPlugin()
	err := engine.RegisterPlugin(plugin)
	if err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Verify plugin is registered
	if _, exists := engine.plugins["pytorch"]; !exists {
		t.Error("PyTorch plugin should be registered")
	}

	// Test framework detection through plugin
	confidence := plugin.DetectFramework("/models/test.pth", nil)
	if confidence < 0.5 {
		t.Errorf("Expected high confidence for .pth file, got %.2f", confidence)
	}

	// Test optimization hints
	context := &OptimizationContext{
		FilePath:  "/models/test.pth",
		FileSize:  100 * 1024 * 1024, // 100MB
		FileType:  "model",
		Framework: "pytorch",
	}

	hints := plugin.GetOptimizationHints(context)
	if len(hints) == 0 {
		t.Error("Plugin should provide optimization hints")
	}

	t.Logf("Plugin provided %d optimization hints", len(hints))
}

// TestOptimizationEngine_UsagePatterns tests usage pattern learning
func TestOptimizationEngine_UsagePatterns(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	context := &OptimizationContext{
		FilePath:      "/models/training_model.pth",
		FileSize:      50 * 1024 * 1024, // 50MB
		FileType:      "model",
		AccessPattern: SequentialAccess,
		Framework:     "pytorch",
		WorkloadType:  "training",
	}

	// Apply optimization multiple times to build usage patterns
	for i := 0; i < 5; i++ {
		result := engine.OptimizeAccess(context)
		if result == nil {
			t.Fatalf("Optimization %d failed", i+1)
		}
	}

	// Check that usage patterns are being tracked
	if len(engine.usagePatterns) == 0 {
		t.Error("Should have learned usage patterns")
	}

	// Verify pattern characteristics
	for patternKey, pattern := range engine.usagePatterns {
		t.Logf("Learned pattern: %s (frequency=%d, success_rate=%.2f)",
			patternKey, pattern.Frequency, pattern.SuccessRate)

		if pattern.Frequency < 1 {
			t.Errorf("Pattern %s should have frequency >= 1", patternKey)
		}
	}
}

// TestOptimizationEngine_Metrics tests metrics collection
func TestOptimizationEngine_Metrics(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	metrics := engine.GetMetrics()

	if metrics == nil {
		t.Fatal("Should return metrics")
	}

	expectedKeys := []string{"enabled", "rules_count", "templates_count", "strategies_count"}

	for _, key := range expectedKeys {
		if _, exists := metrics[key]; !exists {
			t.Errorf("Metrics should contain key: %s", key)
		}
	}

	if metrics["enabled"] != true {
		t.Error("Metrics should show engine as enabled")
	}

	t.Logf("Engine metrics: %+v", metrics)
}

// TestOptimizationEngine_ConfigurationDriven tests configuration-driven optimization
func TestOptimizationEngine_ConfigurationDriven(t *testing.T) {
	engine := NewOptimizationEngine(true)
	defer engine.Shutdown()

	// Test that the engine can apply optimizations based on its loaded configuration
	context := &OptimizationContext{
		FilePath:      "/data/dataset.csv",
		FileSize:      10 * 1024 * 1024, // 10MB
		FileType:      "dataset",
		AccessPattern: SequentialAccess,
		Framework:     "",
		WorkloadType:  "training",
		BatchSize:     32,
	}

	result := engine.OptimizeAccess(context)

	if result == nil {
		t.Fatal("Should return optimization result")
	}

	// The engine should make intelligent decisions based on context
	if result.Applied && len(result.Optimizations) > 0 {
		t.Logf("Successfully applied %d optimizations", len(result.Optimizations))

		for _, opt := range result.Optimizations {
			if opt.Type == "" || opt.Target == "" {
				t.Error("Optimization should have valid type and target")
			}
		}
	}

	if len(result.Recommendations) > 0 {
		t.Logf("Generated %d recommendations", len(result.Recommendations))
		for _, rec := range result.Recommendations {
			t.Logf("Recommendation: %s", rec)
		}
	}
}

// TestOptimizationEngine_Shutdown tests proper shutdown
func TestOptimizationEngine_Shutdown(t *testing.T) {
	engine := NewOptimizationEngine(true)

	if !engine.enabled {
		t.Error("Engine should start enabled")
	}

	engine.Shutdown()

	if engine.enabled {
		t.Error("Engine should be disabled after shutdown")
	}

	// Test that optimization doesn't work after shutdown
	context := &OptimizationContext{
		FilePath: "/test.pth",
		FileSize: 1024,
	}

	result := engine.OptimizeAccess(context)
	if result.Applied {
		t.Error("Should not apply optimizations after shutdown")
	}
}
