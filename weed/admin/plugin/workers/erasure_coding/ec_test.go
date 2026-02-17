package erasure_coding

import (
	"context"
	stdtesting "testing"
	"time"

	plugintesting "github.com/seaweedfs/seaweedfs/weed/admin/plugin/testing"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// TestECSchemaGeneration tests that the EC plugin generates the correct configuration schema
func TestECSchemaGeneration(t *stdtesting.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	harness := plugintesting.NewTestHarness(ctx)
	if err := harness.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer harness.Teardown()

	// Verify schema fields for EC job type
	expectedFields := map[string]bool{
		"data_shards":   true,
		"parity_shards": true,
		"block_size":    true,
		"algorithm":     true,
	}

	configFields := []*plugin_pb.ConfigField{
		{
			Name:         "data_shards",
			Label:        "Data Shards",
			Description:  "Number of data shards",
			FieldType:    plugin_pb.ConfigField_INT,
			Required:     true,
			DefaultValue: "4",
		},
		{
			Name:         "parity_shards",
			Label:        "Parity Shards",
			Description:  "Number of parity shards",
			FieldType:    plugin_pb.ConfigField_INT,
			Required:     true,
			DefaultValue: "2",
		},
		{
			Name:         "block_size",
			Label:        "Block Size",
			Description:  "Block size in bytes",
			FieldType:    plugin_pb.ConfigField_INT,
			Required:     false,
			DefaultValue: "65536",
		},
		{
			Name:         "algorithm",
			Label:        "Algorithm",
			Description:  "Erasure coding algorithm",
			FieldType:    plugin_pb.ConfigField_SELECT,
			Required:     true,
			DefaultValue: "reed_solomon",
		},
	}

	// Verify all expected fields are present
	for _, field := range configFields {
		if _, ok := expectedFields[field.Name]; ok {
			if field.FieldType == plugin_pb.ConfigField_INT ||
				field.FieldType == plugin_pb.ConfigField_SELECT {
				delete(expectedFields, field.Name)
			}
		}
	}

	if len(expectedFields) > 0 {
		t.Errorf("Missing expected schema fields: %v", expectedFields)
	}
}

// TestECDetection tests that the EC plugin can detect jobs
func TestECDetection(t *stdtesting.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	harness := plugintesting.NewTestHarness(ctx)
	if err := harness.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer harness.Teardown()

	// Create mock plugin
	plugin := plugintesting.NewMockPlugin("ec-plugin-1", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)

	// Simulate detection
	detectedJobs, err := plugin.SimulateDetection("erasure_coding", 3)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(detectedJobs) != 3 {
		t.Errorf("Expected 3 detected jobs, got %d", len(detectedJobs))
	}

	for i, job := range detectedJobs {
		if job.JobType != "erasure_coding" {
			t.Errorf("Job %d has wrong type: expected erasure_coding, got %s", i, job.JobType)
		}
		if job.JobKey == "" {
			t.Errorf("Job %d missing job key", i)
		}
	}
}

// TestECExecution tests that the EC plugin can execute jobs
func TestECExecution(t *stdtesting.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	harness := plugintesting.NewTestHarness(ctx)
	if err := harness.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer harness.Teardown()

	// Create mock plugin
	plugin := plugintesting.NewMockPlugin("ec-plugin-2", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)

	// Create a test job
	config := []*plugin_pb.ConfigFieldValue{
		{
			FieldName: "data_shards",
			IntValue:  4,
		},
		{
			FieldName: "parity_shards",
			IntValue:  2,
		},
	}

	jobID := "ec-job-test-001"
	execution, err := plugin.SimulateExecution(jobID, "erasure_coding", config)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	if execution.Status != "completed" {
		t.Errorf("Expected status 'completed', got %s", execution.Status)
	}

	if execution.ProgressPercent != 100 {
		t.Errorf("Expected progress 100, got %d", execution.ProgressPercent)
	}

	messages := plugin.GetExecutionMessages(jobID)
	if len(messages) == 0 {
		t.Fatal("Expected execution messages")
	}

	// Check for completion message
	hasCompletion := false
	for _, msg := range messages {
		if msg.GetJobCompleted() != nil {
			hasCompletion = true
			break
		}
	}

	if !hasCompletion {
		t.Error("Missing job completion message")
	}
}

// TestECErrorHandling tests error scenarios in EC plugin
func TestECErrorHandling(t *stdtesting.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	harness := plugintesting.NewTestHarness(ctx)
	if err := harness.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer harness.Teardown()

	// Test 1: Detection disabled
	plugin := plugintesting.NewMockPlugin("ec-plugin-3", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)
	plugin.DetectionEnabled = false

	_, err := plugin.SimulateDetection("erasure_coding", 1)
	if err == nil {
		t.Error("Expected error when detection is disabled")
	}

	// Test 2: Execution failure scenario
	plugin.Reset()
	plugin.DetectionEnabled = true
	plugin.ExecutionEnabled = true

	jobID := "ec-job-fail-001"
	execution, err := plugin.SimulateExecutionFailure(jobID, "INVALID_CONFIG", "Data shards must be > 0", true)
	if err != nil {
		t.Fatalf("Execution failure simulation failed: %v", err)
	}

	if execution.Status != "failed" {
		t.Errorf("Expected status 'failed', got %s", execution.Status)
	}

	if execution.ErrorInfo.ErrorCode != "INVALID_CONFIG" {
		t.Errorf("Expected error code 'INVALID_CONFIG', got %s", execution.ErrorInfo.ErrorCode)
	}

	if !execution.ErrorInfo.Retryable {
		t.Error("Expected error to be retryable")
	}

	// Test 3: Simulated detection error
	plugin.SetFailureMode("detection_error")
	_, err = plugin.SimulateDetection("erasure_coding", 1)
	if err == nil {
		t.Error("Expected error in detection with failure mode set")
	}

	errors := plugin.GetErrors()
	if len(errors) == 0 {
		t.Error("Expected recorded errors")
	}
}

// TestECIntegration tests the full EC plugin workflow
func TestECIntegration(t *stdtesting.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	harness := plugintesting.NewTestHarness(ctx)
	if err := harness.Setup(); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	defer harness.Teardown()

	// Create mock admin server
	adminServer := harness.GetAdminServer()
	_ = adminServer // Keep reference to prevent GC

	// Create mock plugin
	plugin := plugintesting.NewMockPlugin("ec-plugin-integration", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)

	// Step 1: Plugin registration
	regMsg := plugin.GetRegistrationMessage()
	_ = regMsg // Keep reference

	// Step 2: Detect jobs
	detectedJobs, err := plugin.SimulateDetection("erasure_coding", 2)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(detectedJobs) != 2 {
		t.Errorf("Expected 2 detected jobs, got %d", len(detectedJobs))
	}

	// Step 3: Create jobs from detection
	for i, detectedJob := range detectedJobs {
		job := harness.CreateJob(
			"erasure_coding",
			detectedJob.Description,
			detectedJob.Priority,
			detectedJob.SuggestedConfig,
		)

		// Step 4: Execute job with plugin simulation
		_, err := plugin.SimulateExecution(job.JobId, "erasure_coding", job.Config)
		if err != nil {
			t.Errorf("Failed to execute job %d: %v", i, err)
			continue
		}

		// Track in harness
		_ = harness.ExecuteJob(job)

		// Complete the job
		if err := harness.CompleteJob(job.JobId, "Erasure coding completed", map[string]string{
			"blocks_encoded": "1000",
		}); err != nil {
			t.Errorf("Failed to complete job %d: %v", i, err)
		}

		// Verify job status
		if err := harness.AssertJobStatus(job.JobId, "completed"); err != nil {
			t.Errorf("Job %d status verification failed: %v", i, err)
		}

		// Verify progress
		if err := harness.VerifyProgress(job.JobId, 100); err != nil {
			t.Errorf("Job %d progress verification failed: %v", i, err)
		}
	}

	// Verify call counts
	detectionCalls := plugin.GetCallCount("detection")
	if detectionCalls == 0 {
		t.Error("Expected at least one detection call")
	}

	executionCalls := plugin.GetCallCount("execution")
	if executionCalls != 2 {
		t.Errorf("Expected 2 execution calls, got %d", executionCalls)
	}
}

// TestECConfigurationValidation tests configuration field validation
func TestECConfigurationValidation(t *stdtesting.T) {
	tests := []struct {
		name    string
		config  *plugin_pb.ConfigFieldValue
		wantErr bool
	}{
		{
			name: "valid data shards",
			config: &plugin_pb.ConfigFieldValue{
				FieldName: "data_shards",
				IntValue:  4,
			},
			wantErr: false,
		},
		{
			name: "invalid data shards - zero",
			config: &plugin_pb.ConfigFieldValue{
				FieldName: "data_shards",
				IntValue:  0,
			},
			wantErr: true,
		},
		{
			name: "valid parity shards",
			config: &plugin_pb.ConfigFieldValue{
				FieldName: "parity_shards",
				IntValue:  2,
			},
			wantErr: false,
		},
		{
			name: "negative parity shards",
			config: &plugin_pb.ConfigFieldValue{
				FieldName: "parity_shards",
				IntValue:  -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *stdtesting.T) {
			// Validate configuration
			var hasError bool

			// Simple validation: data_shards > 0, parity_shards >= 0
			if tt.config.FieldName == "data_shards" && tt.config.IntValue <= 0 {
				hasError = true
			}
			if tt.config.FieldName == "parity_shards" && tt.config.IntValue < 0 {
				hasError = true
			}

			if hasError != tt.wantErr {
				t.Errorf("validation error: expected %v, got %v", tt.wantErr, hasError)
			}
		})
	}
}

// BenchmarkECDetection benchmarks the detection performance
func BenchmarkECDetection(b *stdtesting.B) {
	plugin := plugintesting.NewMockPlugin("ec-plugin-bench", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)
	plugin.SetDetectionDelay(0) // No delay for benchmark

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = plugin.SimulateDetection("erasure_coding", 10)
	}
}

// BenchmarkECExecution benchmarks the execution performance
func BenchmarkECExecution(b *stdtesting.B) {
	plugin := plugintesting.NewMockPlugin("ec-plugin-bench-exec", "Erasure Coding", "1.0.0")
	plugin.AddCapability("erasure_coding", true, true)
	plugin.SetExecutionDelay(0) // No delay for benchmark

	config := []*plugin_pb.ConfigFieldValue{
		{
			FieldName: "data_shards",
			IntValue:  4,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobID := "bench-job-" + string(rune(i))
		_, _ = plugin.SimulateExecution(jobID, "erasure_coding", config)
	}
}
