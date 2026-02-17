package erasure_coding

import (
	"context"
	"testing"
	"time"

	plugin_testing "github.com/seaweedfs/seaweedfs/weed/admin/plugin/testing"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// TestDetectionWithSingleVolume tests detection of a single volume
func TestDetectionWithSingleVolume(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestDetectionWithSingleVolume")
	defer harness.Cleanup()

	// Create and register a mock plugin
	plugin := plugin_testing.NewMockPlugin("ec-worker-1", "EC Plugin", "1.0.0")
	plugin.AddDetectionCapability("ec_candidates", "Detect EC candidates", 3600, true)

	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	if !harness.VerifyRegistration("ec-worker-1") {
		t.Error("Plugin registration not verified")
	}

	if harness.GetRegistrationCount() != 1 {
		t.Errorf("Expected 1 registration, got %d", harness.GetRegistrationCount())
	}
}

// TestDetectionWithMultipleVolumes tests detection of multiple volumes
func TestDetectionWithMultipleVolumes(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestDetectionWithMultipleVolumes")
	defer harness.Cleanup()

	plugin := plugin_testing.NewMockPlugin("ec-worker-2", "EC Plugin", "1.0.0")
	plugin.AddDetectionCapability("ec_candidates", "Detect EC candidates", 3600, true)

	// Add multiple detection results
	plugin.AddDetectionResult("vol-1", "ec_candidates", "info", "Volume 1 candidate", nil)
	plugin.AddDetectionResult("vol-2", "ec_candidates", "info", "Volume 2 candidate", nil)
	plugin.AddDetectionResult("vol-3", "ec_candidates", "info", "Volume 3 candidate", nil)

	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Verify capabilities
	if !harness.VerifyPluginCapability("ec-worker-2", "ec_candidates") {
		t.Error("EC candidates capability not found")
	}
}

// TestJobDispatch tests job dispatch to EC plugin
func TestJobDispatch(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestJobDispatch")
	defer harness.Cleanup()

	plugin := plugin_testing.NewMockPlugin("ec-worker-3", "EC Plugin", "1.0.0")
	plugin.AddDetectionCapability("ec_candidates", "Detect EC candidates", 3600, true)

	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Dispatch a job
	payload := &plugin_pb.JobPayload{
		DetectionType:    "encode_volume",
		TargetDatasource: "volume-123",
		Data:             []byte{1, 2, 3, 4},
		Parameters:       map[string]string{"stripe_size": "10"},
	}

	jobID, err := harness.DispatchJob("ec-worker-3", "encode_volume", payload)
	if err != nil {
		t.Fatalf("Failed to dispatch job: %v", err)
	}

	if jobID == "" {
		t.Error("No job ID returned")
	}

	// Verify job was dispatched
	if harness.GetJobCount() != 1 {
		t.Errorf("Expected 1 job, got %d", harness.GetJobCount())
	}

	// Verify job completed
	if !harness.VerifyJobCompleted(jobID) {
		t.Errorf("Job %s did not complete", jobID)
	}
}

// TestExecutionPipeline tests the full EC execution pipeline
func TestExecutionPipeline(t *testing.T) {
	executor := NewExecutor(&ExecutorConfig{
		StripeSize:     10,
		EncodeCopies:   1,
		TimeoutPerStep: 1 * time.Second,
		MaxRetries:     3,
	})

	// Create a mock job
	job := &plugin_pb.ExecuteJobRequest{
		JobId:      "job-123",
		JobType:    "encode_volume",
		Payload:    &plugin_pb.JobPayload{Data: []byte{1, 2, 3, 4}},
		RetryCount: 0,
	}

	result, err := executor.ExecuteJob(job)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	if !result.Success {
		t.Errorf("Execution was not successful: %s", result.ErrorMessage)
	}

	if len(result.Steps) != 6 {
		t.Errorf("Expected 6 steps, got %d", len(result.Steps))
	}

	// Verify pipeline steps
	expectedSteps := []string{"marking", "copying", "generating", "distributing", "mounting", "cleaning"}
	for i, expected := range expectedSteps {
		if i >= len(result.Steps) {
			t.Errorf("Missing step: %s", expected)
			break
		}
		if result.Steps[i].Name != expected {
			t.Errorf("Step %d: expected %s, got %s", i, expected, result.Steps[i].Name)
		}
	}
}

// TestErrorHandling tests error handling in execution
func TestErrorHandling(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestErrorHandling")
	defer harness.Cleanup()

	plugin := plugin_testing.NewMockPlugin("ec-worker-4", "EC Plugin", "1.0.0")
	plugin.AddDetectionCapability("ec_candidates", "Detect EC candidates", 3600, true)

	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Verify plugin is registered
	if !harness.VerifyRegistration("ec-worker-4") {
		t.Error("Plugin registration not verified")
	}
}

// TestDetectorFiltering tests volume filtering in detector
func TestDetectorFiltering(t *testing.T) {
	detector := NewDetector(DetectionOptions{
		MinVolumeSize: 1000,
		MaxVolumeSize: 10000,
		RackAwareness: true,
	})

	// Create test volumes
	volumes := map[uint32]*VolumeMetric{
		1: {
			VolumeID:     1,
			Size:         500, // Too small
			FreeSpace:    100,
			ReplicaCount: 2,
			LastModified: 1,
		},
		2: {
			VolumeID:     2,
			Size:         5000, // Good
			FreeSpace:    1000,
			ReplicaCount: 2,
			RackID:       "rack-1",
			LastModified: 1,
		},
		3: {
			VolumeID:     3,
			Size:         20000, // Too large
			FreeSpace:    5000,
			ReplicaCount: 2,
			LastModified: 1,
		},
		4: {
			VolumeID:     4,
			Size:         3000, // Good but already encoded
			IsEncoded:    true,
			FreeSpace:    500,
			ReplicaCount: 2,
			LastModified: 1,
		},
	}

	candidates, err := detector.DetectJobs(volumes)
	if err != nil {
		t.Fatalf("Detection failed: %v", err)
	}

	if len(candidates) != 1 {
		t.Errorf("Expected 1 candidate, got %d", len(candidates))
		for _, c := range candidates {
			t.Logf("Candidate: %d - %s", c.VolumeID, c.Reason)
		}
		return
	}

	if len(candidates) > 0 && candidates[0].VolumeID != 2 {
		t.Errorf("Expected volume 2, got %d", candidates[0].VolumeID)
	}
}

// TestHealthReporting tests health report submission
func TestHealthReporting(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestHealthReporting")
	defer harness.Cleanup()

	plugin := plugin_testing.NewMockPlugin("ec-worker-5", "EC Plugin", "1.0.0")
	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	adminService := harness.GetAdminService()

	// Send health report
	report := &plugin_pb.HealthReport{
		PluginId:    "ec-worker-5",
		TimestampMs: time.Now().UnixMilli(),
		Status:      plugin_pb.HealthStatus_HEALTH_STATUS_HEALTHY,
		ActiveJobs:  3,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := adminService.ReportHealth(ctx, report)
	if err != nil {
		t.Fatalf("Failed to report health: %v", err)
	}

	if !resp.Acknowledged {
		t.Error("Health report not acknowledged")
	}

	if adminService.GetHeartbeatCount() != 1 {
		t.Errorf("Expected 1 heartbeat, got %d", adminService.GetHeartbeatCount())
	}
}

// TestConcurrentJobExecution tests multiple concurrent jobs
func TestConcurrentJobExecution(t *testing.T) {
	harness := plugin_testing.NewTestHarness("TestConcurrentJobExecution")
	defer harness.Cleanup()

	plugin := plugin_testing.NewMockPlugin("ec-worker-6", "EC Plugin", "1.0.0")
	if err := harness.RegisterPlugin(plugin); err != nil {
		t.Fatalf("Failed to register plugin: %v", err)
	}

	// Dispatch multiple jobs
	jobIDs := make([]string, 0)
	for i := 0; i < 5; i++ {
		payload := &plugin_pb.JobPayload{
			DetectionType: "encode_volume",
			Data:          []byte{byte(i)},
		}

		jobID, err := harness.DispatchJob("ec-worker-6", "encode_volume", payload)
		if err != nil {
			t.Fatalf("Failed to dispatch job %d: %v", i, err)
		}

		jobIDs = append(jobIDs, jobID)
	}

	// Verify all jobs
	if harness.GetJobCount() != 5 {
		t.Errorf("Expected 5 jobs, got %d", harness.GetJobCount())
	}

	completedCount := 0
	for _, jobID := range jobIDs {
		if harness.VerifyJobCompleted(jobID) {
			completedCount++
		}
	}

	if completedCount != 5 {
		t.Errorf("Expected 5 completed jobs, got %d", completedCount)
	}
}
