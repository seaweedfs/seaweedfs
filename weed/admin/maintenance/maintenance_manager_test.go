package maintenance

import (
	"errors"
	"testing"
	"time"
)

func TestMaintenanceManager_ErrorHandling(t *testing.T) {
	config := DefaultMaintenanceConfig()
	config.ScanIntervalSeconds = 1 // Short interval for testing (1 second)

	manager := NewMaintenanceManager(nil, config)

	// Test initial state
	if manager.errorCount != 0 {
		t.Errorf("Expected initial error count to be 0, got %d", manager.errorCount)
	}

	if manager.backoffDelay != time.Second {
		t.Errorf("Expected initial backoff delay to be 1s, got %v", manager.backoffDelay)
	}

	// Test error handling
	err := errors.New("dial tcp [::1]:19333: connect: connection refused")
	manager.handleScanError(err)

	if manager.errorCount != 1 {
		t.Errorf("Expected error count to be 1, got %d", manager.errorCount)
	}

	if manager.lastError != err {
		t.Errorf("Expected last error to be set")
	}

	// Test exponential backoff
	initialDelay := manager.backoffDelay
	manager.handleScanError(err)

	if manager.backoffDelay != initialDelay*2 {
		t.Errorf("Expected backoff delay to double, got %v", manager.backoffDelay)
	}

	if manager.errorCount != 2 {
		t.Errorf("Expected error count to be 2, got %d", manager.errorCount)
	}

	// Test backoff cap
	for i := 0; i < 10; i++ {
		manager.handleScanError(err)
	}

	if manager.backoffDelay > 5*time.Minute {
		t.Errorf("Expected backoff delay to be capped at 5 minutes, got %v", manager.backoffDelay)
	}

	// Test error reset
	manager.resetErrorTracking()

	if manager.errorCount != 0 {
		t.Errorf("Expected error count to be reset to 0, got %d", manager.errorCount)
	}

	if manager.backoffDelay != time.Second {
		t.Errorf("Expected backoff delay to be reset to 1s, got %v", manager.backoffDelay)
	}

	if manager.lastError != nil {
		t.Errorf("Expected last error to be reset to nil")
	}
}

func TestIsConnectionError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{errors.New("connection refused"), true},
		{errors.New("dial tcp [::1]:19333: connect: connection refused"), true},
		{errors.New("connection error: desc = \"transport: Error while dialing\""), true},
		{errors.New("connection timeout"), true},
		{errors.New("no route to host"), true},
		{errors.New("network unreachable"), true},
		{errors.New("some other error"), false},
		{errors.New("invalid argument"), false},
	}

	for _, test := range tests {
		result := isConnectionError(test.err)
		if result != test.expected {
			t.Errorf("For error %v, expected %v, got %v", test.err, test.expected, result)
		}
	}
}

func TestMaintenanceManager_GetErrorState(t *testing.T) {
	config := DefaultMaintenanceConfig()
	manager := NewMaintenanceManager(nil, config)

	// Test initial state
	errorCount, lastError, backoffDelay := manager.GetErrorState()
	if errorCount != 0 || lastError != nil || backoffDelay != time.Second {
		t.Errorf("Expected initial state to be clean")
	}

	// Add some errors
	err := errors.New("test error")
	manager.handleScanError(err)
	manager.handleScanError(err)

	errorCount, lastError, backoffDelay = manager.GetErrorState()
	if errorCount != 2 || lastError != err || backoffDelay != 2*time.Second {
		t.Errorf("Expected error state to be tracked correctly: count=%d, err=%v, delay=%v",
			errorCount, lastError, backoffDelay)
	}
}

func TestMaintenanceManager_LogThrottling(t *testing.T) {
	config := DefaultMaintenanceConfig()
	manager := NewMaintenanceManager(nil, config)

	// This is a basic test to ensure the error handling doesn't panic
	// In practice, you'd want to capture log output to verify throttling
	err := errors.New("test error")

	// Generate many errors to test throttling
	for i := 0; i < 25; i++ {
		manager.handleScanError(err)
	}

	// Should not panic and should have capped backoff
	if manager.backoffDelay > 5*time.Minute {
		t.Errorf("Expected backoff to be capped at 5 minutes")
	}

	if manager.errorCount != 25 {
		t.Errorf("Expected error count to be 25, got %d", manager.errorCount)
	}
}
