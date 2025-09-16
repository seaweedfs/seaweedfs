package protocol

import (
	"testing"
	"time"
)

func TestMetrics_BasicOperations(t *testing.T) {
	metrics := NewMetrics()
	
	// Test recording requests
	metrics.RecordRequest(18, 10*time.Millisecond) // ApiVersions
	metrics.RecordRequest(18, 20*time.Millisecond)
	metrics.RecordRequest(3, 5*time.Millisecond)   // Metadata
	
	// Test recording errors
	metrics.RecordError(18, 30*time.Millisecond)
	
	// Test recording connections
	metrics.RecordConnection()
	metrics.RecordConnection()
	metrics.RecordDisconnection()
	
	// Get API metrics
	apiVersionsMetrics := metrics.GetAPIMetrics(18)
	if apiVersionsMetrics.RequestCount != 3 {
		t.Errorf("Expected 3 requests for ApiVersions, got %d", apiVersionsMetrics.RequestCount)
	}
	
	if apiVersionsMetrics.ErrorCount != 1 {
		t.Errorf("Expected 1 error for ApiVersions, got %d", apiVersionsMetrics.ErrorCount)
	}
	
	// Expected average latency: (10.0 + 20.0 + 30.0) / 3.0 = 20ms
	if apiVersionsMetrics.AvgLatencyMs < 19.0 || apiVersionsMetrics.AvgLatencyMs > 21.0 {
		t.Errorf("Expected average latency around 20ms, got %.2f", apiVersionsMetrics.AvgLatencyMs)
	}
	
	metadataMetrics := metrics.GetAPIMetrics(3)
	if metadataMetrics.RequestCount != 1 {
		t.Errorf("Expected 1 request for Metadata, got %d", metadataMetrics.RequestCount)
	}
	
	if metadataMetrics.ErrorCount != 0 {
		t.Errorf("Expected 0 errors for Metadata, got %d", metadataMetrics.ErrorCount)
	}
	
	// Test connection metrics
	connMetrics := metrics.GetConnectionMetrics()
	if connMetrics.ActiveConnections != 1 {
		t.Errorf("Expected 1 active connection, got %d", connMetrics.ActiveConnections)
	}
	
	if connMetrics.TotalConnections != 2 {
		t.Errorf("Expected 2 total connections, got %d", connMetrics.TotalConnections)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	metrics := NewMetrics()
	
	// Record some test data
	metrics.RecordRequest(18, 15*time.Millisecond)
	metrics.RecordRequest(3, 25*time.Millisecond)
	metrics.RecordError(1, 50*time.Millisecond)
	metrics.RecordConnection()
	
	snapshot := metrics.GetSnapshot()
	
	// Verify snapshot structure
	if len(snapshot.APIs) == 0 {
		t.Error("Expected APIs in snapshot")
	}
	
	if snapshot.Connections.TotalConnections != 1 {
		t.Errorf("Expected 1 total connection in snapshot, got %d", snapshot.Connections.TotalConnections)
	}
	
	if snapshot.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp in snapshot")
	}
	
	// Find ApiVersions in snapshot
	var apiVersionsFound bool
	for _, api := range snapshot.APIs {
		if api.APIKey == 18 {
			apiVersionsFound = true
			if api.APIName != "ApiVersions" {
				t.Errorf("Expected API name 'ApiVersions', got '%s'", api.APIName)
			}
			if api.RequestCount != 1 {
				t.Errorf("Expected 1 request, got %d", api.RequestCount)
			}
			break
		}
	}
	
	if !apiVersionsFound {
		t.Error("ApiVersions not found in snapshot")
	}
}

func TestMetrics_ConcurrentAccess(t *testing.T) {
	metrics := NewMetrics()
	
	// Test concurrent access
	done := make(chan bool, 10)
	
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				metrics.RecordRequest(18, time.Millisecond)
				metrics.RecordConnection()
				metrics.RecordDisconnection()
			}
			done <- true
		}()
	}
	
	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
	
	apiMetrics := metrics.GetAPIMetrics(18)
	if apiMetrics.RequestCount != 1000 {
		t.Errorf("Expected 1000 requests, got %d", apiMetrics.RequestCount)
	}
	
	connMetrics := metrics.GetConnectionMetrics()
	if connMetrics.ActiveConnections != 0 {
		t.Errorf("Expected 0 active connections, got %d", connMetrics.ActiveConnections)
	}
	
	if connMetrics.TotalConnections != 1000 {
		t.Errorf("Expected 1000 total connections, got %d", connMetrics.TotalConnections)
	}
}

func TestMetrics_Reset(t *testing.T) {
	metrics := NewMetrics()
	
	// Record some data
	metrics.RecordRequest(18, 10*time.Millisecond)
	metrics.RecordError(3, 20*time.Millisecond)
	metrics.RecordConnection()
	
	// Verify data exists
	apiMetrics := metrics.GetAPIMetrics(18)
	if apiMetrics.RequestCount == 0 {
		t.Error("Expected non-zero request count before reset")
	}
	
	// Reset metrics
	metrics.Reset()
	
	// Verify data is cleared
	apiMetrics = metrics.GetAPIMetrics(18)
	if apiMetrics.RequestCount != 0 {
		t.Errorf("Expected 0 requests after reset, got %d", apiMetrics.RequestCount)
	}
	
	connMetrics := metrics.GetConnectionMetrics()
	if connMetrics.ActiveConnections != 0 {
		t.Errorf("Expected 0 active connections after reset, got %d", connMetrics.ActiveConnections)
	}
	
	if connMetrics.TotalConnections != 0 {
		t.Errorf("Expected 0 total connections after reset, got %d", connMetrics.TotalConnections)
	}
}

func TestMetrics_GlobalInstance(t *testing.T) {
	// Reset global metrics for clean test
	globalMetrics.Reset()
	
	// Test global convenience functions
	RecordRequestMetrics(18, 5*time.Millisecond)
	RecordErrorMetrics(3, 10*time.Millisecond)
	RecordConnectionMetrics()
	RecordDisconnectionMetrics()
	
	// Verify through global instance
	global := GetGlobalMetrics()
	
	apiMetrics := global.GetAPIMetrics(18)
	if apiMetrics.RequestCount != 1 {
		t.Errorf("Expected 1 request in global metrics, got %d", apiMetrics.RequestCount)
	}
	
	errorMetrics := global.GetAPIMetrics(3)
	if errorMetrics.ErrorCount != 1 {
		t.Errorf("Expected 1 error in global metrics, got %d", errorMetrics.ErrorCount)
	}
	
	connMetrics := global.GetConnectionMetrics()
	if connMetrics.ActiveConnections != 0 {
		t.Errorf("Expected 0 active connections, got %d", connMetrics.ActiveConnections)
	}
	
	if connMetrics.TotalConnections != 1 {
		t.Errorf("Expected 1 total connection, got %d", connMetrics.TotalConnections)
	}
}

func TestMetrics_LatencyCalculation(t *testing.T) {
	metrics := NewMetrics()
	
	// Test various latencies
	latencies := []time.Duration{
		1 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
	}
	
	for _, latency := range latencies {
		metrics.RecordRequest(18, latency)
	}
	
	apiMetrics := metrics.GetAPIMetrics(18)
	// Expected average: (1.0 + 5.0 + 10.0 + 20.0) / 4.0 = 9ms
	
	if apiMetrics.AvgLatencyMs < 8.5 || apiMetrics.AvgLatencyMs > 9.5 {
		t.Errorf("Expected average latency around 9ms, got %.2f", apiMetrics.AvgLatencyMs)
	}
	
	if apiMetrics.RequestCount != 4 {
		t.Errorf("Expected 4 requests, got %d", apiMetrics.RequestCount)
	}
}

func TestMetrics_APINames(t *testing.T) {
	metrics := NewMetrics()
	
	// Test various API keys
	testCases := []struct {
		apiKey   uint16
		expected string
	}{
		{18, "ApiVersions"},
		{3, "Metadata"},
		{0, "Produce"},
		{1, "Fetch"},
		{15, "DescribeGroups"},
		{16, "ListGroups"},
		{999, "Unknown"},
	}
	
	for _, tc := range testCases {
		metrics.RecordRequest(tc.apiKey, time.Millisecond)
		apiMetrics := metrics.GetAPIMetrics(tc.apiKey)
		
		if apiMetrics.APIName != tc.expected {
			t.Errorf("Expected API name '%s' for key %d, got '%s'", 
				tc.expected, tc.apiKey, apiMetrics.APIName)
		}
	}
}
