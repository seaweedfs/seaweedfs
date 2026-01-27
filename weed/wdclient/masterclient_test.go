package wdclient

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
)

// TestWaitUntilConnectedWithoutKeepConnected verifies that WaitUntilConnected
// respects context cancellation when KeepConnectedToMaster is not running.
// This tests the fix for https://github.com/seaweedfs/seaweedfs/issues/7721
func TestWaitUntilConnectedWithoutKeepConnected(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	// Without KeepConnectedToMaster running, WaitUntilConnected should
	// respect context cancellation and not block forever
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	mc.WaitUntilConnected(ctx)
	elapsed := time.Since(start)

	// Should have returned due to context timeout, not blocked forever
	if elapsed > 200*time.Millisecond {
		t.Errorf("WaitUntilConnected blocked for %v, expected to return on context timeout", elapsed)
	}

	// GetMaster should return empty since we never connected
	master := mc.getCurrentMaster()
	if master != "" {
		t.Errorf("Expected empty master, got %s", master)
	}
}

// TestWaitUntilConnectedReturnsImmediatelyWhenConnected verifies that
// WaitUntilConnected returns immediately when a master is already set.
func TestWaitUntilConnectedReturnsImmediatelyWhenConnected(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	// Simulate that KeepConnectedToMaster has already established a connection
	mc.setCurrentMaster("localhost:9333")

	ctx := context.Background()
	start := time.Now()
	mc.WaitUntilConnected(ctx)
	elapsed := time.Since(start)

	// Should return almost immediately (< 10ms)
	if elapsed > 10*time.Millisecond {
		t.Errorf("WaitUntilConnected took %v when master was already set, expected immediate return", elapsed)
	}

	// Verify master is returned
	master := mc.getCurrentMaster()
	if master != "localhost:9333" {
		t.Errorf("Expected master localhost:9333, got %s", master)
	}
}

// TestGetMasterRespectsContextCancellation verifies that GetMaster
// respects context cancellation and doesn't block forever.
func TestGetMasterRespectsContextCancellation(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	// GetMaster calls WaitUntilConnected internally
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	master := mc.GetMaster(ctx)
	elapsed := time.Since(start)

	// Should return on context timeout
	if elapsed > 200*time.Millisecond {
		t.Errorf("GetMaster blocked for %v, expected to return on context timeout", elapsed)
	}

	// Master should be empty since we never connected
	if master != "" {
		t.Errorf("Expected empty master when not connected, got %s", master)
	}
}

// TestMasterClientFilerGroupLogging verifies the FilerGroup is properly set
// and would be logged correctly (regression test for issue #7721 log message format)
func TestMasterClientFilerGroupLogging(t *testing.T) {
	filerGroup := "filer_1"
	clientType := "s3"

	mc := NewMasterClient(grpc.EmptyDialOption{}, filerGroup, clientType, "", "", "", pb.ServerDiscovery{})

	if mc.FilerGroup != filerGroup {
		t.Errorf("Expected FilerGroup %s, got %s", filerGroup, mc.FilerGroup)
	}

	if mc.clientType != clientType {
		t.Errorf("Expected clientType %s, got %s", clientType, mc.clientType)
	}
}
