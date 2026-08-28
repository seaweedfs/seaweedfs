package wdclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// TestWithClientStopsWaitingOnCanceledContext verifies that WithClient hands the
// caller's context to the wait for a master leader, so a caller that has already
// given up is not parked until an election finishes.
func TestWithClientStopsWaitingOnCanceledContext(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	start := time.Now()
	err := mc.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		called = true
		return nil
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if called {
		t.Error("callback ran without a master leader")
	}
	if elapsed > time.Second {
		t.Errorf("WithClient blocked for %v with an already canceled context", elapsed)
	}
}

// TestWithClientStopsWaitingOnDeadline verifies the same bound applies to a
// deadline the caller set rather than an outright cancellation.
func TestWithClientStopsWaitingOnDeadline(t *testing.T) {
	mc := NewMasterClient(grpc.EmptyDialOption{}, "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := mc.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		return nil
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("WithClient blocked for %v past a 100ms deadline", elapsed)
	}
}

// TestWithClientStopsBackoffOnCancel verifies that a cancellation arriving while
// the retry is backing off cuts the backoff short rather than sleeping it out.
func TestWithClientStopsBackoffOnCancel(t *testing.T) {
	mc := NewMasterClient(grpc.WithTransportCredentials(insecure.NewCredentials()), "test-group", "test-client", "", "", "", pb.ServerDiscovery{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	getMasterF := func() pb.ServerAddress { return "localhost:19333" }

	attempts := 0
	start := time.Now()
	err := mc.WithClientCustomGetMaster(ctx, getMasterF, false, func(client master_pb.SeaweedClient) error {
		attempts++
		cancel()
		return errors.New("connection reset by peer")
	})
	elapsed := time.Since(start)

	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if elapsed > time.Second {
		t.Errorf("WithClientCustomGetMaster slept %v after the context was canceled", elapsed)
	}
}
