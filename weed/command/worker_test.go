package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWorkerDefaultJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handlers, err := buildPluginWorkerHandlers(*workerJobType, dialOption, int(vacuum.DefaultMaxExecutionConcurrency), "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(default worker flag) err = %v", err)
	}
	// Expected: vacuum, volume_balance, admin_script, erasure_coding,
	// iceberg_maintenance, ec_balance, s3_lifecycle.
	if len(handlers) != 7 {
		t.Fatalf("expected default worker job types to include 7 handlers, got %d", len(handlers))
	}
}
