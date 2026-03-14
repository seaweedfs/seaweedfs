package command

import (
	"testing"

	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWorkerDefaultJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	handlers, err := buildPluginWorkerHandlers(*workerJobType, dialOption, int(pluginworker.DefaultMaxExecutionConcurrency), "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(default worker flag) err = %v", err)
	}
	// Expected: vacuum, volume_balance, admin_script, erasure_coding, iceberg_maintenance, ec_balance
	if len(handlers) != 6 {
		t.Fatalf("expected default worker job types to include 6 handlers, got %d", len(handlers))
	}
}
