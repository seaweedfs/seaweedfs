package command

import (
	"testing"

	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMiniDefaultPluginJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	// defaultMiniPluginJobTypes is an explicit list: "vacuum,volume_balance,erasure_coding,admin_script"
	handlers, err := buildPluginWorkerHandlers(defaultMiniPluginJobTypes, dialOption, int(pluginworker.DefaultMaxExecutionConcurrency), "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(mini default) err = %v", err)
	}
	if len(handlers) != 4 {
		t.Fatalf("expected mini default job types to include 4 handlers, got %d", len(handlers))
	}
}
