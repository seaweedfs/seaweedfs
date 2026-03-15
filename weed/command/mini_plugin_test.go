package command

import (
	"testing"

	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMiniDefaultPluginJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	// defaultMiniPluginJobTypes is "all", which includes every registered handler
	handlers, err := buildPluginWorkerHandlers(defaultMiniPluginJobTypes, dialOption, int(pluginworker.DefaultMaxExecutionConcurrency), "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(mini default) err = %v", err)
	}
	if len(handlers) != 6 {
		t.Fatalf("expected mini default job types to include 6 handlers, got %d", len(handlers))
	}
}
