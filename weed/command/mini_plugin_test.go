package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMiniDefaultPluginJobTypes(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	// defaultMiniPluginJobTypes is "all", which includes every registered handler
	handlers, err := buildPluginWorkerHandlers(defaultMiniPluginJobTypes, dialOption, int(vacuum.DefaultMaxExecutionConcurrency), "")
	if err != nil {
		t.Fatalf("buildPluginWorkerHandlers(mini default) err = %v", err)
	}
	if len(handlers) != 7 {
		t.Fatalf("expected mini default job types to include 7 handlers, got %d", len(handlers))
	}
}
