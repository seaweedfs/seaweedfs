package command

import (
	"sort"
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
	want := []string{
		"admin_script",
		"ec_balance",
		"erasure_coding",
		"iceberg_maintenance",
		"s3_lifecycle",
		"vacuum",
		"volume_balance",
	}
	got := make([]string, 0, len(handlers))
	for _, h := range handlers {
		got = append(got, h.Capability().JobType)
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("default worker job types: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("default worker job types: got %v, want %v", got, want)
		}
	}
}
