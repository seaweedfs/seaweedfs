package dash

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"

	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
)

// TestVacuumMonitorStateTransitions verifies the state transition logic that
// drives the vacuum monitor goroutine: the plugin registry correctly reports
// whether a vacuum-capable worker is present or absent.
func TestVacuumMonitorStateTransitions(t *testing.T) {
	t.Parallel()

	opts := adminplugin.Options{
		DataDir: "",
	}
	p, err := adminplugin.New(opts)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}
	defer p.Shutdown()

	// Initially no workers => no capable worker.
	if p.HasCapableWorker("vacuum") {
		t.Fatalf("expected no capable worker initially")
	}

	// Simulate state transition false -> true: worker connects.
	p.WorkerConnectForTest(&plugin_pb.WorkerHello{
		WorkerId: "vacuum-worker-1",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: "vacuum", CanDetect: true, CanExecute: true},
		},
	})

	if !p.HasCapableWorker("vacuum") {
		t.Fatalf("expected capable worker after connect")
	}

	// Simulate state transition true -> false: worker disconnects.
	p.WorkerDisconnectForTest("vacuum-worker-1")

	// Give registry removal a moment (it's synchronous, but be safe).
	time.Sleep(10 * time.Millisecond)

	if p.HasCapableWorker("vacuum") {
		t.Fatalf("expected no capable worker after disconnect")
	}
}
