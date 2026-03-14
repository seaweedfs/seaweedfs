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

// fakeToggler records disable/enable calls for testing.
type fakeToggler struct {
	disableCalls int
	enableCalls  int
}

func (f *fakeToggler) disableVacuum() error {
	f.disableCalls++
	return nil
}

func (f *fakeToggler) enableVacuum() error {
	f.enableCalls++
	return nil
}

func TestSyncVacuumState(t *testing.T) {
	t.Parallel()

	t.Run("no change when state matches", func(t *testing.T) {
		tog := &fakeToggler{}
		// both false
		result := syncVacuumState(false, false, tog)
		if result != false {
			t.Error("expected false")
		}
		// both true
		result = syncVacuumState(true, true, tog)
		if result != true {
			t.Error("expected true")
		}
		if tog.disableCalls != 0 || tog.enableCalls != 0 {
			t.Errorf("expected no calls, got disable=%d enable=%d", tog.disableCalls, tog.enableCalls)
		}
	})

	t.Run("worker connects triggers disable", func(t *testing.T) {
		tog := &fakeToggler{}
		result := syncVacuumState(true, false, tog)
		if result != true {
			t.Error("expected true after disable")
		}
		if tog.disableCalls != 1 {
			t.Errorf("expected 1 disable call, got %d", tog.disableCalls)
		}
		if tog.enableCalls != 0 {
			t.Errorf("expected 0 enable calls, got %d", tog.enableCalls)
		}
	})

	t.Run("worker disconnects triggers enable", func(t *testing.T) {
		tog := &fakeToggler{}
		result := syncVacuumState(false, true, tog)
		if result != false {
			t.Error("expected false after enable")
		}
		if tog.enableCalls != 1 {
			t.Errorf("expected 1 enable call, got %d", tog.enableCalls)
		}
		if tog.disableCalls != 0 {
			t.Errorf("expected 0 disable calls, got %d", tog.disableCalls)
		}
	})
}
