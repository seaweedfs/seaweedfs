package dash

import (
	"errors"
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
	err          error
}

func (f *fakeToggler) disableVacuum() error {
	f.disableCalls++
	return f.err
}

func (f *fakeToggler) enableVacuum() error {
	f.enableCalls++
	return f.err
}

func TestSyncVacuumState(t *testing.T) {
	t.Parallel()

	t.Run("worker connects triggers disable", func(t *testing.T) {
		tog := &fakeToggler{}
		result, failed := syncVacuumState(true, false, tog, false)
		if result != true || failed {
			t.Error("expected true after disable, no failure")
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
		result, failed := syncVacuumState(false, true, tog, false)
		if result != false || failed {
			t.Error("expected false after enable, no failure")
		}
		if tog.enableCalls != 1 {
			t.Errorf("expected 1 enable call, got %d", tog.enableCalls)
		}
		if tog.disableCalls != 0 {
			t.Errorf("expected 0 disable calls, got %d", tog.disableCalls)
		}
	})

	// The master keeps the flag in memory only, so a new leader or a restarted
	// master has lost it. The disable has to be re-sent while the worker stays
	// connected, not only when it first connects.
	t.Run("disable is re-sent every tick while the worker stays connected", func(t *testing.T) {
		tog := &fakeToggler{}
		active := false
		for i := 0; i < 3; i++ {
			var failed bool
			active, failed = syncVacuumState(true, active, tog, false)
			if !active || failed {
				t.Fatalf("tick %d: expected active, no failure", i)
			}
		}
		if tog.disableCalls != 3 {
			t.Errorf("expected 3 disable calls, got %d", tog.disableCalls)
		}
		if tog.enableCalls != 0 {
			t.Errorf("expected 0 enable calls, got %d", tog.enableCalls)
		}
	})

	// Same for the enable side: a master that still holds an old plugin disable
	// and becomes leader would otherwise never vacuum again.
	t.Run("enable is re-sent every tick while no worker is connected", func(t *testing.T) {
		tog := &fakeToggler{}
		active := false
		for i := 0; i < 3; i++ {
			var failed bool
			active, failed = syncVacuumState(false, active, tog, false)
			if active || failed {
				t.Fatalf("tick %d: expected inactive, no failure", i)
			}
		}
		if tog.enableCalls != 3 {
			t.Errorf("expected 3 enable calls, got %d", tog.enableCalls)
		}
		if tog.disableCalls != 0 {
			t.Errorf("expected 0 disable calls, got %d", tog.disableCalls)
		}
	})

	t.Run("failed call keeps the previous state and reports failure", func(t *testing.T) {
		tog := &fakeToggler{err: errors.New("master unavailable")}
		result, failed := syncVacuumState(true, false, tog, false)
		if result != false || !failed {
			t.Error("expected previous state false and a failure")
		}
		result, failed = syncVacuumState(true, true, tog, false)
		if result != true || !failed {
			t.Error("expected previous state true and a failure")
		}
		result, failed = syncVacuumState(false, true, tog, false)
		if result != true || !failed {
			t.Error("expected previous state true and a failure")
		}
	})
}
