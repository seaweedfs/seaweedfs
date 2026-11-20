package plugin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
)

// recordingExtension answers from the funcs it is given and counts calls.
type recordingExtension struct {
	defaultSchedulerExtension

	mu         sync.Mutex
	registered int
	lanes      []SchedulerLane
	admitted   []string
	asked      []string
	done       []string

	handleLane bool
	admit      func(job *plugin_pb.JobSpec) bool
	hold       func(job *plugin_pb.JobSpec, asked int) startHold
	holdsLock  *bool
}

func (e *recordingExtension) runLane(ls *schedulerLaneState, jobTypes []string) (bool, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lanes = append(e.lanes, ls.lane)
	return false, e.handleLane
}

func (e *recordingExtension) admitJob(job *plugin_pb.JobSpec) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.admit != nil && !e.admit(job) {
		return false
	}
	e.admitted = append(e.admitted, job.DedupeKey)
	return true
}

func (e *recordingExtension) startJob(job *plugin_pb.JobSpec, executor *WorkerSession) (func(), startHold) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asked = append(e.asked, job.DedupeKey)
	if e.hold != nil {
		if hold := e.hold(job, len(e.asked)); hold != startNow {
			return nil, hold
		}
	}
	return func() {
		e.mu.Lock()
		e.done = append(e.done, job.DedupeKey)
		e.mu.Unlock()
	}, startNow
}

func (e *recordingExtension) jobHoldsLock(job *plugin_pb.JobSpec, executor *WorkerSession) bool {
	if e.holdsLock != nil {
		return *e.holdsLock
	}
	return e.defaultSchedulerExtension.jobHoldsLock(job, executor)
}

func (e *recordingExtension) registerServices(grpc.ServiceRegistrar) {
	e.mu.Lock()
	e.registered++
	e.mu.Unlock()
}

func (e *recordingExtension) snapshot() (admitted, asked, done []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.admitted...), append([]string(nil), e.asked...), append([]string(nil), e.done...)
}

// dispatchWithExtension runs three proposals of jobType against a worker
// that has capacity but no stream, so every started job fails at send.
func dispatchWithExtension(t *testing.T, ext schedulerExtension, lm LockManager, jobType string) (*Plugin, [3]int) {
	t.Helper()
	pluginSvc, err := New(Options{LockManager: lm})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(pluginSvc.Shutdown)
	pluginSvc.schedulerExtension = ext

	pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
		WorkerId: "worker-a",
		Capabilities: []*plugin_pb.JobTypeCapability{
			{JobType: jobType, CanExecute: true, MaxExecutionConcurrency: 4},
		},
	})
	policy := schedulerPolicy{
		ExecutionConcurrency:   1,
		PerWorkerConcurrency:   1,
		ExecutionTimeout:       time.Second,
		ExecutorReserveBackoff: time.Millisecond,
	}
	proposals := []*plugin_pb.JobProposal{
		{ProposalId: "p1", JobType: jobType, DedupeKey: "k1"},
		{ProposalId: "p2", JobType: jobType, DedupeKey: "k2"},
		{ProposalId: "p3", JobType: jobType, DedupeKey: "k3"},
	}
	success, errCount, canceled := pluginSvc.dispatchScheduledProposals(
		context.Background(), jobType, proposals, &plugin_pb.ClusterContext{}, policy)
	return pluginSvc, [3]int{success, errCount, canceled}
}

func TestSchedulerExtensionDefaultsLeaveSchedulingAlone(t *testing.T) {
	ext := (&Plugin{}).schedulerExt()
	if hadJobs, handled := ext.runLane(newLaneState(LaneDefault), nil); hadJobs || handled {
		t.Fatal("default extension took over a lane")
	}
	if !ext.admitJob(&plugin_pb.JobSpec{}) {
		t.Fatal("default extension refused a job")
	}
	if done, hold := ext.startJob(&plugin_pb.JobSpec{}, nil); done != nil || hold != startNow {
		t.Fatal("default extension held a job")
	}
	if !ext.jobHoldsLock(&plugin_pb.JobSpec{JobType: "volume_balance"}, nil) {
		t.Fatal("default extension let a job run without the lock")
	}
}

func TestSchedulerExtensionAdmitsJobsBeforeTheyAreQueued(t *testing.T) {
	ext := &recordingExtension{admit: func(job *plugin_pb.JobSpec) bool { return job.DedupeKey != "k2" }}
	pluginSvc, counts := dispatchWithExtension(t, ext, nil, "s3_lifecycle")
	if counts != [3]int{0, 2, 0} {
		t.Fatalf("dispatch counts = %v, want two failed sends", counts)
	}
	pluginSvc.jobsMu.RLock()
	defer pluginSvc.jobsMu.RUnlock()
	if len(pluginSvc.jobs) != 2 {
		t.Fatalf("tracked jobs = %d, want 2", len(pluginSvc.jobs))
	}
	for _, job := range pluginSvc.jobs {
		if job.DedupeKey == "k2" {
			t.Fatal("a job the extension refused was tracked")
		}
	}
}

func TestSchedulerExtensionHoldsAJobAtTheHeadOfItsQueue(t *testing.T) {
	ext := &recordingExtension{hold: func(_ *plugin_pb.JobSpec, asked int) startHold {
		if asked <= 3 {
			return startRetry
		}
		return startNow
	}}
	_, counts := dispatchWithExtension(t, ext, nil, "s3_lifecycle")
	if counts != [3]int{0, 3, 0} {
		t.Fatalf("dispatch counts = %v", counts)
	}
	_, asked, done := ext.snapshot()
	want := []string{"k1", "k1", "k1", "k1", "k2", "k3"}
	if len(asked) != len(want) {
		t.Fatalf("asked = %v, want %v", asked, want)
	}
	for i := range want {
		if asked[i] != want[i] {
			t.Fatalf("asked = %v, want %v", asked, want)
		}
	}
	if len(done) != 3 || done[0] != "k1" || done[1] != "k2" || done[2] != "k3" {
		t.Fatalf("done = %v, want each started job reported once in order", done)
	}
}

func TestSchedulerExtensionLetsOtherJobsOfTheQueueGoFirst(t *testing.T) {
	ext := &recordingExtension{hold: func(job *plugin_pb.JobSpec, asked int) startHold {
		if job.DedupeKey == "k1" && asked == 1 {
			return startOthersFirst
		}
		return startNow
	}}
	_, counts := dispatchWithExtension(t, ext, nil, "s3_lifecycle")
	if counts != [3]int{0, 3, 0} {
		t.Fatalf("dispatch counts = %v", counts)
	}
	_, _, done := ext.snapshot()
	if len(done) != 3 || done[0] != "k2" || done[1] != "k3" || done[2] != "k1" {
		t.Fatalf("done = %v, want k1 behind the rest", done)
	}
}

func TestSchedulerExtensionReleasesTheSlotOfAHeldJob(t *testing.T) {
	ext := &recordingExtension{hold: func(_ *plugin_pb.JobSpec, asked int) startHold {
		if asked <= 5 {
			return startRetry
		}
		return startNow
	}}
	pluginSvc, _ := dispatchWithExtension(t, ext, nil, "s3_lifecycle")
	executors, err := pluginSvc.registry.ListExecutors("s3_lifecycle")
	if err != nil || len(executors) != 1 {
		t.Fatalf("executors = %v, %v", executors, err)
	}
	if load := pluginSvc.executorWorkerLoad(executors[0]); load != 0 {
		t.Fatalf("worker load = %d after dispatch, held jobs kept their slots", load)
	}
}

func TestSchedulerExtensionDecidesWhetherAJobHoldsTheLock(t *testing.T) {
	for _, holds := range []bool{true, false} {
		lm := &trackingLockManager{}
		holds := holds
		_, counts := dispatchWithExtension(t, &recordingExtension{holdsLock: &holds}, lm, "volume_balance")
		if counts != [3]int{0, 3, 0} {
			t.Fatalf("dispatch counts = %v", counts)
		}
		want := 0
		if holds {
			want = 3
		}
		if got := lm.count(); got != want {
			t.Fatalf("holds=%v: lock acquired %d times, want %d", holds, got, want)
		}
	}

	// A lane that needs no lock is not asked, and takes none.
	lm := &trackingLockManager{}
	holds := true
	dispatchWithExtension(t, &recordingExtension{holdsLock: &holds}, lm, "s3_lifecycle")
	if got := lm.count(); got != 0 {
		t.Fatalf("lifecycle lane acquired the lock %d times", got)
	}
}

func TestSchedulerExtensionCanTakeOverALane(t *testing.T) {
	for _, handle := range []bool{true, false} {
		lm := &trackingLockManager{}
		pluginSvc, err := New(Options{LockManager: lm})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ext := &recordingExtension{handleLane: handle}
		pluginSvc.schedulerExtension = ext
		pluginSvc.clusterContextProvider = func(context.Context) (*plugin_pb.ClusterContext, error) {
			return &plugin_pb.ClusterContext{}, nil
		}
		pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
			WorkerId:     "worker-a",
			Capabilities: []*plugin_pb.JobTypeCapability{{JobType: "vacuum", CanDetect: true}},
		})
		if err := pluginSvc.SaveJobTypeConfig(&plugin_pb.PersistedJobTypeConfig{
			JobType:      "vacuum",
			AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true, DetectionIntervalMinutes: 1},
		}); err != nil {
			t.Fatalf("SaveJobTypeConfig: %v", err)
		}
		pluginSvc.schedulerMu.Lock()
		pluginSvc.nextDetectionAt["vacuum"] = time.Now().UTC().Add(-time.Second)
		pluginSvc.schedulerMu.Unlock()

		pluginSvc.runLaneSchedulerIteration(pluginSvc.lanes[LaneDefault])
		pluginSvc.Shutdown()

		if len(ext.lanes) != 1 || ext.lanes[0] != LaneDefault {
			t.Fatalf("handle=%v: extension saw lanes %v", handle, ext.lanes)
		}
		// The built-in pass detects under the lock; a lane the extension
		// took over never gets there.
		if got := lm.count() > 0; got == handle {
			t.Fatalf("handle=%v: built-in lane pass ran=%v", handle, got)
		}
	}
}

func TestRegisterGrpcServicesIncludesTheExtension(t *testing.T) {
	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()
	ext := &recordingExtension{}
	pluginSvc.schedulerExtension = ext

	server := grpc.NewServer()
	pluginSvc.RegisterGrpcServices(server)
	if _, served := server.GetServiceInfo()[plugin_pb.PluginControlService_ServiceDesc.ServiceName]; !served {
		t.Fatal("the worker stream service is not registered")
	}
	if ext.registered != 1 {
		t.Fatalf("extension asked to register %d times, want once", ext.registered)
	}
}
