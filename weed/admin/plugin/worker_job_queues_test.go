package plugin

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func replicaJob(id, source string) *plugin_pb.JobSpec {
	return &plugin_pb.JobSpec{
		JobId:     id,
		JobType:   "erasure_coding",
		Summary:   id,
		DedupeKey: id,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"source_server": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: source},
			},
		},
	}
}

func registerECWorkers(t *testing.T, pluginSvc *Plugin, workers map[string]string) {
	t.Helper()
	for id, addr := range workers {
		pluginSvc.registry.UpsertFromHello(&plugin_pb.WorkerHello{
			WorkerId: id,
			Address:  addr,
			Capabilities: []*plugin_pb.JobTypeCapability{
				{JobType: "erasure_coding", CanExecute: true, MaxExecutionConcurrency: 8},
			},
		})
	}
}

func TestAssignJobsWaterfillsPreferredWorkers(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-a": "10.0.0.1:9334",
		"worker-b": "10.0.0.2:9334",
		"worker-c": "10.0.0.3:9334",
		"worker-d": "10.0.0.4:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("j1", "10.0.0.1:8080"),
		replicaJob("j2", "10.0.0.1:8080"),
		replicaJob("j3", "10.0.0.1:8080"),
		replicaJob("j4", "10.0.0.1:8080"),
	}
	// All four replica IPs listed so every worker is preferred.
	for _, job := range jobs {
		job.Parameters["replica_servers"] = &plugin_pb.ConfigValue{
			Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{
				Values: []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080", "10.0.0.4:8080"},
			}},
		}
	}

	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, schedulerPolicy{LocalQueuePin: 5})
	if qs.queueLen("worker-a")+qs.queueLen("worker-b")+qs.queueLen("worker-c")+qs.queueLen("worker-d") != 4 {
		t.Fatalf("expected 4 assigned jobs")
	}
	for _, id := range []string{"worker-a", "worker-b", "worker-c", "worker-d"} {
		if qs.queueLen(id) != 1 {
			t.Fatalf("want 1 job on %s, got %d", id, qs.queueLen(id))
		}
	}
}

func TestAssignAndTakeDoesNotStarveOtherHomes(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local":  "10.0.0.1:9334",
		"worker-remote": "10.0.0.2:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("local-1", "10.0.0.1:8080"),
		replicaJob("local-2", "10.0.0.1:8080"),
		replicaJob("remote-1", "10.0.0.2:8080"),
	}
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 2 || qs.queueLen("worker-remote") != 1 {
		t.Fatalf("homes local=%d remote=%d", qs.queueLen("worker-local"), qs.queueLen("worker-remote"))
	}

	_, target1 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	_, release1, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, target1)
	if err != nil {
		t.Fatalf("reserve 1: %v", err)
	}
	defer release1()

	item2, target2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item2 == nil || target2 != "worker-remote" {
		t.Fatalf("second take should start remote home, got job=%v target=%s", item2, target2)
	}
}

func TestTakeStealsOnlyWhenHomeQueueReachesK(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local":  "10.0.0.1:9334",
		"worker-remote": "10.0.0.2:9334",
	})

	jobs := make([]*plugin_pb.JobSpec, 0, 6)
	for i := 0; i < 6; i++ {
		jobs = append(jobs, replicaJob(fmt.Sprintf("j%d", i), "10.0.0.1:8080"))
	}
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 6 {
		t.Fatalf("all jobs should home on local, got %d", qs.queueLen("worker-local"))
	}

	_, target := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if target != "worker-local" {
		t.Fatalf("first take want local, got %s", target)
	}
	_, release, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, "worker-local")
	if err != nil {
		t.Fatalf("reserve local: %v", err)
	}
	defer release()

	// 5 still queued on local (>=K) and local has no slot → steal to remote.
	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" {
		t.Fatalf("expected steal to remote, got job=%v target=%s", stolen, stealTarget)
	}
	if qs.queueLen("worker-local") != 4 {
		t.Fatalf("after steal want 4 left on local, got %d", qs.queueLen("worker-local"))
	}

	// 4 < K: do not steal again.
	if item, tgt := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy); item != nil {
		t.Fatalf("pin should hold remaining jobs, got target=%s", tgt)
	}
}

func TestTakeStealsImmediatelyWhenPinDisabled(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local":  "10.0.0.1:9334",
		"worker-remote": "10.0.0.2:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("a", "10.0.0.1:8080"),
		replicaJob("b", "10.0.0.1:8080"),
	}
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 0}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)

	_, target := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if target != "worker-local" {
		t.Fatalf("first take want local, got %s", target)
	}
	_, release, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, "worker-local")
	if err != nil {
		t.Fatalf("reserve local: %v", err)
	}
	defer release()

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" {
		t.Fatalf("K=0 should steal immediately, got job=%v target=%s", stolen, stealTarget)
	}
}

func TestTakeStealsOrphanedHomeWithoutK(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local":  "10.0.0.1:9334",
		"worker-remote": "10.0.0.2:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("o1", "10.0.0.1:8080"),
		replicaJob("o2", "10.0.0.1:8080"),
	}
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 2 {
		t.Fatalf("want 2 on local, got %d", qs.queueLen("worker-local"))
	}

	pluginSvc.registry.Remove("worker-local")

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" {
		t.Fatalf("dead home should steal without K, got job=%v target=%s", stolen, stealTarget)
	}
	if qs.queueLen("worker-local") != 1 {
		t.Fatalf("want 1 orphan left, got %d", qs.queueLen("worker-local"))
	}

	stolen2, stealTarget2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen2 == nil || stealTarget2 != "worker-remote" {
		t.Fatalf("second orphan should also move, got job=%v target=%s", stolen2, stealTarget2)
	}
}

func TestBindQueuedJobWorkerKeepsPendingState(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	job := replicaJob("bind-1", "10.0.0.1:8080")
	pluginSvc.trackExecutionQueued(job)
	pluginSvc.bindQueuedJobWorker(job, "worker-local")

	pluginSvc.jobsMu.Lock()
	tracked := pluginSvc.jobs[job.JobId]
	pluginSvc.jobsMu.Unlock()
	if tracked == nil {
		t.Fatal("missing tracked job")
	}
	if tracked.State != "job_state_pending" {
		t.Fatalf("state=%s", tracked.State)
	}
	if tracked.Stage != "queued" {
		t.Fatalf("stage=%s", tracked.Stage)
	}
	if tracked.WorkerID != "worker-local" {
		t.Fatalf("worker=%s", tracked.WorkerID)
	}
}
