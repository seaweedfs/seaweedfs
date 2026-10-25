package plugin

import (
	"fmt"
	"testing"
	"time"

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
	policy := schedulerPolicy{
		PerWorkerConcurrency: 1,
		LocalQueuePin:        5,
		OrphanRehomeGrace:    30 * time.Second,
	}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 2 {
		t.Fatalf("want 2 on local, got %d", qs.queueLen("worker-local"))
	}

	pluginSvc.registry.Remove("worker-local")

	if item, tgt := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy); item != nil {
		t.Fatalf("within grace should hold orphan queue, got target=%s", tgt)
	}
	if qs.queueLen("worker-local") != 2 {
		t.Fatalf("held queue changed: %d", qs.queueLen("worker-local"))
	}

	qs.mu.Lock()
	qs.missingSince["worker-local"] = time.Now().Add(-time.Minute)
	qs.mu.Unlock()

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" {
		t.Fatalf("after grace should steal without K, got job=%v target=%s", stolen, stealTarget)
	}
	if qs.queueLen("worker-local") != 1 {
		t.Fatalf("want 1 orphan left, got %d", qs.queueLen("worker-local"))
	}

	stolen2, stealTarget2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen2 == nil || stealTarget2 != "worker-remote" {
		t.Fatalf("second orphan should also move, got job=%v target=%s", stolen2, stealTarget2)
	}
}

func TestTakeKeepsOrphanWhenHomeReconnectsDuringGrace(t *testing.T) {
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

	jobs := []*plugin_pb.JobSpec{replicaJob("blink", "10.0.0.1:8080")}
	policy := schedulerPolicy{
		PerWorkerConcurrency: 1,
		LocalQueuePin:        5,
		OrphanRehomeGrace:    30 * time.Second,
	}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	pluginSvc.registry.Remove("worker-local")
	if item, _ := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy); item != nil {
		t.Fatal("expected hold during grace")
	}

	registerECWorkers(t, pluginSvc, map[string]string{"worker-local": "10.0.0.1:9334"})
	item, target := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item == nil || target != "worker-local" {
		t.Fatalf("reconnected home should run its queue, got job=%v target=%s", item, target)
	}
}

func TestTakeStealsOrphanImmediatelyWhenGraceDisabled(t *testing.T) {
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

	jobs := []*plugin_pb.JobSpec{replicaJob("o1", "10.0.0.1:8080")}
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5, OrphanRehomeGrace: 0}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	pluginSvc.registry.Remove("worker-local")

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" {
		t.Fatalf("grace=0 should steal immediately, got job=%v target=%s", stolen, stealTarget)
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

func TestHomelessAllRemoteWorkersUseFIFOWithoutPinOrGrace(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	// Workers never registered on the replica hostname — homeless, not orphan.
	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-a": "10.0.0.5:9334",
		"worker-b": "10.0.0.6:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("h1", "10.0.0.1:8080"),
		replicaJob("h2", "10.0.0.1:8080"),
		replicaJob("h3", "10.0.0.1:8080"),
	}
	policy := schedulerPolicy{
		PerWorkerConcurrency: 1,
		LocalQueuePin:        5,
		OrphanRehomeGrace:    30 * time.Second,
	}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-a") != 0 || qs.queueLen("worker-b") != 0 {
		t.Fatalf("homeless jobs must not get a fake home, a=%d b=%d", qs.queueLen("worker-a"), qs.queueLen("worker-b"))
	}
	if qs.leftoverLen() != 3 {
		t.Fatalf("want 3 homeless FIFO, got %d", qs.leftoverLen())
	}

	item1, target1 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item1 == nil || item1.job.JobId != "h1" {
		t.Fatalf("FIFO first want h1, got %v", item1)
	}
	_, release1, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, target1)
	if err != nil {
		t.Fatalf("reserve 1: %v", err)
	}
	defer release1()

	item2, target2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item2 == nil || item2.job.JobId != "h2" {
		t.Fatalf("FIFO second want h2 immediately, got %v", item2)
	}
	if target2 == "" || target2 == target1 {
		t.Fatalf("second take should hit the other idle worker, t1=%s t2=%s", target1, target2)
	}
	if qs.leftoverLen() != 1 {
		t.Fatalf("want 1 homeless left, got %d", qs.leftoverLen())
	}
}

func TestMixedReplicaLocalJobsStayHomeBeforeHomeless(t *testing.T) {
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
		replicaJob("homeless-1", "10.0.0.9:8080"),
		replicaJob("homeless-2", "10.0.0.9:8080"),
	}
	policy := schedulerPolicy{
		PerWorkerConcurrency: 1,
		LocalQueuePin:        5,
		OrphanRehomeGrace:    30 * time.Second,
	}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 2 || qs.queueLen("worker-remote") != 0 || qs.leftoverLen() != 2 {
		t.Fatalf("homes local=%d remote=%d leftover=%d", qs.queueLen("worker-local"), qs.queueLen("worker-remote"), qs.leftoverLen())
	}

	item1, target1 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item1 == nil || item1.job.JobId != "local-1" || target1 != "worker-local" {
		t.Fatalf("first take want local-1 on home, got job=%v target=%s", item1, target1)
	}
	_, release, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, "worker-local")
	if err != nil {
		t.Fatalf("reserve local: %v", err)
	}
	defer release()

	item2, target2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item2 == nil || item2.job.JobId != "homeless-1" || target2 != "worker-remote" {
		t.Fatalf("remote should take homeless, not steal pinned home, got job=%v target=%s", item2, target2)
	}
	if qs.queueLen("worker-local") != 1 {
		t.Fatalf("remaining replica-local job must stay home, got %d", qs.queueLen("worker-local"))
	}
	if qs.leftoverLen() != 1 {
		t.Fatalf("one homeless should remain, leftover=%d", qs.leftoverLen())
	}
}

func TestIdleHomeWorkerTakesHomelessAfterEmptyHomeQueue(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local": "10.0.0.1:9334",
	})

	jobs := []*plugin_pb.JobSpec{
		replicaJob("home-1", "10.0.0.1:8080"),
		replicaJob("homeless-1", "10.0.0.9:8080"),
	}
	policy := schedulerPolicy{
		PerWorkerConcurrency: 1,
		LocalQueuePin:        5,
		OrphanRehomeGrace:    30 * time.Second,
	}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 1 || qs.leftoverLen() != 1 {
		t.Fatalf("home=%d leftover=%d", qs.queueLen("worker-local"), qs.leftoverLen())
	}

	item1, target1 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item1 == nil || item1.job.JobId != "home-1" || target1 != "worker-local" {
		t.Fatalf("first take want home-1, got job=%v target=%s", item1, target1)
	}
	item2, target2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item2 == nil || item2.job.JobId != "homeless-1" || target2 != "worker-local" {
		t.Fatalf("idle home with empty queue may take homeless, got job=%v target=%s", item2, target2)
	}
	if qs.leftoverLen() != 0 {
		t.Fatalf("homeless should be gone, leftover=%d", qs.leftoverLen())
	}
}

func TestHomelessWorkerTakesFIFOBeforeStealableHome(t *testing.T) {
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

	jobs := make([]*plugin_pb.JobSpec, 0, 7)
	for i := 0; i < 6; i++ {
		jobs = append(jobs, replicaJob(fmt.Sprintf("j%d", i), "10.0.0.1:8080"))
	}
	jobs = append(jobs, replicaJob("homeless-1", "10.0.0.9:8080"))
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 6 || qs.leftoverLen() != 1 {
		t.Fatalf("home=%d leftover=%d", qs.queueLen("worker-local"), qs.leftoverLen())
	}
	if qs.hasHome("worker-remote") {
		t.Fatal("remote must have no home identity")
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

	item, takeTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item == nil || takeTarget != "worker-remote" || item.job.JobId != "homeless-1" {
		t.Fatalf("remote must take homeless FIFO before K-steal, got job=%v target=%s", item, takeTarget)
	}
	if qs.leftoverLen() != 0 {
		t.Fatalf("homeless should be gone, leftover=%d", qs.leftoverLen())
	}
	if qs.queueLen("worker-local") != 5 {
		t.Fatalf("stealable home must stay until FIFO empty, got %d", qs.queueLen("worker-local"))
	}

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-remote" || stolen.job.JobId == "homeless-1" {
		t.Fatalf("after FIFO empty remote may steal >=K, got job=%v target=%s", stolen, stealTarget)
	}
}

func TestHomeWorkerStealsBeforeHomelessAfterOwnQueueEmpty(t *testing.T) {
	t.Parallel()

	pluginSvc, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pluginSvc.Shutdown()

	registerECWorkers(t, pluginSvc, map[string]string{
		"worker-local": "10.0.0.1:9334",
		"worker-other": "10.0.0.2:9334",
	})

	jobs := make([]*plugin_pb.JobSpec, 0, 8)
	for i := 0; i < 6; i++ {
		jobs = append(jobs, replicaJob(fmt.Sprintf("j%d", i), "10.0.0.1:8080"))
	}
	jobs = append(jobs, replicaJob("other-1", "10.0.0.2:8080"))
	jobs = append(jobs, replicaJob("homeless-1", "10.0.0.9:8080"))
	policy := schedulerPolicy{PerWorkerConcurrency: 1, LocalQueuePin: 5}
	qs := pluginSvc.assignJobsToWorkerQueues("erasure_coding", jobs, policy)
	if qs.queueLen("worker-local") != 6 || qs.queueLen("worker-other") != 1 || qs.leftoverLen() != 1 {
		t.Fatalf("local=%d other=%d leftover=%d", qs.queueLen("worker-local"), qs.queueLen("worker-other"), qs.leftoverLen())
	}
	if !qs.hasHome("worker-other") {
		t.Fatal("other must keep home identity")
	}

	item1, target1 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item1 == nil || target1 != "worker-local" {
		t.Fatalf("first take want local home, got job=%v target=%s", item1, target1)
	}
	_, release, err := pluginSvc.reserveExecutorByID("erasure_coding", policy, "worker-local")
	if err != nil {
		t.Fatalf("reserve local: %v", err)
	}
	defer release()

	item2, target2 := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if item2 == nil || item2.job.JobId != "other-1" || target2 != "worker-other" {
		t.Fatalf("other should drain own home first, got job=%v target=%s", item2, target2)
	}

	stolen, stealTarget := pluginSvc.takeQueuedJob(qs, "erasure_coding", policy)
	if stolen == nil || stealTarget != "worker-other" || stolen.job.JobId == "homeless-1" {
		t.Fatalf("home worker with empty own queue still steals before homeless, got job=%v target=%s", stolen, stealTarget)
	}
	if qs.leftoverLen() != 1 {
		t.Fatalf("homeless should still wait, leftover=%d", qs.leftoverLen())
	}
}
