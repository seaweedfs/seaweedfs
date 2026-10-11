package plugin

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// queuedJob is one scheduled spec parked on a home worker's admin-side queue.
type queuedJob struct {
	job  *plugin_pb.JobSpec
	home string
}

// workerJobQueues holds per-worker FIFOs for one dispatch batch.
// Home assignment is done once up front; run goroutines only pop/steal.
type workerJobQueues struct {
	mu        sync.Mutex
	q         map[string][]*queuedJob
	leftover  []*queuedJob
	remaining int
}

func newWorkerJobQueues() *workerJobQueues {
	return &workerJobQueues{q: make(map[string][]*queuedJob)}
}

func (qs *workerJobQueues) push(home string, job *plugin_pb.JobSpec) {
	if qs == nil || job == nil {
		return
	}
	item := &queuedJob{job: job, home: home}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.pushLocked(item, false)
}

func (qs *workerJobQueues) pushFront(item *queuedJob) {
	if qs == nil || item == nil {
		return
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.pushLocked(item, true)
}

func (qs *workerJobQueues) pushLocked(item *queuedJob, front bool) {
	qs.remaining++
	if item.home == "" {
		if front {
			qs.leftover = append([]*queuedJob{item}, qs.leftover...)
			return
		}
		qs.leftover = append(qs.leftover, item)
		return
	}
	if front {
		qs.q[item.home] = append([]*queuedJob{item}, qs.q[item.home]...)
		return
	}
	qs.q[item.home] = append(qs.q[item.home], item)
}

func (qs *workerJobQueues) isEmpty() bool {
	if qs == nil {
		return true
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return qs.remaining == 0
}

func (qs *workerJobQueues) queueLen(workerID string) int {
	if qs == nil {
		return 0
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return len(qs.q[workerID])
}

func (qs *workerJobQueues) drain() []*queuedJob {
	if qs == nil {
		return nil
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	out := make([]*queuedJob, 0, qs.remaining)
	for id, qq := range qs.q {
		out = append(out, qq...)
		qs.q[id] = nil
	}
	out = append(out, qs.leftover...)
	qs.leftover = nil
	qs.remaining = 0
	return out
}

func popFront(qq []*queuedJob) (*queuedJob, []*queuedJob) {
	item := qq[0]
	if len(qq) == 1 {
		return item, nil
	}
	return item, qq[1:]
}

// assignJobsToWorkerQueues parks each job on the least-loaded replica-host
// worker (or any worker if none match). Single-threaded; no steal here.
func (r *Plugin) assignJobsToWorkerQueues(
	jobType string,
	jobs []*plugin_pb.JobSpec,
	_ schedulerPolicy,
) *workerJobQueues {
	qs := newWorkerJobQueues()
	executors, err := r.registry.ListExecutors(jobType)
	lane := JobTypeLane(jobType)
	queued := make(map[string]int, len(executors))

	for _, job := range jobs {
		if job == nil {
			continue
		}
		if err != nil || len(executors) == 0 {
			qs.push("", job)
			continue
		}
		pref, rest := partitionByPreferredHost(executors, JobReplicaHosts(job))
		cands := pref
		if len(cands) == 0 {
			cands = rest
		}
		sortExecutorsByLoad(cands, func(e *WorkerSession) int {
			return r.executorReservationLoad(e, lane) + queued[e.WorkerID]
		})
		home := cands[0].WorkerID
		qs.push(home, job)
		queued[home]++
		r.bindQueuedJobWorker(job, home)
	}
	return qs
}

// takeQueuedJob pops a home-runnable job, or steals from a queue at/over K
// onto a free worker. Returns nil when nothing can start now.
func (r *Plugin) takeQueuedJob(
	qs *workerJobQueues,
	jobType string,
	policy schedulerPolicy,
) (item *queuedJob, targetID string) {
	if qs == nil {
		return nil, ""
	}
	executors, err := r.registry.ListExecutors(jobType)
	if err != nil || len(executors) == 0 {
		return nil, ""
	}
	lane := JobTypeLane(jobType)
	pin := policy.LocalQueuePin

	qs.mu.Lock()
	defer qs.mu.Unlock()
	if qs.remaining == 0 {
		return nil, ""
	}

	var bestHome *WorkerSession
	bestLoad := 0
	for _, exec := range executors {
		if len(qs.q[exec.WorkerID]) == 0 {
			continue
		}
		limit := schedulerWorkerExecutionLimit(exec, jobType, policy)
		if limit <= 0 {
			continue
		}
		load := r.executorReservationLoad(exec, lane)
		if load >= limit {
			continue
		}
		if bestHome == nil || load < bestLoad || (load == bestLoad && exec.WorkerID < bestHome.WorkerID) {
			bestHome = exec
			bestLoad = load
		}
	}
	if bestHome != nil {
		id := bestHome.WorkerID
		item, qs.q[id] = popFront(qs.q[id])
		qs.remaining--
		return item, id
	}

	var thief *WorkerSession
	for _, exec := range executors {
		limit := schedulerWorkerExecutionLimit(exec, jobType, policy)
		if limit <= 0 {
			continue
		}
		if r.executorReservationLoad(exec, lane) >= limit {
			continue
		}
		if thief == nil || exec.WorkerID < thief.WorkerID {
			thief = exec
		}
	}
	if thief == nil {
		return nil, ""
	}

	if len(qs.leftover) > 0 {
		item, qs.leftover = popFront(qs.leftover)
		qs.remaining--
		return item, thief.WorkerID
	}

	victimID := ""
	victimLen := 0
	for id, qq := range qs.q {
		if len(qq) == 0 {
			continue
		}
		if pin > 0 && len(qq) < pin {
			continue
		}
		if victimID == "" || len(qq) > victimLen || (len(qq) == victimLen && id < victimID) {
			victimID = id
			victimLen = len(qq)
		}
	}
	if victimID == "" {
		return nil, ""
	}
	item, qs.q[victimID] = popFront(qs.q[victimID])
	qs.remaining--
	return item, thief.WorkerID
}
