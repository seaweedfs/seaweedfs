package plugin

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// queuedJob is one scheduled spec parked on a home worker's admin-side queue.
type queuedJob struct {
	job  *plugin_pb.JobSpec
	home string
}

// workerJobQueues holds per-worker FIFOs for one dispatch batch.
// Home assignment is done once up front; run goroutines only pop/steal.
// leftover is the homeless FIFO: replica hostnames with no currently
// registered worker. Orphan grace does not apply here.
type workerJobQueues struct {
	mu           sync.Mutex
	q            map[string][]*queuedJob
	leftover     []*queuedJob
	remaining    int
	missingSince map[string]time.Time
	homes        map[string]struct{}
}

func newWorkerJobQueues() *workerJobQueues {
	return &workerJobQueues{
		q:            make(map[string][]*queuedJob),
		missingSince: make(map[string]time.Time),
		homes:        make(map[string]struct{}),
	}
}

func (qs *workerJobQueues) hasHome(workerID string) bool {
	if qs == nil || workerID == "" {
		return false
	}
	_, ok := qs.homes[workerID]
	return ok
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

func (qs *workerJobQueues) leftoverLen() int {
	if qs == nil {
		return 0
	}
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return len(qs.leftover)
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
// worker. A home exists only if a currently registered worker's -address
// hostname matches a replica host. Otherwise the job is homeless: global
// leftover FIFO, no fake home, no K, no orphan grace. Single-threaded.
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
		pref, _ := partitionByPreferredHost(executors, JobReplicaHosts(job))
		if len(pref) == 0 {
			// Never registered for that hostname: homeless, not an orphan.
			qs.push("", job)
			continue
		}
		for _, e := range pref {
			if e != nil && e.WorkerID != "" {
				qs.homes[e.WorkerID] = struct{}{}
			}
		}
		sortExecutorsByLoad(pref, func(e *WorkerSession) int {
			return r.executorReservationLoad(e, lane) + queued[e.WorkerID]
		})
		home := pref[0].WorkerID
		qs.push(home, job)
		queued[home]++
		r.bindQueuedJobWorker(job, home)
	}
	return qs
}

// takeQueuedJob pops a job for a free worker.
// Home worker (hostname matched a replica / has a home-queue identity):
//  1. own home queue
//  2. steal from another home if len>=K, or orphan past OrphanRehomeGrace
//  3. homeless leftover FIFO
// No-home worker (no matching replica, no home-queue identity):
//  1. homeless leftover FIFO
//  2. only if leftover empty: steal (K / orphan), same rules
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
	defer func() {
		if item != nil && targetID != "" {
			r.bindQueuedJobWorker(item.job, targetID)
		}
	}()

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

	takeLeftover := func() (*queuedJob, string) {
		if len(qs.leftover) == 0 {
			return nil, ""
		}
		item, qs.leftover = popFront(qs.leftover)
		qs.remaining--
		return item, thief.WorkerID
	}
	if !qs.hasHome(thief.WorkerID) {
		if item, id := takeLeftover(); item != nil {
			return item, id
		}
	}

	live := make(map[string]struct{}, len(executors))
	for _, exec := range executors {
		live[exec.WorkerID] = struct{}{}
		delete(qs.missingSince, exec.WorkerID)
	}

	now := time.Now()
	grace := policy.OrphanRehomeGrace
	victimID := ""
	victimLen := 0
	victimOrphan := false
	for id, qq := range qs.q {
		if len(qq) == 0 {
			delete(qs.missingSince, id)
			continue
		}
		_, isLive := live[id]
		orphan := !isLive
		if isLive {
			if pin > 0 && len(qq) < pin {
				continue
			}
		} else {
			if _, ok := qs.missingSince[id]; !ok {
				qs.missingSince[id] = now
			}
			if grace > 0 && now.Sub(qs.missingSince[id]) < grace {
				continue
			}
		}
		better := victimID == ""
		if !better && orphan != victimOrphan {
			better = orphan
		}
		if !better && orphan == victimOrphan {
			better = len(qq) > victimLen || (len(qq) == victimLen && id < victimID)
		}
		if better {
			victimID = id
			victimLen = len(qq)
			victimOrphan = orphan
		}
	}
	if victimID != "" {
		item, qs.q[victimID] = popFront(qs.q[victimID])
		qs.remaining--
		return item, thief.WorkerID
	}
	return takeLeftover()
}
