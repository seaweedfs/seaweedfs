package plugin

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// WorkerSession contains tracked worker metadata and plugin status.
type WorkerSession struct {
	WorkerID        string
	WorkerInstance  string
	Address         string
	WorkerVersion   string
	ProtocolVersion string
	ConnectedAt     time.Time
	LastSeenAt      time.Time
	Capabilities    map[string]*plugin_pb.JobTypeCapability
	Heartbeat       *plugin_pb.WorkerHeartbeat
}

// Registry tracks connected plugin workers and capability-based selection.
type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*WorkerSession
}

func NewRegistry() *Registry {
	return &Registry{sessions: make(map[string]*WorkerSession)}
}

func (r *Registry) UpsertFromHello(hello *plugin_pb.WorkerHello) *WorkerSession {
	now := time.Now()
	caps := make(map[string]*plugin_pb.JobTypeCapability, len(hello.Capabilities))
	for _, c := range hello.Capabilities {
		if c == nil || c.JobType == "" {
			continue
		}
		caps[c.JobType] = cloneJobTypeCapability(c)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	session, ok := r.sessions[hello.WorkerId]
	if !ok {
		session = &WorkerSession{
			WorkerID:    hello.WorkerId,
			ConnectedAt: now,
		}
		r.sessions[hello.WorkerId] = session
	}

	session.WorkerInstance = hello.WorkerInstanceId
	session.Address = hello.Address
	session.WorkerVersion = hello.WorkerVersion
	session.ProtocolVersion = hello.ProtocolVersion
	session.LastSeenAt = now
	session.Capabilities = caps

	return cloneWorkerSession(session)
}

func (r *Registry) Remove(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, workerID)
}

func (r *Registry) UpdateHeartbeat(workerID string, heartbeat *plugin_pb.WorkerHeartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()

	session, ok := r.sessions[workerID]
	if !ok {
		return
	}
	session.Heartbeat = cloneWorkerHeartbeat(heartbeat)
	session.LastSeenAt = time.Now()
}

func (r *Registry) Get(workerID string) (*WorkerSession, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	session, ok := r.sessions[workerID]
	if !ok {
		return nil, false
	}
	return cloneWorkerSession(session), true
}

func (r *Registry) List() []*WorkerSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*WorkerSession, 0, len(r.sessions))
	for _, s := range r.sessions {
		out = append(out, cloneWorkerSession(s))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].WorkerID < out[j].WorkerID
	})
	return out
}

// DetectableJobTypes returns sorted job types that currently have at least one detect-capable worker.
func (r *Registry) DetectableJobTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobTypes := make(map[string]struct{})
	for _, session := range r.sessions {
		for jobType, capability := range session.Capabilities {
			if capability == nil || !capability.CanDetect {
				continue
			}
			jobTypes[jobType] = struct{}{}
		}
	}

	out := make([]string, 0, len(jobTypes))
	for jobType := range jobTypes {
		out = append(out, jobType)
	}
	sort.Strings(out)
	return out
}

// JobTypes returns sorted job types known by connected workers regardless of capability kind.
func (r *Registry) JobTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobTypes := make(map[string]struct{})
	for _, session := range r.sessions {
		for jobType := range session.Capabilities {
			if jobType == "" {
				continue
			}
			jobTypes[jobType] = struct{}{}
		}
	}

	out := make([]string, 0, len(jobTypes))
	for jobType := range jobTypes {
		out = append(out, jobType)
	}
	sort.Strings(out)
	return out
}

// PickSchemaProvider picks one worker for schema requests.
// Preference order:
// 1) workers that can detect this job type
// 2) workers that can execute this job type
// tie-break: more free slots, then lexical worker ID.
func (r *Registry) PickSchemaProvider(jobType string) (*WorkerSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var candidates []*WorkerSession
	for _, s := range r.sessions {
		capability := s.Capabilities[jobType]
		if capability == nil {
			continue
		}
		if capability.CanDetect || capability.CanExecute {
			candidates = append(candidates, s)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no worker available for schema job_type=%s", jobType)
	}

	sort.Slice(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		ac := a.Capabilities[jobType]
		bc := b.Capabilities[jobType]

		// Prefer detect-capable providers first.
		if ac.CanDetect != bc.CanDetect {
			return ac.CanDetect
		}

		aSlots := availableDetectionSlots(a, ac) + availableExecutionSlots(a, ac)
		bSlots := availableDetectionSlots(b, bc) + availableExecutionSlots(b, bc)
		if aSlots != bSlots {
			return aSlots > bSlots
		}
		return a.WorkerID < b.WorkerID
	})

	return cloneWorkerSession(candidates[0]), nil
}

// PickDetector picks one detector worker for a job type.
func (r *Registry) PickDetector(jobType string) (*WorkerSession, error) {
	return r.pickByKind(jobType, true)
}

// PickExecutor picks one executor worker for a job type.
func (r *Registry) PickExecutor(jobType string) (*WorkerSession, error) {
	return r.pickByKind(jobType, false)
}

// ListExecutors returns sorted executor candidates for one job type.
// Ordering is by most available execution slots, then lexical worker ID.
func (r *Registry) ListExecutors(jobType string) ([]*WorkerSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	candidates := r.collectByKindLocked(jobType, false)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no executor worker available for job_type=%s", jobType)
	}

	sortByKind(candidates, jobType, false)

	out := make([]*WorkerSession, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, cloneWorkerSession(candidate))
	}
	return out, nil
}

func (r *Registry) pickByKind(jobType string, detect bool) (*WorkerSession, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	candidates := r.collectByKindLocked(jobType, detect)

	if len(candidates) == 0 {
		kind := "executor"
		if detect {
			kind = "detector"
		}
		return nil, fmt.Errorf("no %s worker available for job_type=%s", kind, jobType)
	}

	sortByKind(candidates, jobType, detect)

	return cloneWorkerSession(candidates[0]), nil
}

func (r *Registry) collectByKindLocked(jobType string, detect bool) []*WorkerSession {
	var candidates []*WorkerSession
	for _, session := range r.sessions {
		capability := session.Capabilities[jobType]
		if capability == nil {
			continue
		}
		if detect && capability.CanDetect {
			candidates = append(candidates, session)
		}
		if !detect && capability.CanExecute {
			candidates = append(candidates, session)
		}
	}
	return candidates
}

func sortByKind(candidates []*WorkerSession, jobType string, detect bool) {
	sort.Slice(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		ac := a.Capabilities[jobType]
		bc := b.Capabilities[jobType]

		var aSlots, bSlots int
		if detect {
			aSlots = availableDetectionSlots(a, ac)
			bSlots = availableDetectionSlots(b, bc)
		} else {
			aSlots = availableExecutionSlots(a, ac)
			bSlots = availableExecutionSlots(b, bc)
		}

		if aSlots != bSlots {
			return aSlots > bSlots
		}
		return a.WorkerID < b.WorkerID
	})
}

func availableDetectionSlots(session *WorkerSession, capability *plugin_pb.JobTypeCapability) int {
	if session.Heartbeat != nil && session.Heartbeat.DetectionSlotsTotal > 0 {
		free := int(session.Heartbeat.DetectionSlotsTotal - session.Heartbeat.DetectionSlotsUsed)
		if free < 0 {
			return 0
		}
		return free
	}
	if capability.MaxDetectionConcurrency > 0 {
		return int(capability.MaxDetectionConcurrency)
	}
	return 1
}

func availableExecutionSlots(session *WorkerSession, capability *plugin_pb.JobTypeCapability) int {
	if session.Heartbeat != nil && session.Heartbeat.ExecutionSlotsTotal > 0 {
		free := int(session.Heartbeat.ExecutionSlotsTotal - session.Heartbeat.ExecutionSlotsUsed)
		if free < 0 {
			return 0
		}
		return free
	}
	if capability.MaxExecutionConcurrency > 0 {
		return int(capability.MaxExecutionConcurrency)
	}
	return 1
}

func cloneWorkerSession(in *WorkerSession) *WorkerSession {
	if in == nil {
		return nil
	}
	out := *in
	out.Capabilities = make(map[string]*plugin_pb.JobTypeCapability, len(in.Capabilities))
	for jobType, cap := range in.Capabilities {
		out.Capabilities[jobType] = cloneJobTypeCapability(cap)
	}
	out.Heartbeat = cloneWorkerHeartbeat(in.Heartbeat)
	return &out
}

func cloneJobTypeCapability(in *plugin_pb.JobTypeCapability) *plugin_pb.JobTypeCapability {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneWorkerHeartbeat(in *plugin_pb.WorkerHeartbeat) *plugin_pb.WorkerHeartbeat {
	if in == nil {
		return nil
	}
	out := *in
	if in.RunningWork != nil {
		out.RunningWork = make([]*plugin_pb.RunningWork, 0, len(in.RunningWork))
		for _, rw := range in.RunningWork {
			if rw == nil {
				continue
			}
			clone := *rw
			out.RunningWork = append(out.RunningWork, &clone)
		}
	}
	if in.QueuedJobsByType != nil {
		out.QueuedJobsByType = make(map[string]int32, len(in.QueuedJobsByType))
		for k, v := range in.QueuedJobsByType {
			out.QueuedJobsByType[k] = v
		}
	}
	if in.Metadata != nil {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for k, v := range in.Metadata {
			out.Metadata[k] = v
		}
	}
	return &out
}
