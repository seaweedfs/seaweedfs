package plugin

import (
	"sort"
	"strings"
	"time"
)

type SchedulerStatus struct {
	Now                  time.Time                `json:"now"`
	Lane                 string                   `json:"lane,omitempty"`
	SchedulerTickSeconds int                      `json:"scheduler_tick_seconds"`
	IdleSleepSeconds     int                      `json:"idle_sleep_seconds,omitempty"`
	NextDetectionAt      *time.Time               `json:"next_detection_at,omitempty"`
	CurrentJobType       string                   `json:"current_job_type,omitempty"`
	CurrentPhase         string                   `json:"current_phase,omitempty"`
	LastIterationHadJobs bool                     `json:"last_iteration_had_jobs,omitempty"`
	LastIterationDoneAt  *time.Time               `json:"last_iteration_done_at,omitempty"`
	Waiting              []SchedulerWaitingStatus `json:"waiting,omitempty"`
	InProcessJobs        []SchedulerJobStatus     `json:"in_process_jobs,omitempty"`
	JobTypes             []SchedulerJobTypeStatus `json:"job_types,omitempty"`
}

type SchedulerWaitingStatus struct {
	Reason  string                 `json:"reason"`
	JobType string                 `json:"job_type,omitempty"`
	Since   *time.Time             `json:"since,omitempty"`
	Until   *time.Time             `json:"until,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
}

type SchedulerJobStatus struct {
	JobID           string     `json:"job_id"`
	JobType         string     `json:"job_type"`
	State           string     `json:"state"`
	Stage           string     `json:"stage,omitempty"`
	WorkerID        string     `json:"worker_id,omitempty"`
	Message         string     `json:"message,omitempty"`
	Progress        float64    `json:"progress,omitempty"`
	CreatedAt       *time.Time `json:"created_at,omitempty"`
	UpdatedAt       *time.Time `json:"updated_at,omitempty"`
	DurationSeconds float64    `json:"duration_seconds,omitempty"`
}

type SchedulerJobTypeStatus struct {
	JobType                  string     `json:"job_type"`
	Enabled                  bool       `json:"enabled"`
	DetectionInFlight        bool       `json:"detection_in_flight"`
	NextDetectionAt          *time.Time `json:"next_detection_at,omitempty"`
	DetectionIntervalSeconds int32      `json:"detection_interval_seconds,omitempty"`
	LastDetectedAt           *time.Time `json:"last_detected_at,omitempty"`
	LastDetectedCount        int        `json:"last_detected_count,omitempty"`
	LastDetectionError       string     `json:"last_detection_error,omitempty"`
	LastDetectionSkipped     string     `json:"last_detection_skipped,omitempty"`
}

type schedulerDetectionInfo struct {
	lastDetectedAt    time.Time
	lastDetectedCount int
	lastErrorAt       time.Time
	lastError         string
	lastSkippedAt     time.Time
	lastSkippedReason string
}

type schedulerRunInfo struct {
	lastRunStartedAt   time.Time
	lastRunCompletedAt time.Time
	lastRunStatus      string
}

type schedulerLoopState struct {
	currentJobType         string
	currentPhase           string
	lastIterationHadJobs   bool
	lastIterationCompleted time.Time
}

func (r *Plugin) recordSchedulerDetectionSuccess(jobType string, count int) {
	if r == nil {
		return
	}
	r.schedulerDetectionMu.Lock()
	defer r.schedulerDetectionMu.Unlock()
	info := r.schedulerDetection[jobType]
	if info == nil {
		info = &schedulerDetectionInfo{}
		r.schedulerDetection[jobType] = info
	}
	info.lastDetectedAt = time.Now().UTC()
	info.lastDetectedCount = count
	info.lastError = ""
	info.lastSkippedReason = ""
}

func (r *Plugin) recordSchedulerDetectionError(jobType string, err error) {
	if r == nil {
		return
	}
	if err == nil {
		return
	}
	r.schedulerDetectionMu.Lock()
	defer r.schedulerDetectionMu.Unlock()
	info := r.schedulerDetection[jobType]
	if info == nil {
		info = &schedulerDetectionInfo{}
		r.schedulerDetection[jobType] = info
	}
	info.lastErrorAt = time.Now().UTC()
	info.lastError = err.Error()
}

func (r *Plugin) recordSchedulerDetectionSkip(jobType string, reason string) {
	if r == nil {
		return
	}
	if strings.TrimSpace(reason) == "" {
		return
	}
	r.schedulerDetectionMu.Lock()
	defer r.schedulerDetectionMu.Unlock()
	info := r.schedulerDetection[jobType]
	if info == nil {
		info = &schedulerDetectionInfo{}
		r.schedulerDetection[jobType] = info
	}
	info.lastSkippedAt = time.Now().UTC()
	info.lastSkippedReason = reason
}

func (r *Plugin) snapshotSchedulerDetection(jobType string) schedulerDetectionInfo {
	if r == nil {
		return schedulerDetectionInfo{}
	}
	r.schedulerDetectionMu.Lock()
	defer r.schedulerDetectionMu.Unlock()
	info := r.schedulerDetection[jobType]
	if info == nil {
		return schedulerDetectionInfo{}
	}
	return *info
}

func (r *Plugin) recordSchedulerRunStart(jobType string) {
	if r == nil {
		return
	}
	r.schedulerRunMu.Lock()
	defer r.schedulerRunMu.Unlock()
	info := r.schedulerRun[jobType]
	if info == nil {
		info = &schedulerRunInfo{}
		r.schedulerRun[jobType] = info
	}
	info.lastRunStartedAt = time.Now().UTC()
	info.lastRunStatus = ""
}

func (r *Plugin) recordSchedulerRunComplete(jobType, status string) {
	if r == nil {
		return
	}
	r.schedulerRunMu.Lock()
	defer r.schedulerRunMu.Unlock()
	info := r.schedulerRun[jobType]
	if info == nil {
		info = &schedulerRunInfo{}
		r.schedulerRun[jobType] = info
	}
	info.lastRunCompletedAt = time.Now().UTC()
	info.lastRunStatus = status
}

func (r *Plugin) snapshotSchedulerRun(jobType string) schedulerRunInfo {
	if r == nil {
		return schedulerRunInfo{}
	}
	r.schedulerRunMu.Lock()
	defer r.schedulerRunMu.Unlock()
	info := r.schedulerRun[jobType]
	if info == nil {
		return schedulerRunInfo{}
	}
	return *info
}

func (r *Plugin) setSchedulerLoopState(jobType, phase string) {
	if r == nil {
		return
	}
	r.schedulerLoopMu.Lock()
	r.schedulerLoopState.currentJobType = jobType
	r.schedulerLoopState.currentPhase = phase
	r.schedulerLoopMu.Unlock()
}

func (r *Plugin) recordSchedulerIterationComplete(hadJobs bool) {
	if r == nil {
		return
	}
	r.schedulerLoopMu.Lock()
	r.schedulerLoopState.lastIterationHadJobs = hadJobs
	r.schedulerLoopState.lastIterationCompleted = time.Now().UTC()
	r.schedulerLoopMu.Unlock()
}

func (r *Plugin) snapshotSchedulerLoopState() schedulerLoopState {
	if r == nil {
		return schedulerLoopState{}
	}
	r.schedulerLoopMu.Lock()
	defer r.schedulerLoopMu.Unlock()
	return r.schedulerLoopState
}

// aggregateLaneLoopStates merges per-lane loop states into a single
// schedulerLoopState for the aggregate GetSchedulerStatus API. It picks
// the most recent iteration completion, any currently-active job type,
// and a phase that reflects whether any lane is actively working.
func (r *Plugin) aggregateLaneLoopStates() schedulerLoopState {
	if r == nil || len(r.lanes) == 0 {
		return r.snapshotSchedulerLoopState()
	}

	var agg schedulerLoopState
	for _, ls := range r.lanes {
		snap := r.snapshotLaneLoopState(ls)
		if snap.lastIterationCompleted.After(agg.lastIterationCompleted) {
			agg.lastIterationCompleted = snap.lastIterationCompleted
		}
		if snap.lastIterationHadJobs {
			agg.lastIterationHadJobs = true
		}
		// Prefer showing an active phase over idle/sleeping.
		if snap.currentJobType != "" {
			agg.currentJobType = snap.currentJobType
			agg.currentPhase = snap.currentPhase
		}
	}
	// If no lane is actively processing, show the most interesting phase.
	if agg.currentPhase == "" {
		for _, ls := range r.lanes {
			snap := r.snapshotLaneLoopState(ls)
			if snap.currentPhase != "" {
				agg.currentPhase = snap.currentPhase
				break
			}
		}
	}
	return agg
}

// --- Per-lane loop state helpers ---

func (r *Plugin) setLaneLoopState(ls *schedulerLaneState, jobType, phase string) {
	if r == nil || ls == nil {
		return
	}
	ls.loopMu.Lock()
	ls.loop.currentJobType = jobType
	ls.loop.currentPhase = phase
	ls.loopMu.Unlock()
}

func (r *Plugin) recordLaneIterationComplete(ls *schedulerLaneState, hadJobs bool) {
	if r == nil || ls == nil {
		return
	}
	ls.loopMu.Lock()
	ls.loop.lastIterationHadJobs = hadJobs
	ls.loop.lastIterationCompleted = time.Now().UTC()
	ls.loopMu.Unlock()
}

func (r *Plugin) snapshotLaneLoopState(ls *schedulerLaneState) schedulerLoopState {
	if r == nil || ls == nil {
		return schedulerLoopState{}
	}
	ls.loopMu.Lock()
	defer ls.loopMu.Unlock()
	return ls.loop
}

// GetLaneSchedulerStatus returns scheduler status scoped to a single lane.
func (r *Plugin) GetLaneSchedulerStatus(lane SchedulerLane) SchedulerStatus {
	ls := r.lanes[lane]
	if ls == nil {
		return SchedulerStatus{Now: time.Now().UTC()}
	}
	now := time.Now().UTC()
	loopState := r.snapshotLaneLoopState(ls)
	idleSleep := LaneIdleSleep(lane)
	allInProcess := r.listInProcessJobs(now)
	laneInProcess := make([]SchedulerJobStatus, 0, len(allInProcess))
	for _, job := range allInProcess {
		if JobTypeLane(job.JobType) == lane {
			laneInProcess = append(laneInProcess, job)
		}
	}

	status := SchedulerStatus{
		Now:                  now,
		Lane:                 string(lane),
		SchedulerTickSeconds: int(secondsFromDuration(r.schedulerTick)),
		InProcessJobs:        laneInProcess,
		IdleSleepSeconds:     int(idleSleep / time.Second),
		CurrentJobType:       loopState.currentJobType,
		CurrentPhase:         loopState.currentPhase,
		LastIterationHadJobs: loopState.lastIterationHadJobs,
	}
	nextDetectionAt := r.earliestLaneDetectionAt(lane)
	if nextDetectionAt.IsZero() && loopState.currentPhase == "sleeping" && !loopState.lastIterationCompleted.IsZero() {
		nextDetectionAt = loopState.lastIterationCompleted.Add(idleSleep)
	}
	if !nextDetectionAt.IsZero() {
		at := nextDetectionAt
		status.NextDetectionAt = &at
	}
	if !loopState.lastIterationCompleted.IsZero() {
		at := loopState.lastIterationCompleted
		status.LastIterationDoneAt = &at
	}

	states, err := r.ListSchedulerStates()
	if err != nil {
		return status
	}

	waiting := make([]SchedulerWaitingStatus, 0)
	jobTypes := make([]SchedulerJobTypeStatus, 0)

	for _, state := range states {
		if JobTypeLane(state.JobType) != lane {
			continue
		}
		jobType := state.JobType
		info := r.snapshotSchedulerDetection(jobType)

		jobStatus := SchedulerJobTypeStatus{
			JobType:                  jobType,
			Enabled:                  state.Enabled,
			DetectionInFlight:        state.DetectionInFlight,
			NextDetectionAt:          state.NextDetectionAt,
			DetectionIntervalSeconds: state.DetectionIntervalSeconds,
		}
		if !info.lastDetectedAt.IsZero() {
			jobStatus.LastDetectedAt = timeToPtr(info.lastDetectedAt)
			jobStatus.LastDetectedCount = info.lastDetectedCount
		}
		if info.lastError != "" {
			jobStatus.LastDetectionError = info.lastError
		}
		if info.lastSkippedReason != "" {
			jobStatus.LastDetectionSkipped = info.lastSkippedReason
		}
		jobTypes = append(jobTypes, jobStatus)

		if state.DetectionInFlight {
			waiting = append(waiting, SchedulerWaitingStatus{
				Reason:  "detection_in_flight",
				JobType: jobType,
			})
		} else if state.Enabled && state.NextDetectionAt != nil && now.Before(*state.NextDetectionAt) {
			waiting = append(waiting, SchedulerWaitingStatus{
				Reason:  "next_detection_at",
				JobType: jobType,
				Until:   state.NextDetectionAt,
			})
		}
	}

	sort.Slice(jobTypes, func(i, j int) bool {
		return jobTypes[i].JobType < jobTypes[j].JobType
	})

	status.Waiting = waiting
	status.JobTypes = jobTypes
	return status
}

func (r *Plugin) GetSchedulerStatus() SchedulerStatus {
	now := time.Now().UTC()

	// Aggregate loop state across all lanes instead of reading the
	// legacy single-loop state which is no longer updated.
	aggregated := r.aggregateLaneLoopStates()

	status := SchedulerStatus{
		Now:                  now,
		SchedulerTickSeconds: int(secondsFromDuration(r.schedulerTick)),
		InProcessJobs:        r.listInProcessJobs(now),
		IdleSleepSeconds:     int(defaultSchedulerIdleSleep / time.Second),
		CurrentJobType:       aggregated.currentJobType,
		CurrentPhase:         aggregated.currentPhase,
		LastIterationHadJobs: aggregated.lastIterationHadJobs,
	}
	nextDetectionAt := r.earliestNextDetectionAt()
	if nextDetectionAt.IsZero() && aggregated.currentPhase == "sleeping" && !aggregated.lastIterationCompleted.IsZero() {
		nextDetectionAt = aggregated.lastIterationCompleted.Add(defaultSchedulerIdleSleep)
	}
	if !nextDetectionAt.IsZero() {
		at := nextDetectionAt
		status.NextDetectionAt = &at
	}
	if !aggregated.lastIterationCompleted.IsZero() {
		at := aggregated.lastIterationCompleted
		status.LastIterationDoneAt = &at
	}

	states, err := r.ListSchedulerStates()
	if err != nil {
		return status
	}

	waiting := make([]SchedulerWaitingStatus, 0)
	jobTypes := make([]SchedulerJobTypeStatus, 0, len(states))

	for _, state := range states {
		jobType := state.JobType
		info := r.snapshotSchedulerDetection(jobType)

		jobStatus := SchedulerJobTypeStatus{
			JobType:                  jobType,
			Enabled:                  state.Enabled,
			DetectionInFlight:        state.DetectionInFlight,
			NextDetectionAt:          state.NextDetectionAt,
			DetectionIntervalSeconds: state.DetectionIntervalSeconds,
		}
		if !info.lastDetectedAt.IsZero() {
			jobStatus.LastDetectedAt = timeToPtr(info.lastDetectedAt)
			jobStatus.LastDetectedCount = info.lastDetectedCount
		}
		if info.lastError != "" {
			jobStatus.LastDetectionError = info.lastError
		}
		if info.lastSkippedReason != "" {
			jobStatus.LastDetectionSkipped = info.lastSkippedReason
		}
		jobTypes = append(jobTypes, jobStatus)

		if state.DetectionInFlight {
			waiting = append(waiting, SchedulerWaitingStatus{
				Reason:  "detection_in_flight",
				JobType: jobType,
			})
		} else if state.Enabled && state.NextDetectionAt != nil && now.Before(*state.NextDetectionAt) {
			waiting = append(waiting, SchedulerWaitingStatus{
				Reason:  "next_detection_at",
				JobType: jobType,
				Until:   state.NextDetectionAt,
			})
		}
	}

	sort.Slice(jobTypes, func(i, j int) bool {
		return jobTypes[i].JobType < jobTypes[j].JobType
	})

	status.Waiting = waiting
	status.JobTypes = jobTypes
	return status
}

func (r *Plugin) listInProcessJobs(now time.Time) []SchedulerJobStatus {
	active := make([]SchedulerJobStatus, 0)
	if r == nil {
		return active
	}

	r.jobsMu.RLock()
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if !isActiveTrackedJobState(job.State) {
			continue
		}
		start := timeToPtr(now)
		if job.CreatedAt != nil && !job.CreatedAt.IsZero() {
			start = job.CreatedAt
		} else if job.UpdatedAt != nil && !job.UpdatedAt.IsZero() {
			start = job.UpdatedAt
		}
		durationSeconds := 0.0
		if start != nil {
			durationSeconds = now.Sub(*start).Seconds()
		}
		active = append(active, SchedulerJobStatus{
			JobID:           job.JobID,
			JobType:         job.JobType,
			State:           strings.ToLower(job.State),
			Stage:           job.Stage,
			WorkerID:        job.WorkerID,
			Message:         job.Message,
			Progress:        job.Progress,
			CreatedAt:       job.CreatedAt,
			UpdatedAt:       job.UpdatedAt,
			DurationSeconds: durationSeconds,
		})
	}
	r.jobsMu.RUnlock()

	sort.Slice(active, func(i, j int) bool {
		if active[i].DurationSeconds != active[j].DurationSeconds {
			return active[i].DurationSeconds > active[j].DurationSeconds
		}
		return active[i].JobID < active[j].JobID
	})

	return active
}
