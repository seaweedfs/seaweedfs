package plugin

import (
	"sort"
	"strings"
	"time"
)

type SchedulerStatus struct {
	Now                  time.Time                `json:"now"`
	SchedulerTickSeconds int                      `json:"scheduler_tick_seconds"`
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

func (r *Plugin) GetSchedulerStatus() SchedulerStatus {
	now := time.Now().UTC()
	loopState := r.snapshotSchedulerLoopState()
	status := SchedulerStatus{
		Now:                  now,
		SchedulerTickSeconds: int(secondsFromDuration(r.schedulerTick)),
		InProcessJobs:        r.listInProcessJobs(now),
		CurrentJobType:       loopState.currentJobType,
		CurrentPhase:         loopState.currentPhase,
		LastIterationHadJobs: loopState.lastIterationHadJobs,
	}
	nextDetectionAt := r.earliestNextDetectionAt()
	if nextDetectionAt.IsZero() && loopState.currentPhase == "sleeping" && !loopState.lastIterationCompleted.IsZero() {
		nextDetectionAt = loopState.lastIterationCompleted.Add(defaultSchedulerIdleSleep)
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
