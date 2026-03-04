package plugin

import (
	"sort"
	"strings"
	"time"
)

type SchedulerStatus struct {
	Now                       time.Time                `json:"now"`
	SchedulerTickSeconds      int                      `json:"scheduler_tick_seconds"`
	IdleSleepSeconds          int                      `json:"idle_sleep_seconds"`
	Phase                     string                   `json:"phase"`
	CurrentJobType            string                   `json:"current_job_type,omitempty"`
	IterationStartedAt        *time.Time               `json:"iteration_started_at,omitempty"`
	LastIterationEndedAt      *time.Time               `json:"last_iteration_ended_at,omitempty"`
	LastIterationWorkDetected bool                     `json:"last_iteration_work_detected"`
	Waiting                   []SchedulerWaitingStatus `json:"waiting,omitempty"`
	InProcessJobs             []SchedulerJobStatus     `json:"in_process_jobs,omitempty"`
	JobTypes                  []SchedulerJobTypeStatus `json:"job_types,omitempty"`
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
	JobType                      string     `json:"job_type"`
	Enabled                      bool       `json:"enabled"`
	DetectionInFlight            bool       `json:"detection_in_flight"`
	MaxJobTypeDurationSeconds    int32      `json:"max_job_type_duration_seconds,omitempty"`
	LastDetectedAt               *time.Time `json:"last_detected_at,omitempty"`
	LastDetectedCount            int        `json:"last_detected_count,omitempty"`
	LastDetectionError           string     `json:"last_detection_error,omitempty"`
	LastDetectionSkipped         string     `json:"last_detection_skipped,omitempty"`
}

type schedulerDetectionInfo struct {
	lastDetectedAt    time.Time
	lastDetectedCount int
	lastErrorAt       time.Time
	lastError         string
	lastSkippedAt     time.Time
	lastSkippedReason string
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

func (r *Plugin) GetSchedulerStatus() SchedulerStatus {
	now := time.Now().UTC()

	r.schedulerMu.Lock()
	phase := r.schedulerPhase
	currentJobType := r.currentJobType
	iterationStartedAt := r.iterationStartedAt
	lastIterationEndedAt := r.lastIterationEndedAt
	lastIterationWorkDetected := r.lastIterationWorkDetected
	r.schedulerMu.Unlock()

	status := SchedulerStatus{
		Now:                       now,
		SchedulerTickSeconds:      int(secondsFromDuration(r.idleSleepDuration)),
		IdleSleepSeconds:          int(secondsFromDuration(r.idleSleepDuration)),
		Phase:                     phase,
		CurrentJobType:            currentJobType,
		LastIterationWorkDetected: lastIterationWorkDetected,
		InProcessJobs:             r.listInProcessJobs(now),
	}
	if !iterationStartedAt.IsZero() {
		status.IterationStartedAt = timeToPtr(iterationStartedAt)
	}
	if !lastIterationEndedAt.IsZero() {
		status.LastIterationEndedAt = timeToPtr(lastIterationEndedAt)
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
			JobType:                   jobType,
			Enabled:                   state.Enabled,
			DetectionInFlight:         state.DetectionInFlight,
			MaxJobTypeDurationSeconds: state.MaxJobTypeDurationSeconds,
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
				Reason:  "processing",
				JobType: jobType,
			})
		}
	}

	if phase == "idle" && !lastIterationEndedAt.IsZero() {
		waiting = append(waiting, SchedulerWaitingStatus{
			Reason: "idle_sleep",
		})
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
