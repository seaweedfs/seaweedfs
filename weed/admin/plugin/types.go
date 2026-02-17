package plugin

import "time"

const (
	// Keep exactly the last 10 successful and last 10 error runs per job type.
	MaxSuccessfulRunHistory = 10
	MaxErrorRunHistory      = 10
)

type RunOutcome string

const (
	RunOutcomeSuccess RunOutcome = "success"
	RunOutcomeError   RunOutcome = "error"
)

type JobRunRecord struct {
	RunID       string     `json:"run_id"`
	JobID       string     `json:"job_id"`
	JobType     string     `json:"job_type"`
	WorkerID    string     `json:"worker_id"`
	Outcome     RunOutcome `json:"outcome"`
	Message     string     `json:"message,omitempty"`
	DurationMs  int64      `json:"duration_ms,omitempty"`
	CompletedAt time.Time  `json:"completed_at"`
}

type JobTypeRunHistory struct {
	JobType         string         `json:"job_type"`
	SuccessfulRuns  []JobRunRecord `json:"successful_runs"`
	ErrorRuns       []JobRunRecord `json:"error_runs"`
	LastUpdatedTime time.Time      `json:"last_updated_time"`
}

type TrackedJob struct {
	JobID         string    `json:"job_id"`
	JobType       string    `json:"job_type"`
	RequestID     string    `json:"request_id"`
	WorkerID      string    `json:"worker_id"`
	DedupeKey     string    `json:"dedupe_key,omitempty"`
	Summary       string    `json:"summary,omitempty"`
	State         string    `json:"state"`
	Progress      float64   `json:"progress"`
	Stage         string    `json:"stage,omitempty"`
	Message       string    `json:"message,omitempty"`
	Attempt       int32     `json:"attempt,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	ResultSummary string    `json:"result_summary,omitempty"`
}

type JobActivity struct {
	JobID      string                 `json:"job_id"`
	JobType    string                 `json:"job_type"`
	RequestID  string                 `json:"request_id,omitempty"`
	WorkerID   string                 `json:"worker_id,omitempty"`
	Source     string                 `json:"source"`
	Message    string                 `json:"message"`
	Stage      string                 `json:"stage,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
	OccurredAt time.Time              `json:"occurred_at"`
}

type SchedulerJobTypeState struct {
	JobType                       string     `json:"job_type"`
	Enabled                       bool       `json:"enabled"`
	PolicyError                   string     `json:"policy_error,omitempty"`
	DetectionInFlight             bool       `json:"detection_in_flight"`
	NextDetectionAt               *time.Time `json:"next_detection_at,omitempty"`
	DetectionIntervalSeconds      int32      `json:"detection_interval_seconds,omitempty"`
	DetectionTimeoutSeconds       int32      `json:"detection_timeout_seconds,omitempty"`
	ExecutionTimeoutSeconds       int32      `json:"execution_timeout_seconds,omitempty"`
	MaxJobsPerDetection           int32      `json:"max_jobs_per_detection,omitempty"`
	GlobalExecutionConcurrency    int        `json:"global_execution_concurrency,omitempty"`
	PerWorkerExecutionConcurrency int        `json:"per_worker_execution_concurrency,omitempty"`
	RetryLimit                    int        `json:"retry_limit,omitempty"`
	RetryBackoffSeconds           int32      `json:"retry_backoff_seconds,omitempty"`
	DetectorAvailable             bool       `json:"detector_available"`
	DetectorWorkerID              string     `json:"detector_worker_id,omitempty"`
	ExecutorWorkerCount           int        `json:"executor_worker_count"`
}
