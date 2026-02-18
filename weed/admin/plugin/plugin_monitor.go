package plugin

import (
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	maxTrackedJobsTotal = 1000
	maxActivityRecords  = 4000
)

func (r *Plugin) loadPersistedMonitorState() error {
	trackedJobs, err := r.store.LoadTrackedJobs()
	if err != nil {
		return err
	}
	activities, err := r.store.LoadActivities()
	if err != nil {
		return err
	}

	if len(trackedJobs) > 0 {
		r.jobsMu.Lock()
		for i := range trackedJobs {
			job := trackedJobs[i]
			if strings.TrimSpace(job.JobID) == "" {
				continue
			}
			jobCopy := job
			r.jobs[job.JobID] = &jobCopy
		}
		r.pruneTrackedJobsLocked()
		r.jobsMu.Unlock()
	}

	if len(activities) > maxActivityRecords {
		activities = activities[len(activities)-maxActivityRecords:]
	}
	if len(activities) > 0 {
		r.activitiesMu.Lock()
		r.activities = append([]JobActivity(nil), activities...)
		r.activitiesMu.Unlock()
	}

	return nil
}

func (r *Plugin) ListTrackedJobs(jobType string, state string, limit int) []TrackedJob {
	r.jobsMu.RLock()
	defer r.jobsMu.RUnlock()

	normalizedJobType := strings.TrimSpace(jobType)
	normalizedState := strings.TrimSpace(strings.ToLower(state))

	items := make([]TrackedJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if normalizedJobType != "" && job.JobType != normalizedJobType {
			continue
		}
		if normalizedState != "" && strings.ToLower(job.State) != normalizedState {
			continue
		}
		items = append(items, *job)
	}

	sort.Slice(items, func(i, j int) bool {
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return items[i].JobID < items[j].JobID
	})

	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

func (r *Plugin) GetTrackedJob(jobID string) (*TrackedJob, bool) {
	r.jobsMu.RLock()
	defer r.jobsMu.RUnlock()

	job, ok := r.jobs[jobID]
	if !ok || job == nil {
		return nil, false
	}
	clone := *job
	return &clone, true
}

func (r *Plugin) ListActivities(jobType string, limit int) []JobActivity {
	r.activitiesMu.RLock()
	defer r.activitiesMu.RUnlock()

	normalized := strings.TrimSpace(jobType)
	activities := make([]JobActivity, 0, len(r.activities))
	for _, activity := range r.activities {
		if normalized != "" && activity.JobType != normalized {
			continue
		}
		activities = append(activities, activity)
	}

	sort.Slice(activities, func(i, j int) bool {
		return activities[i].OccurredAt.After(activities[j].OccurredAt)
	})
	if limit > 0 && len(activities) > limit {
		activities = activities[:limit]
	}
	return activities
}

func (r *Plugin) handleJobProgressUpdate(workerID string, update *plugin_pb.JobProgressUpdate) {
	if update == nil {
		return
	}

	now := time.Now().UTC()
	resolvedWorkerID := strings.TrimSpace(workerID)

	if strings.TrimSpace(update.JobId) != "" {
		r.jobsMu.Lock()
		job := r.jobs[update.JobId]
		if job == nil {
			job = &TrackedJob{
				JobID:     update.JobId,
				JobType:   update.JobType,
				RequestID: update.RequestId,
				WorkerID:  resolvedWorkerID,
				CreatedAt: now,
			}
			r.jobs[update.JobId] = job
		}

		if update.JobType != "" {
			job.JobType = update.JobType
		}
		if update.RequestId != "" {
			job.RequestID = update.RequestId
		}
		if job.WorkerID != "" {
			resolvedWorkerID = job.WorkerID
		} else if resolvedWorkerID != "" {
			job.WorkerID = resolvedWorkerID
		}
		job.State = strings.ToLower(update.State.String())
		job.Progress = update.ProgressPercent
		job.Stage = update.Stage
		job.Message = update.Message
		job.UpdatedAt = now
		r.pruneTrackedJobsLocked()
		r.jobsMu.Unlock()
		r.persistTrackedJobsSnapshot()
	}

	r.trackWorkerActivities(update.JobType, update.JobId, update.RequestId, resolvedWorkerID, update.Activities)
	if update.Message != "" || update.Stage != "" {
		source := "worker_progress"
		if strings.TrimSpace(update.JobId) == "" {
			source = "worker_detection"
		}
		r.appendActivity(JobActivity{
			JobID:      update.JobId,
			JobType:    update.JobType,
			RequestID:  update.RequestId,
			WorkerID:   resolvedWorkerID,
			Source:     source,
			Message:    update.Message,
			Stage:      update.Stage,
			OccurredAt: now,
		})
	}
}

func (r *Plugin) trackExecutionStart(requestID, workerID string, job *plugin_pb.JobSpec, attempt int32) {
	if job == nil || strings.TrimSpace(job.JobId) == "" {
		return
	}

	now := time.Now().UTC()

	r.jobsMu.Lock()
	tracked := r.jobs[job.JobId]
	if tracked == nil {
		tracked = &TrackedJob{
			JobID:     job.JobId,
			CreatedAt: now,
		}
		r.jobs[job.JobId] = tracked
	}

	tracked.JobType = job.JobType
	tracked.RequestID = requestID
	tracked.WorkerID = workerID
	tracked.DedupeKey = job.DedupeKey
	tracked.Summary = job.Summary
	tracked.State = strings.ToLower(plugin_pb.JobState_JOB_STATE_ASSIGNED.String())
	tracked.Progress = 0
	tracked.Stage = "assigned"
	tracked.Message = "job assigned to worker"
	tracked.Attempt = attempt
	if tracked.CreatedAt.IsZero() {
		tracked.CreatedAt = now
	}
	tracked.UpdatedAt = now
	r.pruneTrackedJobsLocked()
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()

	r.appendActivity(JobActivity{
		JobID:      job.JobId,
		JobType:    job.JobType,
		RequestID:  requestID,
		WorkerID:   workerID,
		Source:     "admin_dispatch",
		Message:    "job assigned",
		Stage:      "assigned",
		OccurredAt: now,
	})
}

func (r *Plugin) trackExecutionQueued(job *plugin_pb.JobSpec) {
	if job == nil || strings.TrimSpace(job.JobId) == "" {
		return
	}

	now := time.Now().UTC()

	r.jobsMu.Lock()
	tracked := r.jobs[job.JobId]
	if tracked == nil {
		tracked = &TrackedJob{
			JobID:     job.JobId,
			CreatedAt: now,
		}
		r.jobs[job.JobId] = tracked
	}

	tracked.JobType = job.JobType
	tracked.DedupeKey = job.DedupeKey
	tracked.Summary = job.Summary
	tracked.State = strings.ToLower(plugin_pb.JobState_JOB_STATE_PENDING.String())
	tracked.Progress = 0
	tracked.Stage = "queued"
	tracked.Message = "waiting for available executor"
	if tracked.CreatedAt.IsZero() {
		tracked.CreatedAt = now
	}
	tracked.UpdatedAt = now
	r.pruneTrackedJobsLocked()
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()

	r.appendActivity(JobActivity{
		JobID:      job.JobId,
		JobType:    job.JobType,
		Source:     "admin_scheduler",
		Message:    "job queued for execution",
		Stage:      "queued",
		OccurredAt: now,
	})
}

func (r *Plugin) trackExecutionCompletion(completed *plugin_pb.JobCompleted) *TrackedJob {
	if completed == nil || strings.TrimSpace(completed.JobId) == "" {
		return nil
	}

	now := time.Now().UTC()
	if completed.CompletedAt != nil {
		now = completed.CompletedAt.AsTime().UTC()
	}

	r.jobsMu.Lock()
	tracked := r.jobs[completed.JobId]
	if tracked == nil {
		tracked = &TrackedJob{
			JobID:     completed.JobId,
			CreatedAt: now,
		}
		r.jobs[completed.JobId] = tracked
	}

	if completed.JobType != "" {
		tracked.JobType = completed.JobType
	}
	if completed.RequestId != "" {
		tracked.RequestID = completed.RequestId
	}
	if completed.Success {
		tracked.State = strings.ToLower(plugin_pb.JobState_JOB_STATE_SUCCEEDED.String())
		tracked.Progress = 100
		tracked.Stage = "completed"
		if completed.Result != nil {
			tracked.ResultSummary = completed.Result.Summary
		}
		tracked.Message = tracked.ResultSummary
		if tracked.Message == "" {
			tracked.Message = "completed"
		}
		tracked.ErrorMessage = ""
	} else {
		tracked.State = strings.ToLower(plugin_pb.JobState_JOB_STATE_FAILED.String())
		tracked.Stage = "failed"
		tracked.ErrorMessage = completed.ErrorMessage
		tracked.Message = completed.ErrorMessage
	}

	tracked.UpdatedAt = now
	tracked.CompletedAt = now
	r.pruneTrackedJobsLocked()
	clone := *tracked
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()

	r.appendActivity(JobActivity{
		JobID:      completed.JobId,
		JobType:    completed.JobType,
		RequestID:  completed.RequestId,
		WorkerID:   tracked.WorkerID,
		Source:     "worker_completion",
		Message:    clone.Message,
		Stage:      clone.Stage,
		OccurredAt: now,
	})

	return &clone
}

func (r *Plugin) trackWorkerActivities(jobType, jobID, requestID, workerID string, events []*plugin_pb.ActivityEvent) {
	if len(events) == 0 {
		return
	}
	for _, event := range events {
		if event == nil {
			continue
		}
		timestamp := time.Now().UTC()
		if event.CreatedAt != nil {
			timestamp = event.CreatedAt.AsTime().UTC()
		}
		r.appendActivity(JobActivity{
			JobID:      jobID,
			JobType:    jobType,
			RequestID:  requestID,
			WorkerID:   workerID,
			Source:     strings.ToLower(event.Source.String()),
			Message:    event.Message,
			Stage:      event.Stage,
			Details:    configValueMapToPlain(event.Details),
			OccurredAt: timestamp,
		})
	}
}

func (r *Plugin) appendActivity(activity JobActivity) {
	if activity.OccurredAt.IsZero() {
		activity.OccurredAt = time.Now().UTC()
	}

	r.activitiesMu.Lock()
	r.activities = append(r.activities, activity)
	if len(r.activities) > maxActivityRecords {
		r.activities = r.activities[len(r.activities)-maxActivityRecords:]
	}
	r.activitiesMu.Unlock()
	r.persistActivitiesSnapshot()
}

func (r *Plugin) pruneTrackedJobsLocked() {
	if len(r.jobs) <= maxTrackedJobsTotal {
		return
	}

	type trackedRef struct {
		jobID   string
		updated time.Time
	}

	completed := make([]trackedRef, 0, len(r.jobs))
	for jobID, job := range r.jobs {
		if job == nil {
			continue
		}
		if job.State == strings.ToLower(plugin_pb.JobState_JOB_STATE_SUCCEEDED.String()) ||
			job.State == strings.ToLower(plugin_pb.JobState_JOB_STATE_FAILED.String()) ||
			job.State == strings.ToLower(plugin_pb.JobState_JOB_STATE_CANCELED.String()) {
			completed = append(completed, trackedRef{jobID: jobID, updated: job.UpdatedAt})
		}
	}

	if len(completed) == 0 {
		return
	}

	sort.Slice(completed, func(i, j int) bool {
		return completed[i].updated.Before(completed[j].updated)
	})

	toDelete := len(r.jobs) - maxTrackedJobsTotal
	if toDelete <= 0 {
		return
	}
	if toDelete > len(completed) {
		toDelete = len(completed)
	}

	for i := 0; i < toDelete; i++ {
		delete(r.jobs, completed[i].jobID)
	}
}

func configValueMapToPlain(values map[string]*plugin_pb.ConfigValue) map[string]interface{} {
	if len(values) == 0 {
		return nil
	}

	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&plugin_pb.ValueMap{Fields: values})
	if err != nil {
		return nil
	}

	decoded := map[string]interface{}{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil
	}

	fields, ok := decoded["fields"].(map[string]interface{})
	if !ok {
		return nil
	}
	return fields
}

func (r *Plugin) persistTrackedJobsSnapshot() {
	r.jobsMu.RLock()
	jobs := make([]TrackedJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		if job == nil || strings.TrimSpace(job.JobID) == "" {
			continue
		}
		jobs = append(jobs, *job)
	}
	r.jobsMu.RUnlock()

	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].UpdatedAt.Equal(jobs[j].UpdatedAt) {
			return jobs[i].UpdatedAt.After(jobs[j].UpdatedAt)
		}
		return jobs[i].JobID < jobs[j].JobID
	})
	if len(jobs) > maxTrackedJobsTotal {
		jobs = jobs[:maxTrackedJobsTotal]
	}

	if err := r.store.SaveTrackedJobs(jobs); err != nil {
		glog.Warningf("Plugin failed to persist tracked jobs: %v", err)
	}
}

func (r *Plugin) persistActivitiesSnapshot() {
	r.activitiesMu.RLock()
	activities := append([]JobActivity(nil), r.activities...)
	r.activitiesMu.RUnlock()

	if len(activities) > maxActivityRecords {
		activities = activities[len(activities)-maxActivityRecords:]
	}

	if err := r.store.SaveActivities(activities); err != nil {
		glog.Warningf("Plugin failed to persist activities: %v", err)
	}
}
