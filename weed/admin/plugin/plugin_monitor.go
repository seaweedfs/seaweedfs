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
			// Backward compatibility: migrate older inline detail payloads
			// out of tracked_jobs.json into dedicated per-job detail files.
			if hasTrackedJobRichDetails(job) {
				if err := r.store.SaveJobDetail(job); err != nil {
					glog.Warningf("Plugin failed to migrate detail snapshot for job %s: %v", job.JobID, err)
				}
			}
			stripTrackedJobDetailFields(&job)
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
		items = append(items, cloneTrackedJob(*job))
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
	clone := cloneTrackedJob(*job)
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

func (r *Plugin) ListJobActivities(jobID string, limit int) []JobActivity {
	normalizedJobID := strings.TrimSpace(jobID)
	if normalizedJobID == "" {
		return nil
	}

	r.activitiesMu.RLock()
	activities := make([]JobActivity, 0, len(r.activities))
	for _, activity := range r.activities {
		if strings.TrimSpace(activity.JobID) != normalizedJobID {
			continue
		}
		activities = append(activities, activity)
	}
	r.activitiesMu.RUnlock()

	sort.Slice(activities, func(i, j int) bool {
		return activities[i].OccurredAt.Before(activities[j].OccurredAt)
	})
	if limit > 0 && len(activities) > limit {
		activities = activities[len(activities)-limit:]
	}
	return activities
}

func (r *Plugin) BuildJobDetail(jobID string, activityLimit int, relatedLimit int) (*JobDetail, bool, error) {
	normalizedJobID := strings.TrimSpace(jobID)
	if normalizedJobID == "" {
		return nil, false, nil
	}

	trackedJobs, err := r.store.LoadTrackedJobs()
	if err != nil {
		return nil, false, err
	}

	var trackedSnapshot *TrackedJob
	for i := range trackedJobs {
		if strings.TrimSpace(trackedJobs[i].JobID) != normalizedJobID {
			continue
		}
		candidate := cloneTrackedJob(trackedJobs[i])
		stripTrackedJobDetailFields(&candidate)
		trackedSnapshot = &candidate
		break
	}

	detailJob, err := r.store.LoadJobDetail(normalizedJobID)
	if err != nil {
		return nil, false, err
	}

	if trackedSnapshot == nil && detailJob == nil {
		return nil, false, nil
	}
	if detailJob == nil && trackedSnapshot != nil {
		clone := cloneTrackedJob(*trackedSnapshot)
		detailJob = &clone
	}
	if detailJob == nil {
		return nil, false, nil
	}
	if trackedSnapshot != nil {
		mergeTrackedStatusIntoDetail(detailJob, trackedSnapshot)
	}

	activities, err := r.store.LoadActivities()
	if err != nil {
		return nil, true, err
	}

	detail := &JobDetail{
		Job:         detailJob,
		Activities:  filterJobActivitiesFromSlice(activities, normalizedJobID, activityLimit),
		LastUpdated: time.Now().UTC(),
	}

	if history, err := r.store.LoadRunHistory(detailJob.JobType); err != nil {
		return nil, true, err
	} else if history != nil {
		for i := range history.SuccessfulRuns {
			record := history.SuccessfulRuns[i]
			if strings.TrimSpace(record.JobID) == normalizedJobID {
				recordCopy := record
				detail.RunRecord = &recordCopy
				break
			}
		}
		if detail.RunRecord == nil {
			for i := range history.ErrorRuns {
				record := history.ErrorRuns[i]
				if strings.TrimSpace(record.JobID) == normalizedJobID {
					recordCopy := record
					detail.RunRecord = &recordCopy
					break
				}
			}
		}
	}

	if relatedLimit > 0 {
		related := make([]TrackedJob, 0, relatedLimit)
		for i := range trackedJobs {
			candidate := cloneTrackedJob(trackedJobs[i])
			if strings.TrimSpace(candidate.JobType) != strings.TrimSpace(detailJob.JobType) {
				continue
			}
			if strings.TrimSpace(candidate.JobID) == normalizedJobID {
				continue
			}
			stripTrackedJobDetailFields(&candidate)
			related = append(related, candidate)
			if len(related) >= relatedLimit {
				break
			}
		}
		detail.RelatedJobs = related
	}

	return detail, true, nil
}

func filterJobActivitiesFromSlice(all []JobActivity, jobID string, limit int) []JobActivity {
	if strings.TrimSpace(jobID) == "" || len(all) == 0 {
		return nil
	}

	activities := make([]JobActivity, 0, len(all))
	for _, activity := range all {
		if strings.TrimSpace(activity.JobID) != jobID {
			continue
		}
		activities = append(activities, activity)
	}

	sort.Slice(activities, func(i, j int) bool {
		return activities[i].OccurredAt.Before(activities[j].OccurredAt)
	})
	if limit > 0 && len(activities) > limit {
		activities = activities[len(activities)-limit:]
	}
	return activities
}

func stripTrackedJobDetailFields(job *TrackedJob) {
	if job == nil {
		return
	}
	job.Detail = ""
	job.Parameters = nil
	job.Labels = nil
	job.ResultOutputValues = nil
}

func hasTrackedJobRichDetails(job TrackedJob) bool {
	return strings.TrimSpace(job.Detail) != "" ||
		len(job.Parameters) > 0 ||
		len(job.Labels) > 0 ||
		len(job.ResultOutputValues) > 0
}

func mergeTrackedStatusIntoDetail(detail *TrackedJob, tracked *TrackedJob) {
	if detail == nil || tracked == nil {
		return
	}

	if detail.JobType == "" {
		detail.JobType = tracked.JobType
	}
	if detail.RequestID == "" {
		detail.RequestID = tracked.RequestID
	}
	if detail.WorkerID == "" {
		detail.WorkerID = tracked.WorkerID
	}
	if detail.DedupeKey == "" {
		detail.DedupeKey = tracked.DedupeKey
	}
	if detail.Summary == "" {
		detail.Summary = tracked.Summary
	}
	if detail.State == "" {
		detail.State = tracked.State
	}
	if detail.Progress == 0 {
		detail.Progress = tracked.Progress
	}
	if detail.Stage == "" {
		detail.Stage = tracked.Stage
	}
	if detail.Message == "" {
		detail.Message = tracked.Message
	}
	if detail.Attempt == 0 {
		detail.Attempt = tracked.Attempt
	}
	if detail.CreatedAt.IsZero() {
		detail.CreatedAt = tracked.CreatedAt
	}
	if detail.UpdatedAt.IsZero() {
		detail.UpdatedAt = tracked.UpdatedAt
	}
	if detail.CompletedAt.IsZero() {
		detail.CompletedAt = tracked.CompletedAt
	}
	if detail.ErrorMessage == "" {
		detail.ErrorMessage = tracked.ErrorMessage
	}
	if detail.ResultSummary == "" {
		detail.ResultSummary = tracked.ResultSummary
	}
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
	trackedSnapshot := cloneTrackedJob(*tracked)
	r.pruneTrackedJobsLocked()
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()
	r.persistJobDetailSnapshot(job.JobId, func(detail *TrackedJob) {
		detail.JobID = job.JobId
		detail.JobType = job.JobType
		detail.RequestID = requestID
		detail.WorkerID = workerID
		detail.DedupeKey = job.DedupeKey
		detail.Summary = job.Summary
		detail.Detail = job.Detail
		detail.Parameters = configValueMapToPlain(job.Parameters)
		if len(job.Labels) > 0 {
			labels := make(map[string]string, len(job.Labels))
			for key, value := range job.Labels {
				labels[key] = value
			}
			detail.Labels = labels
		} else {
			detail.Labels = nil
		}
		detail.State = trackedSnapshot.State
		detail.Progress = trackedSnapshot.Progress
		detail.Stage = trackedSnapshot.Stage
		detail.Message = trackedSnapshot.Message
		detail.Attempt = attempt
		if detail.CreatedAt.IsZero() {
			detail.CreatedAt = trackedSnapshot.CreatedAt
		}
		detail.UpdatedAt = trackedSnapshot.UpdatedAt
	})

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
	trackedSnapshot := cloneTrackedJob(*tracked)
	r.pruneTrackedJobsLocked()
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()
	r.persistJobDetailSnapshot(job.JobId, func(detail *TrackedJob) {
		detail.JobID = job.JobId
		detail.JobType = job.JobType
		detail.DedupeKey = job.DedupeKey
		detail.Summary = job.Summary
		detail.Detail = job.Detail
		detail.Parameters = configValueMapToPlain(job.Parameters)
		if len(job.Labels) > 0 {
			labels := make(map[string]string, len(job.Labels))
			for key, value := range job.Labels {
				labels[key] = value
			}
			detail.Labels = labels
		} else {
			detail.Labels = nil
		}
		detail.State = trackedSnapshot.State
		detail.Progress = trackedSnapshot.Progress
		detail.Stage = trackedSnapshot.Stage
		detail.Message = trackedSnapshot.Message
		if detail.CreatedAt.IsZero() {
			detail.CreatedAt = trackedSnapshot.CreatedAt
		}
		detail.UpdatedAt = trackedSnapshot.UpdatedAt
	})

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
	clone := cloneTrackedJob(*tracked)
	r.jobsMu.Unlock()
	r.persistTrackedJobsSnapshot()
	r.persistJobDetailSnapshot(completed.JobId, func(detail *TrackedJob) {
		detail.JobID = completed.JobId
		if completed.JobType != "" {
			detail.JobType = completed.JobType
		}
		if completed.RequestId != "" {
			detail.RequestID = completed.RequestId
		}
		detail.State = clone.State
		detail.Progress = clone.Progress
		detail.Stage = clone.Stage
		detail.Message = clone.Message
		detail.ErrorMessage = clone.ErrorMessage
		detail.ResultSummary = clone.ResultSummary
		if completed.Success && completed.Result != nil {
			detail.ResultOutputValues = configValueMapToPlain(completed.Result.OutputValues)
		} else {
			detail.ResultOutputValues = nil
		}
		if detail.CreatedAt.IsZero() {
			detail.CreatedAt = clone.CreatedAt
		}
		detail.UpdatedAt = clone.UpdatedAt
		detail.CompletedAt = clone.CompletedAt
	})

	r.appendActivity(JobActivity{
		JobID:      completed.JobId,
		JobType:    completed.JobType,
		RequestID:  completed.RequestId,
		WorkerID:   clone.WorkerID,
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
		clone := cloneTrackedJob(*job)
		stripTrackedJobDetailFields(&clone)
		jobs = append(jobs, clone)
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

func (r *Plugin) persistJobDetailSnapshot(jobID string, apply func(detail *TrackedJob)) {
	normalizedJobID := strings.TrimSpace(jobID)
	if normalizedJobID == "" {
		return
	}

	detail, err := r.store.LoadJobDetail(normalizedJobID)
	if err != nil {
		glog.Warningf("Plugin failed to load job detail snapshot for %s: %v", normalizedJobID, err)
		return
	}
	if detail == nil {
		detail = &TrackedJob{JobID: normalizedJobID}
	}
	if detail.JobID == "" {
		detail.JobID = normalizedJobID
	}
	if apply != nil {
		apply(detail)
	}
	if detail.UpdatedAt.IsZero() {
		detail.UpdatedAt = time.Now().UTC()
	}
	if err := r.store.SaveJobDetail(*detail); err != nil {
		glog.Warningf("Plugin failed to persist job detail snapshot for %s: %v", normalizedJobID, err)
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
