package plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var errExecutorAtCapacity = errors.New("executor is at capacity")

const (
	defaultSchedulerTick                       = 5 * time.Second
	defaultScheduledDetectionInterval          = 300 * time.Second
	defaultScheduledDetectionTimeout           = 45 * time.Second
	defaultScheduledExecutionTimeout           = 90 * time.Second
	defaultScheduledMaxResults           int32 = 200
	defaultScheduledExecutionConcurrency       = 1
	defaultScheduledPerWorkerConcurrency       = 1
	maxScheduledExecutionConcurrency           = 32
	defaultScheduledRetryBackoff               = 5 * time.Second
	defaultClusterContextTimeout               = 10 * time.Second
	defaultWaitingBacklogFloor                 = 8
	defaultWaitingBacklogMultiplier            = 4
)

type schedulerPolicy struct {
	DetectionInterval      time.Duration
	DetectionTimeout       time.Duration
	ExecutionTimeout       time.Duration
	RetryBackoff           time.Duration
	MaxResults             int32
	ExecutionConcurrency   int
	PerWorkerConcurrency   int
	RetryLimit             int
	ExecutorReserveBackoff time.Duration
}

func (r *Plugin) schedulerLoop() {
	ticker := time.NewTicker(r.schedulerTick)
	defer ticker.Stop()

	// Try once immediately on startup.
	r.runSchedulerTick()

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-ticker.C:
			r.runSchedulerTick()
		}
	}
}

func (r *Plugin) runSchedulerTick() {
	jobTypes := r.registry.DetectableJobTypes()
	if len(jobTypes) == 0 {
		return
	}

	active := make(map[string]struct{}, len(jobTypes))
	for _, jobType := range jobTypes {
		active[jobType] = struct{}{}

		policy, enabled, err := r.loadSchedulerPolicy(jobType)
		if err != nil {
			glog.Warningf("Plugin scheduler failed to load policy for %s: %v", jobType, err)
			continue
		}
		if !enabled {
			r.clearSchedulerJobType(jobType)
			continue
		}

		if !r.markDetectionDue(jobType, policy.DetectionInterval) {
			continue
		}

		go r.runScheduledDetection(jobType, policy)
	}

	r.pruneSchedulerState(active)
	r.pruneDetectorLeases(active)
}

func (r *Plugin) loadSchedulerPolicy(jobType string) (schedulerPolicy, bool, error) {
	cfg, err := r.store.LoadJobTypeConfig(jobType)
	if err != nil {
		return schedulerPolicy{}, false, err
	}
	descriptor, err := r.store.LoadDescriptor(jobType)
	if err != nil {
		return schedulerPolicy{}, false, err
	}

	adminRuntime := deriveSchedulerAdminRuntime(cfg, descriptor)
	if adminRuntime == nil {
		return schedulerPolicy{}, false, nil
	}
	if !adminRuntime.Enabled {
		return schedulerPolicy{}, false, nil
	}

	policy := schedulerPolicy{
		DetectionInterval:      durationFromSeconds(adminRuntime.DetectionIntervalSeconds, defaultScheduledDetectionInterval),
		DetectionTimeout:       durationFromSeconds(adminRuntime.DetectionTimeoutSeconds, defaultScheduledDetectionTimeout),
		ExecutionTimeout:       defaultScheduledExecutionTimeout,
		RetryBackoff:           durationFromSeconds(adminRuntime.RetryBackoffSeconds, defaultScheduledRetryBackoff),
		MaxResults:             adminRuntime.MaxJobsPerDetection,
		ExecutionConcurrency:   int(adminRuntime.GlobalExecutionConcurrency),
		PerWorkerConcurrency:   int(adminRuntime.PerWorkerExecutionConcurrency),
		RetryLimit:             int(adminRuntime.RetryLimit),
		ExecutorReserveBackoff: 200 * time.Millisecond,
	}

	if policy.DetectionInterval < r.schedulerTick {
		policy.DetectionInterval = r.schedulerTick
	}
	if policy.MaxResults <= 0 {
		policy.MaxResults = defaultScheduledMaxResults
	}
	if policy.ExecutionConcurrency <= 0 {
		policy.ExecutionConcurrency = defaultScheduledExecutionConcurrency
	}
	if policy.ExecutionConcurrency > maxScheduledExecutionConcurrency {
		policy.ExecutionConcurrency = maxScheduledExecutionConcurrency
	}
	if policy.PerWorkerConcurrency <= 0 {
		policy.PerWorkerConcurrency = defaultScheduledPerWorkerConcurrency
	}
	if policy.PerWorkerConcurrency > policy.ExecutionConcurrency {
		policy.PerWorkerConcurrency = policy.ExecutionConcurrency
	}
	if policy.RetryLimit < 0 {
		policy.RetryLimit = 0
	}

	// Plugin protocol currently has only detection timeout in admin settings.
	execTimeout := time.Duration(adminRuntime.DetectionTimeoutSeconds*2) * time.Second
	if execTimeout < defaultScheduledExecutionTimeout {
		execTimeout = defaultScheduledExecutionTimeout
	}
	policy.ExecutionTimeout = execTimeout

	return policy, true, nil
}

func (r *Plugin) ListSchedulerStates() ([]SchedulerJobTypeState, error) {
	jobTypes, err := r.ListKnownJobTypes()
	if err != nil {
		return nil, err
	}

	r.schedulerMu.Lock()
	nextDetectionAt := make(map[string]time.Time, len(r.nextDetectionAt))
	for jobType, nextRun := range r.nextDetectionAt {
		nextDetectionAt[jobType] = nextRun
	}
	detectionInFlight := make(map[string]bool, len(r.detectionInFlight))
	for jobType, inFlight := range r.detectionInFlight {
		detectionInFlight[jobType] = inFlight
	}
	r.schedulerMu.Unlock()

	states := make([]SchedulerJobTypeState, 0, len(jobTypes))
	for _, jobType := range jobTypes {
		state := SchedulerJobTypeState{
			JobType:           jobType,
			DetectionInFlight: detectionInFlight[jobType],
		}

		if nextRun, ok := nextDetectionAt[jobType]; ok && !nextRun.IsZero() {
			nextRunUTC := nextRun.UTC()
			state.NextDetectionAt = &nextRunUTC
		}

		policy, enabled, loadErr := r.loadSchedulerPolicy(jobType)
		if loadErr != nil {
			state.PolicyError = loadErr.Error()
		} else {
			state.Enabled = enabled
			if enabled {
				state.DetectionIntervalSeconds = secondsFromDuration(policy.DetectionInterval)
				state.DetectionTimeoutSeconds = secondsFromDuration(policy.DetectionTimeout)
				state.ExecutionTimeoutSeconds = secondsFromDuration(policy.ExecutionTimeout)
				state.MaxJobsPerDetection = policy.MaxResults
				state.GlobalExecutionConcurrency = policy.ExecutionConcurrency
				state.PerWorkerExecutionConcurrency = policy.PerWorkerConcurrency
				state.RetryLimit = policy.RetryLimit
				state.RetryBackoffSeconds = secondsFromDuration(policy.RetryBackoff)
			}
		}

		leasedWorkerID := r.getDetectorLease(jobType)
		if leasedWorkerID != "" {
			state.DetectorWorkerID = leasedWorkerID
			if worker, ok := r.registry.Get(leasedWorkerID); ok {
				if capability := worker.Capabilities[jobType]; capability != nil && capability.CanDetect {
					state.DetectorAvailable = true
				}
			}
		}
		if state.DetectorWorkerID == "" {
			detector, detectorErr := r.registry.PickDetector(jobType)
			if detectorErr == nil && detector != nil {
				state.DetectorAvailable = true
				state.DetectorWorkerID = detector.WorkerID
			}
		}

		executors, executorErr := r.registry.ListExecutors(jobType)
		if executorErr == nil {
			state.ExecutorWorkerCount = len(executors)
		}

		states = append(states, state)
	}

	return states, nil
}

func deriveSchedulerAdminRuntime(
	cfg *plugin_pb.PersistedJobTypeConfig,
	descriptor *plugin_pb.JobTypeDescriptor,
) *plugin_pb.AdminRuntimeConfig {
	if cfg != nil && cfg.AdminRuntime != nil {
		adminConfig := *cfg.AdminRuntime
		return &adminConfig
	}

	if descriptor == nil || descriptor.AdminRuntimeDefaults == nil {
		return nil
	}

	defaults := descriptor.AdminRuntimeDefaults
	return &plugin_pb.AdminRuntimeConfig{
		Enabled:                       defaults.Enabled,
		DetectionIntervalSeconds:      defaults.DetectionIntervalSeconds,
		DetectionTimeoutSeconds:       defaults.DetectionTimeoutSeconds,
		MaxJobsPerDetection:           defaults.MaxJobsPerDetection,
		GlobalExecutionConcurrency:    defaults.GlobalExecutionConcurrency,
		PerWorkerExecutionConcurrency: defaults.PerWorkerExecutionConcurrency,
		RetryLimit:                    defaults.RetryLimit,
		RetryBackoffSeconds:           defaults.RetryBackoffSeconds,
	}
}

func (r *Plugin) markDetectionDue(jobType string, interval time.Duration) bool {
	now := time.Now().UTC()

	r.schedulerMu.Lock()
	defer r.schedulerMu.Unlock()

	if r.detectionInFlight[jobType] {
		return false
	}

	nextRun, exists := r.nextDetectionAt[jobType]
	if exists && now.Before(nextRun) {
		return false
	}

	r.nextDetectionAt[jobType] = now.Add(interval)
	r.detectionInFlight[jobType] = true
	return true
}

func (r *Plugin) finishDetection(jobType string) {
	r.schedulerMu.Lock()
	delete(r.detectionInFlight, jobType)
	r.schedulerMu.Unlock()
}

func (r *Plugin) pruneSchedulerState(activeJobTypes map[string]struct{}) {
	r.schedulerMu.Lock()
	defer r.schedulerMu.Unlock()

	for jobType := range r.nextDetectionAt {
		if _, ok := activeJobTypes[jobType]; !ok {
			delete(r.nextDetectionAt, jobType)
			delete(r.detectionInFlight, jobType)
		}
	}
}

func (r *Plugin) clearSchedulerJobType(jobType string) {
	r.schedulerMu.Lock()
	delete(r.nextDetectionAt, jobType)
	delete(r.detectionInFlight, jobType)
	r.schedulerMu.Unlock()
	r.clearDetectorLease(jobType, "")
}

func (r *Plugin) pruneDetectorLeases(activeJobTypes map[string]struct{}) {
	r.detectorLeaseMu.Lock()
	defer r.detectorLeaseMu.Unlock()

	for jobType := range r.detectorLeases {
		if _, ok := activeJobTypes[jobType]; !ok {
			delete(r.detectorLeases, jobType)
		}
	}
}

func (r *Plugin) runScheduledDetection(jobType string, policy schedulerPolicy) {
	defer r.finishDetection(jobType)

	start := time.Now().UTC()
	r.appendActivity(JobActivity{
		JobType:    jobType,
		Source:     "admin_scheduler",
		Message:    "scheduled detection started",
		Stage:      "detecting",
		OccurredAt: start,
	})

	if skip, waitingCount, waitingThreshold := r.shouldSkipDetectionForWaitingJobs(jobType, policy); skip {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection skipped: waiting backlog %d reached threshold %d", waitingCount, waitingThreshold),
			Stage:      "skipped_waiting_backlog",
			OccurredAt: time.Now().UTC(),
		})
		return
	}

	clusterContext, err := r.loadSchedulerClusterContext()
	if err != nil {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection aborted: %v", err),
			Stage:      "failed",
			OccurredAt: time.Now().UTC(),
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), policy.DetectionTimeout)
	proposals, err := r.RunDetection(ctx, jobType, clusterContext, policy.MaxResults)
	cancel()
	if err != nil {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection failed: %v", err),
			Stage:      "failed",
			OccurredAt: time.Now().UTC(),
		})
		return
	}

	r.appendActivity(JobActivity{
		JobType:    jobType,
		Source:     "admin_scheduler",
		Message:    fmt.Sprintf("scheduled detection completed: %d proposal(s)", len(proposals)),
		Stage:      "detected",
		OccurredAt: time.Now().UTC(),
	})

	filteredByActive, skippedActive := r.filterProposalsWithActiveJobs(jobType, proposals)
	if skippedActive > 0 {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection skipped %d proposal(s) due to active assigned/running jobs", skippedActive),
			Stage:      "deduped_active_jobs",
			OccurredAt: time.Now().UTC(),
		})
	}

	if len(filteredByActive) == 0 {
		return
	}

	filtered := r.filterScheduledProposals(filteredByActive)
	if len(filtered) != len(filteredByActive) {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection deduped %d proposal(s) within this run", len(filteredByActive)-len(filtered)),
			Stage:      "deduped",
			OccurredAt: time.Now().UTC(),
		})
	}

	if len(filtered) == 0 {
		return
	}

	r.dispatchScheduledProposals(jobType, filtered, clusterContext, policy)
}

func (r *Plugin) loadSchedulerClusterContext() (*plugin_pb.ClusterContext, error) {
	if r.clusterContextProvider == nil {
		return nil, fmt.Errorf("cluster context provider is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultClusterContextTimeout)
	defer cancel()

	clusterContext, err := r.clusterContextProvider(ctx)
	if err != nil {
		return nil, err
	}
	if clusterContext == nil {
		return nil, fmt.Errorf("cluster context provider returned nil")
	}
	return clusterContext, nil
}

func (r *Plugin) dispatchScheduledProposals(
	jobType string,
	proposals []*plugin_pb.JobProposal,
	clusterContext *plugin_pb.ClusterContext,
	policy schedulerPolicy,
) {
	jobQueue := make(chan *plugin_pb.JobSpec, len(proposals))
	for index, proposal := range proposals {
		job := buildScheduledJobSpec(jobType, proposal, index)
		r.trackExecutionQueued(job)
		select {
		case <-r.shutdownCh:
			close(jobQueue)
			return
		default:
			jobQueue <- job
		}
	}
	close(jobQueue)

	var wg sync.WaitGroup
	var statsMu sync.Mutex
	successCount := 0
	errorCount := 0

	workerCount := policy.ExecutionConcurrency
	if workerCount < 1 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobQueue {
				select {
				case <-r.shutdownCh:
					return
				default:
				}

				for {
					executor, release, reserveErr := r.reserveScheduledExecutor(jobType, policy)
					if reserveErr != nil {
						if strings.Contains(strings.ToLower(reserveErr.Error()), "shutting down") {
							return
						}
						statsMu.Lock()
						errorCount++
						statsMu.Unlock()
						r.appendActivity(JobActivity{
							JobType:    jobType,
							Source:     "admin_scheduler",
							Message:    fmt.Sprintf("scheduled execution reservation failed: %v", reserveErr),
							Stage:      "failed",
							OccurredAt: time.Now().UTC(),
						})
						break
					}

					err := r.executeScheduledJobWithExecutor(executor, job, clusterContext, policy)
					release()
					if errors.Is(err, errExecutorAtCapacity) {
						r.trackExecutionQueued(job)
						if !waitForShutdownOrTimer(r.shutdownCh, policy.ExecutorReserveBackoff) {
							return
						}
						continue
					}
					if err != nil {
						statsMu.Lock()
						errorCount++
						statsMu.Unlock()
						r.appendActivity(JobActivity{
							JobID:      job.JobId,
							JobType:    job.JobType,
							Source:     "admin_scheduler",
							Message:    fmt.Sprintf("scheduled execution failed: %v", err),
							Stage:      "failed",
							OccurredAt: time.Now().UTC(),
						})
						break
					}

					statsMu.Lock()
					successCount++
					statsMu.Unlock()
					break
				}
			}
		}()
	}

	wg.Wait()

	r.appendActivity(JobActivity{
		JobType:    jobType,
		Source:     "admin_scheduler",
		Message:    fmt.Sprintf("scheduled execution finished: success=%d error=%d", successCount, errorCount),
		Stage:      "executed",
		OccurredAt: time.Now().UTC(),
	})
}

func (r *Plugin) reserveScheduledExecutor(
	jobType string,
	policy schedulerPolicy,
) (*WorkerSession, func(), error) {
	for {
		select {
		case <-r.shutdownCh:
			return nil, nil, fmt.Errorf("plugin is shutting down")
		default:
		}

		executors, err := r.registry.ListExecutors(jobType)
		if err != nil {
			if !waitForShutdownOrTimer(r.shutdownCh, policy.ExecutorReserveBackoff) {
				return nil, nil, fmt.Errorf("plugin is shutting down")
			}
			continue
		}

		for _, executor := range executors {
			release, ok := r.tryReserveExecutorCapacity(executor, jobType, policy)
			if !ok {
				continue
			}
			return executor, release, nil
		}

		if !waitForShutdownOrTimer(r.shutdownCh, policy.ExecutorReserveBackoff) {
			return nil, nil, fmt.Errorf("plugin is shutting down")
		}
	}
}

func (r *Plugin) tryReserveExecutorCapacity(
	executor *WorkerSession,
	jobType string,
	policy schedulerPolicy,
) (func(), bool) {
	if executor == nil || strings.TrimSpace(executor.WorkerID) == "" {
		return nil, false
	}

	limit := schedulerWorkerExecutionLimit(executor, jobType, policy)
	if limit <= 0 {
		return nil, false
	}
	heartbeatUsed := 0
	if executor.Heartbeat != nil && executor.Heartbeat.ExecutionSlotsUsed > 0 {
		heartbeatUsed = int(executor.Heartbeat.ExecutionSlotsUsed)
	}

	workerID := strings.TrimSpace(executor.WorkerID)

	r.schedulerExecMu.Lock()
	reserved := r.schedulerExecReservations[workerID]
	if heartbeatUsed+reserved >= limit {
		r.schedulerExecMu.Unlock()
		return nil, false
	}
	r.schedulerExecReservations[workerID] = reserved + 1
	r.schedulerExecMu.Unlock()

	release := func() {
		r.releaseExecutorCapacity(workerID)
	}
	return release, true
}

func (r *Plugin) releaseExecutorCapacity(workerID string) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return
	}

	r.schedulerExecMu.Lock()
	defer r.schedulerExecMu.Unlock()

	current := r.schedulerExecReservations[workerID]
	if current <= 1 {
		delete(r.schedulerExecReservations, workerID)
		return
	}
	r.schedulerExecReservations[workerID] = current - 1
}

func schedulerWorkerExecutionLimit(executor *WorkerSession, jobType string, policy schedulerPolicy) int {
	limit := policy.PerWorkerConcurrency
	if limit <= 0 {
		limit = defaultScheduledPerWorkerConcurrency
	}

	if capability := executor.Capabilities[jobType]; capability != nil && capability.MaxExecutionConcurrency > 0 {
		capLimit := int(capability.MaxExecutionConcurrency)
		if capLimit < limit {
			limit = capLimit
		}
	}

	if executor.Heartbeat != nil && executor.Heartbeat.ExecutionSlotsTotal > 0 {
		heartbeatLimit := int(executor.Heartbeat.ExecutionSlotsTotal)
		if heartbeatLimit < limit {
			limit = heartbeatLimit
		}
	}

	if limit < 0 {
		return 0
	}
	return limit
}

func (r *Plugin) executeScheduledJobWithExecutor(
	executor *WorkerSession,
	job *plugin_pb.JobSpec,
	clusterContext *plugin_pb.ClusterContext,
	policy schedulerPolicy,
) error {
	maxAttempts := policy.RetryLimit + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-r.shutdownCh:
			return fmt.Errorf("plugin is shutting down")
		default:
		}

		execCtx, cancel := context.WithTimeout(context.Background(), policy.ExecutionTimeout)
		_, err := r.executeJobWithExecutor(execCtx, executor, job, clusterContext, int32(attempt))
		cancel()
		if err == nil {
			return nil
		}
		if isExecutorAtCapacityError(err) {
			return errExecutorAtCapacity
		}
		lastErr = err

		if attempt < maxAttempts {
			r.appendActivity(JobActivity{
				JobID:      job.JobId,
				JobType:    job.JobType,
				Source:     "admin_scheduler",
				Message:    fmt.Sprintf("retrying job attempt %d/%d after error: %v", attempt, maxAttempts, err),
				Stage:      "retry",
				OccurredAt: time.Now().UTC(),
			})
			if !waitForShutdownOrTimer(r.shutdownCh, policy.RetryBackoff) {
				return fmt.Errorf("plugin is shutting down")
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("execution failed without an explicit error")
	}
	return lastErr
}

func (r *Plugin) shouldSkipDetectionForWaitingJobs(jobType string, policy schedulerPolicy) (bool, int, int) {
	waitingCount := r.countWaitingTrackedJobs(jobType)
	threshold := waitingBacklogThreshold(policy)
	if threshold <= 0 {
		return false, waitingCount, threshold
	}
	return waitingCount >= threshold, waitingCount, threshold
}

func (r *Plugin) countWaitingTrackedJobs(jobType string) int {
	normalizedJobType := strings.TrimSpace(jobType)
	if normalizedJobType == "" {
		return 0
	}

	waiting := 0
	r.jobsMu.RLock()
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if strings.TrimSpace(job.JobType) != normalizedJobType {
			continue
		}
		if !isWaitingTrackedJobState(job.State) {
			continue
		}
		waiting++
	}
	r.jobsMu.RUnlock()

	return waiting
}

func waitingBacklogThreshold(policy schedulerPolicy) int {
	concurrency := policy.ExecutionConcurrency
	if concurrency <= 0 {
		concurrency = defaultScheduledExecutionConcurrency
	}
	threshold := concurrency * defaultWaitingBacklogMultiplier
	if threshold < defaultWaitingBacklogFloor {
		threshold = defaultWaitingBacklogFloor
	}
	if policy.MaxResults > 0 && threshold > int(policy.MaxResults) {
		threshold = int(policy.MaxResults)
	}
	return threshold
}

func isExecutorAtCapacityError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errExecutorAtCapacity) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "executor is at capacity")
}

func buildScheduledJobSpec(jobType string, proposal *plugin_pb.JobProposal, index int) *plugin_pb.JobSpec {
	now := timestamppb.Now()

	jobID := fmt.Sprintf("%s-scheduled-%d-%d", jobType, now.AsTime().UnixNano(), index)
	if proposal != nil && proposal.ProposalId != "" {
		jobID = proposal.ProposalId
	}

	job := &plugin_pb.JobSpec{
		JobId:       jobID,
		JobType:     jobType,
		Priority:    plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Parameters:  map[string]*plugin_pb.ConfigValue{},
		Labels:      map[string]string{},
		CreatedAt:   now,
		ScheduledAt: now,
	}

	if proposal == nil {
		return job
	}

	if proposal.JobType != "" {
		job.JobType = proposal.JobType
	}
	job.DedupeKey = proposal.DedupeKey
	job.Priority = proposal.Priority
	job.Summary = proposal.Summary
	job.Detail = proposal.Detail

	if proposal.Parameters != nil {
		job.Parameters = cloneConfigValueMap(proposal.Parameters)
	}
	if proposal.Labels != nil {
		job.Labels = make(map[string]string, len(proposal.Labels))
		for key, value := range proposal.Labels {
			job.Labels[key] = value
		}
	}
	if proposal.NotBefore != nil {
		job.ScheduledAt = proposal.NotBefore
	}

	return job
}

func durationFromSeconds(seconds int32, defaultValue time.Duration) time.Duration {
	if seconds <= 0 {
		return defaultValue
	}
	return time.Duration(seconds) * time.Second
}

func secondsFromDuration(duration time.Duration) int32 {
	if duration <= 0 {
		return 0
	}
	return int32(duration / time.Second)
}

func waitForShutdownOrTimer(shutdown <-chan struct{}, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-shutdown:
		return false
	case <-timer.C:
		return true
	}
}

func (r *Plugin) filterProposalsWithActiveJobs(jobType string, proposals []*plugin_pb.JobProposal) ([]*plugin_pb.JobProposal, int) {
	if len(proposals) == 0 {
		return proposals, 0
	}

	activeKeys := make(map[string]struct{})
	r.jobsMu.RLock()
	for _, job := range r.jobs {
		if job == nil {
			continue
		}
		if strings.TrimSpace(job.JobType) != strings.TrimSpace(jobType) {
			continue
		}
		if !isActiveTrackedJobState(job.State) {
			continue
		}

		key := strings.TrimSpace(job.DedupeKey)
		if key == "" {
			key = strings.TrimSpace(job.JobID)
		}
		if key == "" {
			continue
		}
		activeKeys[key] = struct{}{}
	}
	r.jobsMu.RUnlock()

	if len(activeKeys) == 0 {
		return proposals, 0
	}

	filtered := make([]*plugin_pb.JobProposal, 0, len(proposals))
	skipped := 0
	for _, proposal := range proposals {
		if proposal == nil {
			continue
		}
		key := proposalExecutionKey(proposal)
		if key != "" {
			if _, exists := activeKeys[key]; exists {
				skipped++
				continue
			}
		}
		filtered = append(filtered, proposal)
	}

	return filtered, skipped
}

func proposalExecutionKey(proposal *plugin_pb.JobProposal) string {
	if proposal == nil {
		return ""
	}
	key := strings.TrimSpace(proposal.DedupeKey)
	if key != "" {
		return key
	}
	return strings.TrimSpace(proposal.ProposalId)
}

func isActiveTrackedJobState(state string) bool {
	normalized := strings.ToLower(strings.TrimSpace(state))
	switch normalized {
	case "pending", "assigned", "running", "in_progress", "job_state_pending", "job_state_assigned", "job_state_running":
		return true
	default:
		return false
	}
}

func isWaitingTrackedJobState(state string) bool {
	normalized := strings.ToLower(strings.TrimSpace(state))
	return normalized == "pending" || normalized == "job_state_pending"
}

func (r *Plugin) filterScheduledProposals(proposals []*plugin_pb.JobProposal) []*plugin_pb.JobProposal {
	filtered := make([]*plugin_pb.JobProposal, 0, len(proposals))
	seenInRun := make(map[string]struct{}, len(proposals))

	for _, proposal := range proposals {
		if proposal == nil {
			continue
		}

		key := proposal.DedupeKey
		if key == "" {
			key = proposal.ProposalId
		}
		if key == "" {
			filtered = append(filtered, proposal)
			continue
		}

		if _, exists := seenInRun[key]; exists {
			continue
		}

		seenInRun[key] = struct{}{}
		filtered = append(filtered, proposal)
	}

	return filtered
}
