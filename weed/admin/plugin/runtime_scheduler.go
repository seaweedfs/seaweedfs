package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	defaultScheduledDedupeTTL                  = 24 * time.Hour
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

func (r *Runtime) schedulerLoop() {
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

func (r *Runtime) runSchedulerTick() {
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
}

func (r *Runtime) loadSchedulerPolicy(jobType string) (schedulerPolicy, bool, error) {
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

	// Plugin protocol currently has only detection timeout in runtime config.
	execTimeout := time.Duration(adminRuntime.DetectionTimeoutSeconds*2) * time.Second
	if execTimeout < defaultScheduledExecutionTimeout {
		execTimeout = defaultScheduledExecutionTimeout
	}
	policy.ExecutionTimeout = execTimeout

	return policy, true, nil
}

func deriveSchedulerAdminRuntime(
	cfg *plugin_pb.PersistedJobTypeConfig,
	descriptor *plugin_pb.JobTypeDescriptor,
) *plugin_pb.AdminRuntimeConfig {
	if cfg != nil && cfg.AdminRuntime != nil {
		runtime := *cfg.AdminRuntime
		return &runtime
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

func (r *Runtime) markDetectionDue(jobType string, interval time.Duration) bool {
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

func (r *Runtime) finishDetection(jobType string) {
	r.schedulerMu.Lock()
	delete(r.detectionInFlight, jobType)
	r.schedulerMu.Unlock()
}

func (r *Runtime) pruneSchedulerState(activeJobTypes map[string]struct{}) {
	r.schedulerMu.Lock()
	defer r.schedulerMu.Unlock()

	for jobType := range r.nextDetectionAt {
		if _, ok := activeJobTypes[jobType]; !ok {
			delete(r.nextDetectionAt, jobType)
			delete(r.detectionInFlight, jobType)
		}
	}
}

func (r *Runtime) clearSchedulerJobType(jobType string) {
	r.schedulerMu.Lock()
	delete(r.nextDetectionAt, jobType)
	delete(r.detectionInFlight, jobType)
	r.schedulerMu.Unlock()
}

func (r *Runtime) runScheduledDetection(jobType string, policy schedulerPolicy) {
	defer r.finishDetection(jobType)

	start := time.Now().UTC()
	r.appendActivity(JobActivity{
		JobType:    jobType,
		Source:     "admin_scheduler",
		Message:    "scheduled detection started",
		Stage:      "detecting",
		OccurredAt: start,
	})

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

	filtered := r.filterScheduledProposals(jobType, proposals, defaultScheduledDedupeTTL)
	if len(filtered) != len(proposals) {
		r.appendActivity(JobActivity{
			JobType:    jobType,
			Source:     "admin_scheduler",
			Message:    fmt.Sprintf("scheduled detection deduped %d proposal(s)", len(proposals)-len(filtered)),
			Stage:      "deduped",
			OccurredAt: time.Now().UTC(),
		})
	}

	if len(filtered) == 0 {
		return
	}

	r.dispatchScheduledProposals(jobType, filtered, clusterContext, policy)
}

func (r *Runtime) loadSchedulerClusterContext() (*plugin_pb.ClusterContext, error) {
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

func (r *Runtime) dispatchScheduledProposals(
	jobType string,
	proposals []*plugin_pb.JobProposal,
	clusterContext *plugin_pb.ClusterContext,
	policy schedulerPolicy,
) {
	type scheduledProposal struct {
		index    int
		proposal *plugin_pb.JobProposal
	}

	jobQueue := make(chan scheduledProposal, len(proposals))
	for index, proposal := range proposals {
		select {
		case <-r.shutdownCh:
			close(jobQueue)
			return
		default:
			jobQueue <- scheduledProposal{index: index, proposal: proposal}
		}
	}
	close(jobQueue)

	var limiterMu sync.Mutex
	workerLimiters := make(map[string]chan struct{})

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

			for item := range jobQueue {
				select {
				case <-r.shutdownCh:
					return
				default:
				}

				executor, release, reserveErr := r.reserveScheduledExecutor(jobType, workerLimiters, &limiterMu, policy)
				if reserveErr != nil {
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
					continue
				}

				job := buildScheduledJobSpec(jobType, item.proposal, item.index)
				err := r.executeScheduledJobWithExecutor(executor, job, clusterContext, policy)
				release()

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
					continue
				}

				statsMu.Lock()
				successCount++
				statsMu.Unlock()
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

func (r *Runtime) reserveScheduledExecutor(
	jobType string,
	workerLimiters map[string]chan struct{},
	limiterMu *sync.Mutex,
	policy schedulerPolicy,
) (*WorkerSession, func(), error) {
	reserveDeadline := time.Now().Add(policy.ExecutionTimeout)

	for {
		select {
		case <-r.shutdownCh:
			return nil, nil, fmt.Errorf("plugin runtime is shutting down")
		default:
		}

		executors, err := r.registry.ListExecutors(jobType)
		if err != nil {
			if time.Now().After(reserveDeadline) {
				return nil, nil, err
			}
			if !waitForShutdownOrTimer(r.shutdownCh, policy.ExecutorReserveBackoff) {
				return nil, nil, fmt.Errorf("plugin runtime is shutting down")
			}
			continue
		}

		limiterMu.Lock()
		for _, executor := range executors {
			workerLimiter := workerLimiters[executor.WorkerID]
			if workerLimiter == nil {
				workerLimiter = make(chan struct{}, policy.PerWorkerConcurrency)
				workerLimiters[executor.WorkerID] = workerLimiter
			}

			select {
			case workerLimiter <- struct{}{}:
				limiterMu.Unlock()
				release := func() { <-workerLimiter }
				return executor, release, nil
			default:
			}
		}
		limiterMu.Unlock()

		if time.Now().After(reserveDeadline) {
			return nil, nil, fmt.Errorf("no executor slot became available for job_type=%s", jobType)
		}
		if !waitForShutdownOrTimer(r.shutdownCh, policy.ExecutorReserveBackoff) {
			return nil, nil, fmt.Errorf("plugin runtime is shutting down")
		}
	}
}

func (r *Runtime) executeScheduledJobWithExecutor(
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
			return fmt.Errorf("plugin runtime is shutting down")
		default:
		}

		execCtx, cancel := context.WithTimeout(context.Background(), policy.ExecutionTimeout)
		_, err := r.executeJobWithExecutor(execCtx, executor, job, clusterContext, int32(attempt))
		cancel()
		if err == nil {
			return nil
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
				return fmt.Errorf("plugin runtime is shutting down")
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("execution failed without an explicit error")
	}
	return lastErr
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

func (r *Runtime) filterScheduledProposals(jobType string, proposals []*plugin_pb.JobProposal, dedupeTTL time.Duration) []*plugin_pb.JobProposal {
	now := time.Now().UTC()
	if dedupeTTL <= 0 {
		dedupeTTL = defaultScheduledDedupeTTL
	}

	r.dedupeMu.Lock()
	defer r.dedupeMu.Unlock()

	typedCache := r.recentDedupeByType[jobType]
	if typedCache == nil {
		typedCache = make(map[string]time.Time)
		r.recentDedupeByType[jobType] = typedCache
	}

	cutoff := now.Add(-dedupeTTL)
	for dedupeKey, seenAt := range typedCache {
		if seenAt.Before(cutoff) {
			delete(typedCache, dedupeKey)
		}
	}

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
		if _, exists := typedCache[key]; exists {
			continue
		}

		seenInRun[key] = struct{}{}
		typedCache[key] = now
		filtered = append(filtered, proposal)
	}

	return filtered
}
