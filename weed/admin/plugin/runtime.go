package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultOutgoingBuffer      = 128
	defaultSendTimeout         = 5 * time.Second
	defaultHeartbeatInterval   = 30
	defaultReconnectDelay      = 5
	defaultPendingSchemaBuffer = 1
)

type RuntimeOptions struct {
	DataDir                string
	OutgoingBufferSize     int
	SendTimeout            time.Duration
	SchedulerTick          time.Duration
	ClusterContextProvider func(context.Context) (*plugin_pb.ClusterContext, error)
}

type Runtime struct {
	plugin_pb.UnimplementedPluginControlServiceServer

	store    *ConfigStore
	registry *Registry

	outgoingBuffer int
	sendTimeout    time.Duration

	schedulerTick          time.Duration
	clusterContextProvider func(context.Context) (*plugin_pb.ClusterContext, error)

	schedulerMu       sync.Mutex
	nextDetectionAt   map[string]time.Time
	detectionInFlight map[string]bool

	dedupeMu           sync.Mutex
	recentDedupeByType map[string]map[string]time.Time

	sessionsMu sync.RWMutex
	sessions   map[string]*streamSession

	pendingSchemaMu sync.Mutex
	pendingSchema   map[string]chan *plugin_pb.ConfigSchemaResponse

	pendingDetectionMu sync.Mutex
	pendingDetection   map[string]*pendingDetectionState

	pendingExecutionMu sync.Mutex
	pendingExecution   map[string]chan *plugin_pb.JobCompleted

	jobsMu sync.RWMutex
	jobs   map[string]*TrackedJob

	activitiesMu sync.RWMutex
	activities   []JobActivity

	shutdownCh chan struct{}
}

type streamSession struct {
	workerID  string
	outgoing  chan *plugin_pb.AdminToWorkerMessage
	closeOnce sync.Once
}

type pendingDetectionState struct {
	proposals []*plugin_pb.JobProposal
	complete  chan *plugin_pb.DetectionComplete
}

func NewRuntime(options RuntimeOptions) (*Runtime, error) {
	store, err := NewConfigStore(options.DataDir)
	if err != nil {
		return nil, err
	}

	bufferSize := options.OutgoingBufferSize
	if bufferSize <= 0 {
		bufferSize = defaultOutgoingBuffer
	}
	sendTimeout := options.SendTimeout
	if sendTimeout <= 0 {
		sendTimeout = defaultSendTimeout
	}
	schedulerTick := options.SchedulerTick
	if schedulerTick <= 0 {
		schedulerTick = defaultSchedulerTick
	}

	runtime := &Runtime{
		store:                  store,
		registry:               NewRegistry(),
		outgoingBuffer:         bufferSize,
		sendTimeout:            sendTimeout,
		schedulerTick:          schedulerTick,
		clusterContextProvider: options.ClusterContextProvider,
		sessions:               make(map[string]*streamSession),
		pendingSchema:          make(map[string]chan *plugin_pb.ConfigSchemaResponse),
		pendingDetection:       make(map[string]*pendingDetectionState),
		pendingExecution:       make(map[string]chan *plugin_pb.JobCompleted),
		nextDetectionAt:        make(map[string]time.Time),
		detectionInFlight:      make(map[string]bool),
		recentDedupeByType:     make(map[string]map[string]time.Time),
		jobs:                   make(map[string]*TrackedJob),
		activities:             make([]JobActivity, 0, 256),
		shutdownCh:             make(chan struct{}),
	}

	if runtime.clusterContextProvider != nil {
		go runtime.schedulerLoop()
	}

	return runtime, nil
}

func (r *Runtime) Shutdown() {
	select {
	case <-r.shutdownCh:
		return
	default:
		close(r.shutdownCh)
	}

	r.sessionsMu.Lock()
	for workerID, session := range r.sessions {
		session.close()
		delete(r.sessions, workerID)
	}
	r.sessionsMu.Unlock()

	r.pendingSchemaMu.Lock()
	for requestID, ch := range r.pendingSchema {
		close(ch)
		delete(r.pendingSchema, requestID)
	}
	r.pendingSchemaMu.Unlock()

	r.pendingDetectionMu.Lock()
	for requestID, state := range r.pendingDetection {
		close(state.complete)
		delete(r.pendingDetection, requestID)
	}
	r.pendingDetectionMu.Unlock()

	r.pendingExecutionMu.Lock()
	for requestID, ch := range r.pendingExecution {
		close(ch)
		delete(r.pendingExecution, requestID)
	}
	r.pendingExecutionMu.Unlock()
}

func (r *Runtime) WorkerStream(stream plugin_pb.PluginControlService_WorkerStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("receive worker hello: %w", err)
	}

	hello := first.GetHello()
	if hello == nil {
		return fmt.Errorf("first message must be hello")
	}
	if strings.TrimSpace(hello.WorkerId) == "" {
		return fmt.Errorf("worker_id is required")
	}

	workerID := hello.WorkerId
	r.registry.UpsertFromHello(hello)

	session := &streamSession{
		workerID: workerID,
		outgoing: make(chan *plugin_pb.AdminToWorkerMessage, r.outgoingBuffer),
	}
	r.putSession(session)
	defer r.cleanupSession(workerID)

	glog.V(0).Infof("Plugin worker connected: %s (%s)", workerID, hello.Address)

	sendErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- r.sendLoop(stream.Context(), stream, session)
	}()

	if err := r.sendAdminHello(workerID); err != nil {
		glog.Warningf("failed to send plugin admin hello to %s: %v", workerID, err)
	}
	go r.prefetchDescriptorsFromHello(hello)

	for {
		select {
		case <-r.shutdownCh:
			return nil
		case err := <-sendErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		default:
		}

		message, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return nil
			}
			if errors.Is(recvErr, context.Canceled) {
				return nil
			}
			return fmt.Errorf("receive plugin message from %s: %w", workerID, recvErr)
		}

		r.handleWorkerMessage(workerID, message)
	}
}

func (r *Runtime) RequestConfigSchema(ctx context.Context, jobType string, forceRefresh bool) (*plugin_pb.JobTypeDescriptor, error) {
	if !forceRefresh {
		descriptor, err := r.store.LoadDescriptor(jobType)
		if err != nil {
			return nil, err
		}
		if descriptor != nil {
			return descriptor, nil
		}
	}

	provider, err := r.registry.PickSchemaProvider(jobType)
	if err != nil {
		return nil, err
	}

	requestID, err := newRequestID("schema")
	if err != nil {
		return nil, err
	}

	responseCh := make(chan *plugin_pb.ConfigSchemaResponse, defaultPendingSchemaBuffer)
	r.pendingSchemaMu.Lock()
	r.pendingSchema[requestID] = responseCh
	r.pendingSchemaMu.Unlock()
	defer func() {
		r.pendingSchemaMu.Lock()
		delete(r.pendingSchema, requestID)
		r.pendingSchemaMu.Unlock()
	}()

	requestMessage := &plugin_pb.AdminToWorkerMessage{
		RequestId: requestID,
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_RequestConfigSchema{
			RequestConfigSchema: &plugin_pb.RequestConfigSchema{
				JobType:      jobType,
				ForceRefresh: forceRefresh,
			},
		},
	}

	if err := r.sendToWorker(provider.WorkerID, requestMessage); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response, ok := <-responseCh:
		if !ok {
			return nil, fmt.Errorf("schema request %s interrupted", requestID)
		}
		if response == nil {
			return nil, fmt.Errorf("schema request %s returned empty response", requestID)
		}
		if !response.Success {
			return nil, fmt.Errorf("schema request failed for %s: %s", jobType, response.ErrorMessage)
		}
		if response.GetJobTypeDescriptor() == nil {
			return nil, fmt.Errorf("schema request for %s returned no descriptor", jobType)
		}
		return response.GetJobTypeDescriptor(), nil
	}
}

func (r *Runtime) LoadJobTypeConfig(jobType string) (*plugin_pb.PersistedJobTypeConfig, error) {
	return r.store.LoadJobTypeConfig(jobType)
}

func (r *Runtime) SaveJobTypeConfig(config *plugin_pb.PersistedJobTypeConfig) error {
	return r.store.SaveJobTypeConfig(config)
}

func (r *Runtime) LoadDescriptor(jobType string) (*plugin_pb.JobTypeDescriptor, error) {
	return r.store.LoadDescriptor(jobType)
}

func (r *Runtime) LoadRunHistory(jobType string) (*JobTypeRunHistory, error) {
	return r.store.LoadRunHistory(jobType)
}

func (r *Runtime) IsConfigured() bool {
	return r.store.IsConfigured()
}

func (r *Runtime) BaseDir() string {
	return r.store.BaseDir()
}

// RunDetection requests one detector worker to produce job proposals for a job type.
func (r *Runtime) RunDetection(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
) ([]*plugin_pb.JobProposal, error) {
	detector, err := r.registry.PickDetector(jobType)
	if err != nil {
		return nil, err
	}

	requestID, err := newRequestID("detect")
	if err != nil {
		return nil, err
	}

	adminRuntime, adminConfigValues, workerConfigValues, err := r.loadJobTypeConfigPayload(jobType)
	if err != nil {
		return nil, err
	}

	state := &pendingDetectionState{
		complete: make(chan *plugin_pb.DetectionComplete, 1),
	}
	r.pendingDetectionMu.Lock()
	r.pendingDetection[requestID] = state
	r.pendingDetectionMu.Unlock()
	defer func() {
		r.pendingDetectionMu.Lock()
		delete(r.pendingDetection, requestID)
		r.pendingDetectionMu.Unlock()
	}()

	message := &plugin_pb.AdminToWorkerMessage{
		RequestId: requestID,
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_RunDetectionRequest{
			RunDetectionRequest: &plugin_pb.RunDetectionRequest{
				RequestId:          requestID,
				JobType:            jobType,
				DetectionSequence:  time.Now().UnixNano(),
				AdminRuntime:       adminRuntime,
				AdminConfigValues:  adminConfigValues,
				WorkerConfigValues: workerConfigValues,
				ClusterContext:     clusterContext,
				MaxResults:         maxResults,
			},
		},
	}

	if err := r.sendToWorker(detector.WorkerID, message); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case complete, ok := <-state.complete:
		if !ok {
			return nil, fmt.Errorf("detection request %s interrupted", requestID)
		}
		proposals := cloneJobProposals(state.proposals)
		if complete == nil {
			return proposals, fmt.Errorf("detection request %s returned no completion state", requestID)
		}
		if !complete.Success {
			return proposals, fmt.Errorf("detection failed for %s: %s", jobType, complete.ErrorMessage)
		}
		return proposals, nil
	}
}

// ExecuteJob sends one job to a capable executor worker and waits for completion.
func (r *Runtime) ExecuteJob(
	ctx context.Context,
	job *plugin_pb.JobSpec,
	clusterContext *plugin_pb.ClusterContext,
	attempt int32,
) (*plugin_pb.JobCompleted, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	if strings.TrimSpace(job.JobType) == "" {
		return nil, fmt.Errorf("job_type is required")
	}

	executor, err := r.registry.PickExecutor(job.JobType)
	if err != nil {
		return nil, err
	}

	return r.executeJobWithExecutor(ctx, executor, job, clusterContext, attempt)
}

func (r *Runtime) executeJobWithExecutor(
	ctx context.Context,
	executor *WorkerSession,
	job *plugin_pb.JobSpec,
	clusterContext *plugin_pb.ClusterContext,
	attempt int32,
) (*plugin_pb.JobCompleted, error) {
	if executor == nil {
		return nil, fmt.Errorf("executor is nil")
	}
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	if strings.TrimSpace(job.JobType) == "" {
		return nil, fmt.Errorf("job_type is required")
	}

	if strings.TrimSpace(job.JobId) == "" {
		var err error
		job.JobId, err = newRequestID("job")
		if err != nil {
			return nil, err
		}
	}

	requestID, err := newRequestID("exec")
	if err != nil {
		return nil, err
	}

	adminRuntime, adminConfigValues, workerConfigValues, err := r.loadJobTypeConfigPayload(job.JobType)
	if err != nil {
		return nil, err
	}

	completedCh := make(chan *plugin_pb.JobCompleted, 1)
	r.pendingExecutionMu.Lock()
	r.pendingExecution[requestID] = completedCh
	r.pendingExecutionMu.Unlock()
	defer func() {
		r.pendingExecutionMu.Lock()
		delete(r.pendingExecution, requestID)
		r.pendingExecutionMu.Unlock()
	}()

	r.trackExecutionStart(requestID, executor.WorkerID, job, attempt)

	message := &plugin_pb.AdminToWorkerMessage{
		RequestId: requestID,
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_ExecuteJobRequest{
			ExecuteJobRequest: &plugin_pb.ExecuteJobRequest{
				RequestId:          requestID,
				Job:                job,
				AdminRuntime:       adminRuntime,
				AdminConfigValues:  adminConfigValues,
				WorkerConfigValues: workerConfigValues,
				ClusterContext:     clusterContext,
				Attempt:            attempt,
			},
		},
	}

	if err := r.sendToWorker(executor.WorkerID, message); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case completed, ok := <-completedCh:
		if !ok {
			return nil, fmt.Errorf("execution request %s interrupted", requestID)
		}
		if completed == nil {
			return nil, fmt.Errorf("execution request %s returned empty completion", requestID)
		}
		if !completed.Success {
			return completed, fmt.Errorf("job %s failed: %s", job.JobId, completed.ErrorMessage)
		}
		return completed, nil
	}
}

func (r *Runtime) ListWorkers() []*WorkerSession {
	return r.registry.List()
}

func (r *Runtime) ListKnownJobTypes() ([]string, error) {
	registryJobTypes := r.registry.JobTypes()
	storedJobTypes, err := r.store.ListJobTypes()
	if err != nil {
		return nil, err
	}

	jobTypeSet := make(map[string]struct{}, len(registryJobTypes)+len(storedJobTypes))
	for _, jobType := range registryJobTypes {
		jobTypeSet[jobType] = struct{}{}
	}
	for _, jobType := range storedJobTypes {
		jobTypeSet[jobType] = struct{}{}
	}

	out := make([]string, 0, len(jobTypeSet))
	for jobType := range jobTypeSet {
		out = append(out, jobType)
	}
	sort.Strings(out)
	return out, nil
}

func (r *Runtime) PickDetectorWorker(jobType string) (*WorkerSession, error) {
	return r.registry.PickDetector(jobType)
}

func (r *Runtime) PickExecutorWorker(jobType string) (*WorkerSession, error) {
	return r.registry.PickExecutor(jobType)
}

func (r *Runtime) sendAdminHello(workerID string) error {
	msg := &plugin_pb.AdminToWorkerMessage{
		RequestId: "",
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_Hello{
			Hello: &plugin_pb.AdminHello{
				Accepted:                 true,
				Message:                  "plugin runtime connected",
				HeartbeatIntervalSeconds: defaultHeartbeatInterval,
				ReconnectDelaySeconds:    defaultReconnectDelay,
			},
		},
	}
	return r.sendToWorker(workerID, msg)
}

func (r *Runtime) sendLoop(
	ctx context.Context,
	stream plugin_pb.PluginControlService_WorkerStreamServer,
	session *streamSession,
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.shutdownCh:
			return nil
		case msg, ok := <-session.outgoing:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (r *Runtime) sendToWorker(workerID string, message *plugin_pb.AdminToWorkerMessage) error {
	r.sessionsMu.RLock()
	session, ok := r.sessions[workerID]
	r.sessionsMu.RUnlock()
	if !ok {
		return fmt.Errorf("worker %s is not connected", workerID)
	}

	select {
	case <-r.shutdownCh:
		return fmt.Errorf("plugin runtime is shutting down")
	case session.outgoing <- message:
		return nil
	case <-time.After(r.sendTimeout):
		return fmt.Errorf("timed out sending message to worker %s", workerID)
	}
}

func (r *Runtime) handleWorkerMessage(workerID string, message *plugin_pb.WorkerToAdminMessage) {
	if message == nil {
		return
	}

	switch body := message.Body.(type) {
	case *plugin_pb.WorkerToAdminMessage_Hello:
		r.registry.UpsertFromHello(body.Hello)
	case *plugin_pb.WorkerToAdminMessage_Heartbeat:
		r.registry.UpdateHeartbeat(workerID, body.Heartbeat)
	case *plugin_pb.WorkerToAdminMessage_ConfigSchemaResponse:
		r.handleConfigSchemaResponse(body.ConfigSchemaResponse)
	case *plugin_pb.WorkerToAdminMessage_DetectionProposals:
		r.handleDetectionProposals(body.DetectionProposals)
	case *plugin_pb.WorkerToAdminMessage_DetectionComplete:
		r.handleDetectionComplete(body.DetectionComplete)
	case *plugin_pb.WorkerToAdminMessage_JobProgressUpdate:
		r.handleJobProgressUpdate(body.JobProgressUpdate)
	case *plugin_pb.WorkerToAdminMessage_JobCompleted:
		r.handleJobCompleted(body.JobCompleted)
	case *plugin_pb.WorkerToAdminMessage_Acknowledge:
		if !body.Acknowledge.Accepted {
			glog.Warningf("Plugin worker %s rejected request %s: %s", workerID, body.Acknowledge.RequestId, body.Acknowledge.Message)
		}
	default:
		// Keep the transport open even if admin does not yet consume all message variants.
	}
}

func (r *Runtime) handleConfigSchemaResponse(response *plugin_pb.ConfigSchemaResponse) {
	if response == nil {
		return
	}

	if response.Success && response.GetJobTypeDescriptor() != nil {
		jobType := response.JobType
		if jobType == "" {
			jobType = response.GetJobTypeDescriptor().JobType
		}
		if jobType != "" {
			if err := r.store.SaveDescriptor(jobType, response.GetJobTypeDescriptor()); err != nil {
				glog.Warningf("Plugin failed to persist descriptor for %s: %v", jobType, err)
			}
			if err := r.ensureJobTypeConfigFromDescriptor(jobType, response.GetJobTypeDescriptor()); err != nil {
				glog.Warningf("Plugin failed to bootstrap config for %s: %v", jobType, err)
			}
		}
	}

	if response.RequestId == "" {
		return
	}

	r.pendingSchemaMu.Lock()
	ch := r.pendingSchema[response.RequestId]
	r.pendingSchemaMu.Unlock()
	if ch == nil {
		return
	}

	select {
	case ch <- response:
	default:
	}
}

func (r *Runtime) ensureJobTypeConfigFromDescriptor(jobType string, descriptor *plugin_pb.JobTypeDescriptor) error {
	if descriptor == nil || strings.TrimSpace(jobType) == "" {
		return nil
	}

	existing, err := r.store.LoadJobTypeConfig(jobType)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}

	workerDefaults := cloneConfigValueMap(descriptor.WorkerDefaultValues)
	if len(workerDefaults) == 0 && descriptor.WorkerConfigForm != nil {
		workerDefaults = cloneConfigValueMap(descriptor.WorkerConfigForm.DefaultValues)
	}

	adminDefaults := map[string]*plugin_pb.ConfigValue{}
	if descriptor.AdminConfigForm != nil {
		adminDefaults = cloneConfigValueMap(descriptor.AdminConfigForm.DefaultValues)
	}

	adminRuntime := &plugin_pb.AdminRuntimeConfig{}
	if descriptor.AdminRuntimeDefaults != nil {
		defaults := descriptor.AdminRuntimeDefaults
		adminRuntime = &plugin_pb.AdminRuntimeConfig{
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

	cfg := &plugin_pb.PersistedJobTypeConfig{
		JobType:            jobType,
		DescriptorVersion:  descriptor.DescriptorVersion,
		AdminConfigValues:  adminDefaults,
		WorkerConfigValues: workerDefaults,
		AdminRuntime:       adminRuntime,
		UpdatedAt:          timestamppb.Now(),
		UpdatedBy:          "plugin-runtime",
	}

	return r.store.SaveJobTypeConfig(cfg)
}

func (r *Runtime) handleDetectionProposals(message *plugin_pb.DetectionProposals) {
	if message == nil || message.RequestId == "" {
		return
	}

	r.pendingDetectionMu.Lock()
	state := r.pendingDetection[message.RequestId]
	if state != nil {
		state.proposals = append(state.proposals, cloneJobProposals(message.Proposals)...)
	}
	r.pendingDetectionMu.Unlock()
}

func (r *Runtime) handleDetectionComplete(message *plugin_pb.DetectionComplete) {
	if message == nil {
		return
	}
	if !message.Success {
		glog.Warningf("Plugin detection failed job_type=%s: %s", message.JobType, message.ErrorMessage)
	}
	if message.RequestId == "" {
		return
	}

	r.pendingDetectionMu.Lock()
	state := r.pendingDetection[message.RequestId]
	r.pendingDetectionMu.Unlock()
	if state == nil {
		return
	}

	select {
	case state.complete <- message:
	default:
	}
}

func (r *Runtime) handleJobCompleted(completed *plugin_pb.JobCompleted) {
	if completed == nil || completed.JobType == "" {
		return
	}

	if completed.RequestId != "" {
		r.pendingExecutionMu.Lock()
		waiter := r.pendingExecution[completed.RequestId]
		r.pendingExecutionMu.Unlock()
		if waiter != nil {
			select {
			case waiter <- completed:
			default:
			}
		}
	}

	r.trackWorkerActivities(completed.JobType, completed.JobId, completed.RequestId, "", completed.Activities)

	record := &JobRunRecord{
		RunID:       completed.RequestId,
		JobID:       completed.JobId,
		JobType:     completed.JobType,
		WorkerID:    "",
		Outcome:     RunOutcomeError,
		CompletedAt: time.Now().UTC(),
	}
	if completed.CompletedAt != nil {
		record.CompletedAt = completed.CompletedAt.AsTime().UTC()
	}
	if completed.Success {
		record.Outcome = RunOutcomeSuccess
	}
	if completed.Success {
		record.Message = "completed"
		if completed.Result != nil && completed.Result.Summary != "" {
			record.Message = completed.Result.Summary
		}
	} else {
		record.Message = completed.ErrorMessage
	}

	if tracked := r.trackExecutionCompletion(completed); tracked != nil {
		if tracked.WorkerID != "" {
			record.WorkerID = tracked.WorkerID
		}
		if !tracked.CreatedAt.IsZero() && !record.CompletedAt.IsZero() && record.CompletedAt.After(tracked.CreatedAt) {
			record.DurationMs = int64(record.CompletedAt.Sub(tracked.CreatedAt) / time.Millisecond)
		}
	}

	if err := r.store.AppendRunRecord(completed.JobType, record); err != nil {
		glog.Warningf("Plugin failed to append run record for %s: %v", completed.JobType, err)
	}
}

func (r *Runtime) putSession(session *streamSession) {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()

	if old, exists := r.sessions[session.workerID]; exists {
		old.close()
	}
	r.sessions[session.workerID] = session
}

func (r *Runtime) cleanupSession(workerID string) {
	r.registry.Remove(workerID)

	r.sessionsMu.Lock()
	session, exists := r.sessions[workerID]
	if exists {
		delete(r.sessions, workerID)
		session.close()
	}
	r.sessionsMu.Unlock()

	glog.V(0).Infof("Plugin worker disconnected: %s", workerID)
}

func newRequestID(prefix string) (string, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	if prefix == "" {
		prefix = "req"
	}
	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixNano(), hex.EncodeToString(buf)), nil
}

func (r *Runtime) loadJobTypeConfigPayload(jobType string) (
	*plugin_pb.AdminRuntimeConfig,
	map[string]*plugin_pb.ConfigValue,
	map[string]*plugin_pb.ConfigValue,
	error,
) {
	config, err := r.store.LoadJobTypeConfig(jobType)
	if err != nil {
		return nil, nil, nil, err
	}

	if config == nil {
		return &plugin_pb.AdminRuntimeConfig{}, map[string]*plugin_pb.ConfigValue{}, map[string]*plugin_pb.ConfigValue{}, nil
	}

	adminRuntime := config.AdminRuntime
	if adminRuntime == nil {
		adminRuntime = &plugin_pb.AdminRuntimeConfig{}
	}
	return adminRuntime, cloneConfigValueMap(config.AdminConfigValues), cloneConfigValueMap(config.WorkerConfigValues), nil
}

func cloneJobProposals(in []*plugin_pb.JobProposal) []*plugin_pb.JobProposal {
	if len(in) == 0 {
		return nil
	}
	out := make([]*plugin_pb.JobProposal, 0, len(in))
	for _, proposal := range in {
		if proposal == nil {
			continue
		}
		clone := *proposal
		if proposal.Parameters != nil {
			clone.Parameters = cloneConfigValueMap(proposal.Parameters)
		}
		if proposal.Labels != nil {
			clone.Labels = make(map[string]string, len(proposal.Labels))
			for k, v := range proposal.Labels {
				clone.Labels[k] = v
			}
		}
		out = append(out, &clone)
	}
	return out
}

func cloneConfigValueMap(in map[string]*plugin_pb.ConfigValue) map[string]*plugin_pb.ConfigValue {
	if len(in) == 0 {
		return map[string]*plugin_pb.ConfigValue{}
	}
	out := make(map[string]*plugin_pb.ConfigValue, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		clone := *value
		out[key] = &clone
	}
	return out
}

func (s *streamSession) close() {
	s.closeOnce.Do(func() {
		close(s.outgoing)
	})
}
