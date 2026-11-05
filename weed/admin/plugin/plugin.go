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
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultOutgoingBuffer      = 128
	defaultSendTimeout         = 5 * time.Second
	defaultHeartbeatInterval   = 30
	defaultReconnectDelay      = 5
	defaultPendingSchemaBuffer = 1
	adminScriptJobType         = "admin_script"
	// How often an in-flight detection re-checks that its worker is still
	// registered and heartbeating; a session that went stale fails the
	// detection the same way a closed stream does.
	defaultDetectionLivenessInterval = 30 * time.Second
)

type Options struct {
	DataDir                string
	OutgoingBufferSize     int
	SendTimeout            time.Duration
	SchedulerTick          time.Duration
	ClusterContextProvider func(context.Context) (*plugin_pb.ClusterContext, error)
	LockManager            LockManager
	// ConfigDefaultsProvider is an optional callback invoked when a job type's
	// config is being bootstrapped from its descriptor defaults. It can enrich
	// or replace the default config before it is persisted. If nil, descriptor
	// defaults are used as-is.
	ConfigDefaultsProvider func(config *plugin_pb.PersistedJobTypeConfig) *plugin_pb.PersistedJobTypeConfig
}

// JobTypeInfo contains metadata about a plugin job type.
type JobTypeInfo struct {
	JobType     string `json:"job_type"`
	DisplayName string `json:"display_name"`
	Weight      int32  `json:"weight"`
}

type Plugin struct {
	plugin_pb.UnimplementedPluginControlServiceServer

	store    *ConfigStore
	registry *Registry

	outgoingBuffer int
	sendTimeout    time.Duration

	detectionLivenessInterval time.Duration

	schedulerTick          time.Duration
	clusterContextProvider func(context.Context) (*plugin_pb.ClusterContext, error)
	configDefaultsProvider func(config *plugin_pb.PersistedJobTypeConfig) *plugin_pb.PersistedJobTypeConfig
	lockManager            LockManager

	schedulerMu       sync.Mutex
	nextDetectionAt   map[string]time.Time
	detectionInFlight map[string]bool

	detectorLeaseMu sync.Mutex
	detectorLeases  map[string]string

	lanes map[SchedulerLane]*schedulerLaneState

	// Execution slots this admin holds on workers, one entry per job in
	// flight; see executorLoadLocked for how they meet the heartbeat.
	execResMu            sync.Mutex
	execRes              map[uint64]execReservation
	execResSeq           uint64
	adminScriptRunMu     sync.RWMutex
	schedulerDetectionMu sync.Mutex
	schedulerDetection   map[string]*schedulerDetectionInfo
	schedulerRunMu       sync.Mutex
	schedulerRun         map[string]*schedulerRunInfo
	schedulerLoopMu      sync.Mutex
	schedulerLoopState   schedulerLoopState
	schedulerWakeCh      chan struct{}

	dedupeMu           sync.Mutex
	recentDedupeByType map[string]map[string]time.Time

	sessionsMu sync.RWMutex
	sessions   map[string]*streamSession

	pendingSchemaMu sync.Mutex
	pendingSchema   map[string]chan *plugin_pb.ConfigSchemaResponse

	pendingDetectionMu sync.Mutex
	pendingDetection   map[string]*pendingDetectionState

	pendingExecutionMu sync.Mutex
	pendingExecution   map[string]*pendingExecutionState

	pendingPreviewMu sync.Mutex
	pendingPreview   map[string]chan *plugin_pb.ObjectPreviewResponse

	observations *ObservationStore

	jobsMu sync.RWMutex
	jobs   map[string]*TrackedJob
	// serialize stale job cleanup to avoid duplicate expirations
	staleJobsMu sync.Mutex

	undeliveredCancelsMu sync.Mutex
	undeliveredCancels   map[string]undeliveredCancel

	jobDetailsMu sync.Mutex

	activitiesMu sync.RWMutex
	activities   []JobActivity

	dirtyJobs       bool
	dirtyActivities bool
	persistTicker   *time.Ticker

	ctx       context.Context
	ctxCancel context.CancelFunc

	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	wg           sync.WaitGroup
}

type streamSession struct {
	workerID  string
	outgoing  chan *plugin_pb.AdminToWorkerMessage
	done      chan struct{}
	closeOnce sync.Once
}

type pendingDetectionState struct {
	proposals []*plugin_pb.JobProposal
	complete  chan *plugin_pb.DetectionComplete
	jobType   string
	workerID  string
}

// detectionWorkerDisconnectedError reports a detection whose worker session
// ended (stream closed, replaced by a reconnect, or heartbeat went stale)
// before it answered.
type detectionWorkerDisconnectedError struct {
	WorkerID string
	Reason   string
	session  *streamSession
}

func (e *detectionWorkerDisconnectedError) Error() string {
	msg := fmt.Sprintf("detection worker %s disconnected", e.WorkerID)
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	return msg
}

// DetectionReport captures one detection run including request metadata.
type DetectionReport struct {
	RequestID string
	JobType   string
	WorkerID  string
	Proposals []*plugin_pb.JobProposal
	Complete  *plugin_pb.DetectionComplete
}

func New(options Options) (*Plugin, error) {
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

	lanes := make(map[SchedulerLane]*schedulerLaneState, len(AllLanes()))
	for _, lane := range AllLanes() {
		lanes[lane] = newLaneState(lane)
	}

	plugin := &Plugin{
		store:                     store,
		registry:                  NewRegistry(),
		outgoingBuffer:            bufferSize,
		sendTimeout:               sendTimeout,
		detectionLivenessInterval: defaultDetectionLivenessInterval,
		schedulerTick:             schedulerTick,
		clusterContextProvider:    options.ClusterContextProvider,
		configDefaultsProvider:    options.ConfigDefaultsProvider,
		lockManager:               options.LockManager,
		lanes:                     lanes,
		sessions:                  make(map[string]*streamSession),
		pendingSchema:             make(map[string]chan *plugin_pb.ConfigSchemaResponse),
		pendingDetection:          make(map[string]*pendingDetectionState),
		pendingExecution:          make(map[string]*pendingExecutionState),
		pendingPreview:            make(map[string]chan *plugin_pb.ObjectPreviewResponse),
		observations:              NewObservationStore(),
		nextDetectionAt:           make(map[string]time.Time),
		detectionInFlight:         make(map[string]bool),
		detectorLeases:            make(map[string]string),
		execRes:                   make(map[uint64]execReservation),
		schedulerDetection:        make(map[string]*schedulerDetectionInfo),
		schedulerRun:              make(map[string]*schedulerRunInfo),
		recentDedupeByType:        make(map[string]map[string]time.Time),
		jobs:                      make(map[string]*TrackedJob),
		activities:                make([]JobActivity, 0, 256),
		persistTicker:             time.NewTicker(2 * time.Second),
		schedulerWakeCh:           make(chan struct{}, 1),
		shutdownCh:                make(chan struct{}),
	}
	plugin.ctx, plugin.ctxCancel = context.WithCancel(context.Background())

	if err := plugin.loadPersistedMonitorState(); err != nil {
		glog.Warningf("Plugin failed to load persisted monitoring state: %v", err)
	}

	if plugin.clusterContextProvider != nil {
		for _, ls := range plugin.lanes {
			plugin.wg.Add(1)
			go plugin.laneSchedulerLoop(ls)
		}
	}
	plugin.wg.Add(1)
	go plugin.persistenceLoop()

	return plugin, nil
}

func (r *Plugin) Shutdown() {
	if r.ctxCancel != nil {
		r.ctxCancel()
	}
	if r.persistTicker != nil {
		r.persistTicker.Stop()
	}

	r.shutdownOnce.Do(func() { close(r.shutdownCh) })

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

	r.pendingPreviewMu.Lock()
	for requestID, ch := range r.pendingPreview {
		close(ch)
		delete(r.pendingPreview, requestID)
	}
	r.pendingPreviewMu.Unlock()

	r.pendingDetectionMu.Lock()
	for requestID, state := range r.pendingDetection {
		close(state.complete)
		delete(r.pendingDetection, requestID)
	}
	r.pendingDetectionMu.Unlock()

	r.pendingExecutionMu.Lock()
	for requestID, pending := range r.pendingExecution {
		close(pending.completed)
		delete(r.pendingExecution, requestID)
	}
	r.pendingExecutionMu.Unlock()

	r.wg.Wait()
}

func (r *Plugin) WorkerStream(stream plugin_pb.PluginControlService_WorkerStreamServer) error {
	if err := security.CheckGrpcAdminPeer(stream.Context(), security.SigningKey(util.GetViper().GetString("jwt.filer_signing.key"))); err != nil {
		return err
	}

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
	session := &streamSession{
		workerID: workerID,
		outgoing: make(chan *plugin_pb.AdminToWorkerMessage, r.outgoingBuffer),
		done:     make(chan struct{}),
	}
	r.registerSession(session, hello)
	defer r.cleanupSession(session)

	glog.V(0).Infof("Plugin worker connected: %s (%s)", workerID, hello.Address)

	sendErrCh := make(chan error, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		sendErrCh <- r.sendLoop(stream.Context(), stream, session)
	}()

	if err := r.sendAdminHello(session); err != nil {
		glog.Warningf("failed to send plugin admin hello to %s: %v", workerID, err)
	}
	go r.prefetchDescriptorsFromHello(hello)

	recvErrCh := make(chan error, 1)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			message, recvErr := stream.Recv()
			if recvErr != nil {
				recvErrCh <- recvErr
				return
			}
			r.handleWorkerMessage(session, message)
		}
	}()

	for {
		select {
		case <-r.shutdownCh:
			return nil
		case err := <-sendErrCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			return nil
		case recvErr := <-recvErrCh:
			if errors.Is(recvErr, io.EOF) || errors.Is(recvErr, context.Canceled) {
				return nil
			}
			return fmt.Errorf("receive plugin message from %s: %w", workerID, recvErr)
		}
	}
}

func (r *Plugin) RequestConfigSchema(ctx context.Context, jobType string, forceRefresh bool) (*plugin_pb.JobTypeDescriptor, error) {
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
	return r.requestConfigSchema(ctx, jobType, forceRefresh, provider.WorkerID)
}

func (r *Plugin) requestConfigSchema(ctx context.Context, jobType string, forceRefresh bool, workerID string) (*plugin_pb.JobTypeDescriptor, error) {
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

	if err := r.sendToWorker(workerID, requestMessage); err != nil {
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

func (r *Plugin) LoadJobTypeConfig(jobType string) (*plugin_pb.PersistedJobTypeConfig, error) {
	return r.store.LoadJobTypeConfig(jobType)
}

func (r *Plugin) SaveJobTypeConfig(config *plugin_pb.PersistedJobTypeConfig) error {
	if err := r.store.SaveJobTypeConfig(config); err != nil {
		return err
	}
	r.wakeScheduler()
	return nil
}

func (r *Plugin) LoadDescriptor(jobType string) (*plugin_pb.JobTypeDescriptor, error) {
	return r.store.LoadDescriptor(jobType)
}

func (r *Plugin) LoadRunHistory(jobType string) (*JobTypeRunHistory, error) {
	return r.store.LoadRunHistory(jobType)
}

func (r *Plugin) IsConfigured() bool {
	return r.store.IsConfigured()
}

func (r *Plugin) BaseDir() string {
	return r.store.BaseDir()
}

func (r *Plugin) acquireAdminLock(reason string) (func(), error) {
	if r == nil || r.lockManager == nil {
		return func() {}, nil
	}
	release, err := r.lockManager.Acquire(reason)
	if release == nil {
		// Lock manager implementations may return a nil release when
		// disabled; callers always invoke the release function.
		release = func() {}
	}
	return release, err
}

// RunDetectionWithReport requests one detector worker and returns proposals with request metadata.
func (r *Plugin) RunDetectionWithReport(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
) (*DetectionReport, error) {
	return r.runDetectionWithReport(ctx, jobType, clusterContext, maxResults, nil)
}

// runDetectionWithReport is RunDetectionWithReport with a stream the pick
// must avoid: the one whose closure a retry is recovering from.
func (r *Plugin) runDetectionWithReport(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
	avoid *streamSession,
) (*DetectionReport, error) {
	releaseGate := r.acquireDetectionExecutionGate(jobType, false)
	defer releaseGate()

	detector, err := r.pickDetector(jobType, avoid)
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
	lastCompletedRun := r.loadLastSuccessfulRun(jobType)
	if strings.EqualFold(strings.TrimSpace(jobType), adminScriptJobType) {
		lastCompletedRun = r.loadLastCompletedRun(jobType)
	}

	state := &pendingDetectionState{
		complete: make(chan *plugin_pb.DetectionComplete, 1),
		jobType:  jobType,
		workerID: detector.WorkerID,
	}
	r.pendingDetectionMu.Lock()
	r.pendingDetection[requestID] = state
	r.pendingDetectionMu.Unlock()
	defer func() {
		r.pendingDetectionMu.Lock()
		delete(r.pendingDetection, requestID)
		r.pendingDetectionMu.Unlock()
	}()

	r.appendActivity(JobActivity{
		JobType:    jobType,
		RequestID:  requestID,
		WorkerID:   detector.WorkerID,
		Source:     "detector",
		Stage:      "requested",
		Message:    "detection requested",
		OccurredAt: timeToPtr(time.Now().UTC()),
		Details: map[string]interface{}{
			"max_results": maxResults,
		},
	})

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
				LastSuccessfulRun:  lastCompletedRun,
				MaxResults:         maxResults,
			},
		},
	}

	// The request is bound to the stream it was sent on: if that stream ends
	// (disconnect or a reconnect with a new session) the answer can never
	// arrive, so the wait below fails instead of running out the timeout.
	session, connected := r.getSession(detector.WorkerID)
	if !connected {
		err = fmt.Errorf("worker %s is not connected", detector.WorkerID)
	} else {
		err = r.sendToSession(session, message)
	}
	if err != nil {
		r.clearDetectorLease(jobType, detector.WorkerID)
		r.appendActivity(JobActivity{
			JobType:    jobType,
			RequestID:  requestID,
			WorkerID:   detector.WorkerID,
			Source:     "detector",
			Stage:      "failed_to_send",
			Message:    err.Error(),
			OccurredAt: timeToPtr(time.Now().UTC()),
		})
		return nil, err
	}

	return r.awaitDetectionOutcome(ctx, jobType, requestID, session, state)
}

// awaitDetectionOutcome waits for the request's completion, the end of the
// stream it was sent on, a stale worker heartbeat, or ctx. A completion the
// worker delivered before its stream ended is buffered in state.complete, and
// the stream's closure can be observed in the same select: that completion
// wins, so a scan that finished right before its worker went away is reported
// as the success it was, not as a disconnect. A ctx that ended stands as is:
// the caller stopped wanting the answer.
func (r *Plugin) awaitDetectionOutcome(
	ctx context.Context,
	jobType string,
	requestID string,
	session *streamSession,
	state *pendingDetectionState,
) (*DetectionReport, error) {
	workerID := session.workerID
	emptyReport := func() *DetectionReport {
		return &DetectionReport{
			RequestID: requestID,
			JobType:   jobType,
			WorkerID:  workerID,
		}
	}
	finish := func(complete *plugin_pb.DetectionComplete, ok bool) (*DetectionReport, error) {
		if !ok {
			return emptyReport(), fmt.Errorf("detection request %s interrupted", requestID)
		}
		proposals := cloneJobProposals(state.proposals)
		report := &DetectionReport{
			RequestID: requestID,
			JobType:   jobType,
			WorkerID:  workerID,
			Proposals: proposals,
			Complete:  complete,
		}
		if complete == nil {
			return report, fmt.Errorf("detection request %s returned no completion state", requestID)
		}
		if !complete.Success {
			return report, fmt.Errorf("detection failed for %s: %s", jobType, complete.ErrorMessage)
		}
		return report, nil
	}
	// finishIfCompleted takes a completion that is already queued when the
	// worker is judged lost. The lease still goes: the worker that answered
	// is gone or unreachable, so the next pick must start fresh.
	finishIfCompleted := func() (bool, *DetectionReport, error) {
		select {
		case complete, ok := <-state.complete:
			r.clearDetectorLease(jobType, workerID)
			report, err := finish(complete, ok)
			return true, report, err
		default:
			return false, nil, nil
		}
	}

	livenessInterval := r.detectionLivenessInterval
	if livenessInterval <= 0 {
		livenessInterval = defaultDetectionLivenessInterval
	}
	liveness := time.NewTicker(livenessInterval)
	defer liveness.Stop()

	for {
		select {
		case <-ctx.Done():
			r.sendCancel(workerID, requestID, plugin_pb.WorkKind_WORK_KIND_DETECTION, ctx.Err())
			r.appendActivity(JobActivity{
				JobType:    jobType,
				RequestID:  requestID,
				WorkerID:   workerID,
				Source:     "detector",
				Stage:      "canceled",
				Message:    "detection canceled",
				OccurredAt: timeToPtr(time.Now().UTC()),
			})
			return emptyReport(), ctx.Err()
		case <-session.done:
			if completed, report, err := finishIfCompleted(); completed {
				return report, err
			}
			return emptyReport(), r.failDetectionWorkerLost(jobType, requestID, session, "", false)
		case <-liveness.C:
			if r.isWorkerAvailable(workerID) {
				continue
			}
			if completed, report, err := finishIfCompleted(); completed {
				return report, err
			}
			// The stream may still be open, so the worker is told to stop.
			return emptyReport(), r.failDetectionWorkerLost(jobType, requestID, session, "heartbeat stale", true)
		case complete, ok := <-state.complete:
			return finish(complete, ok)
		}
	}
}

// failDetectionWorkerLost closes out a detection whose worker session ended
// before it answered. The detector lease is dropped so the next pick starts
// fresh; the worker is only told to cancel when its stream may still be open.
func (r *Plugin) failDetectionWorkerLost(jobType, requestID string, session *streamSession, reason string, cancelOnWorker bool) error {
	workerID := session.workerID
	err := &detectionWorkerDisconnectedError{WorkerID: workerID, Reason: reason, session: session}
	r.clearDetectorLease(jobType, workerID)
	if cancelOnWorker {
		r.sendCancel(workerID, requestID, plugin_pb.WorkKind_WORK_KIND_DETECTION, err)
	}
	glog.Warningf("Plugin detection %s for %s failed: %v", requestID, jobType, err)
	r.appendActivity(JobActivity{
		JobType:    jobType,
		RequestID:  requestID,
		WorkerID:   workerID,
		Source:     "detector",
		Stage:      "disconnected",
		Message:    err.Error(),
		OccurredAt: timeToPtr(time.Now().UTC()),
	})
	return err
}

// RunDetection requests one detector worker to produce job proposals for a job type.
func (r *Plugin) RunDetection(
	ctx context.Context,
	jobType string,
	clusterContext *plugin_pb.ClusterContext,
	maxResults int32,
) ([]*plugin_pb.JobProposal, error) {
	report, err := r.RunDetectionWithReport(ctx, jobType, clusterContext, maxResults)
	if report == nil {
		return nil, err
	}
	return report.Proposals, err
}

// ExecuteJob sends one job to a capable executor worker and waits for completion.
func (r *Plugin) ExecuteJob(
	ctx context.Context,
	job *plugin_pb.JobSpec,
	clusterContext *plugin_pb.ClusterContext,
	attempt int32,
) (*plugin_pb.JobCompleted, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	jobType := strings.TrimSpace(job.JobType)
	if jobType == "" {
		return nil, fmt.Errorf("job_type is required")
	}
	releaseGate := r.acquireDetectionExecutionGate(jobType, true)
	defer releaseGate()

	executor, err := r.registry.PickExecutor(jobType, JobReplicaHosts(job)...)
	if err != nil {
		return nil, err
	}

	return r.executeJobWithExecutor(ctx, executor, job, clusterContext, attempt)
}

func (r *Plugin) acquireDetectionExecutionGate(jobType string, execution bool) func() {
	normalizedJobType := strings.ToLower(strings.TrimSpace(jobType))
	if execution && normalizedJobType == adminScriptJobType {
		r.adminScriptRunMu.Lock()
		return func() {
			r.adminScriptRunMu.Unlock()
		}
	}
	if normalizedJobType != adminScriptJobType {
		r.adminScriptRunMu.RLock()
		return func() {
			r.adminScriptRunMu.RUnlock()
		}
	}
	return func() {}
}

func (r *Plugin) executeJobWithExecutor(
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

	// Apply per-job-type cluster-allocation decoration (e.g. s3_lifecycle
	// divides cluster_deletes_per_second by min(workers, maxJobsPerDetection)
	// and ships the share via ClusterContext.Metadata). No-op for job
	// types without an allocator registered. MaxJobsPerDetection caps
	// the divisor so a singleton job (maxJobs=1) gets the full budget on
	// the single active worker, not 1/N of it.
	clusterContext = r.decorateClusterContextForJob(clusterContext, job.JobType, adminConfigValues, int(adminRuntime.GetMaxJobsPerDetection()))

	// The request goes out on this stream and no other, so a newer stream
	// of the worker can tell it never received it.
	session, ok := r.getSession(executor.WorkerID)
	if !ok {
		return nil, fmt.Errorf("worker %s is not connected", executor.WorkerID)
	}

	r.trackExecutionStart(requestID, executor.WorkerID, job, attempt)

	completedCh := make(chan *plugin_pb.JobCompleted, 1)
	pending := &pendingExecutionState{
		completed: completedCh,
		jobID:     job.JobId,
		jobType:   job.JobType,
		session:   session,
	}
	r.pendingExecutionMu.Lock()
	r.pendingExecution[requestID] = pending
	r.pendingExecutionMu.Unlock()
	defer func() {
		r.pendingExecutionMu.Lock()
		delete(r.pendingExecution, requestID)
		r.pendingExecutionMu.Unlock()
	}()

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

	if err := r.sendToSession(session, message); err != nil {
		// The worker never got the request, so no report will close the
		// attempt unless an expiry already claimed it. Unregister first:
		// nobody reads completedCh any more. Shutdown may have done so.
		r.pendingExecutionMu.Lock()
		delete(r.pendingExecution, requestID)
		claimed := pending.failed
		r.pendingExecutionMu.Unlock()
		if !claimed {
			r.handleJobCompleted(&plugin_pb.JobCompleted{
				JobId:        job.JobId,
				JobType:      job.JobType,
				RequestId:    requestID,
				ErrorMessage: err.Error(),
				CompletedAt:  timestamppb.Now(),
			})
		}
		return nil, err
	}

	select {
	case <-ctx.Done():
		r.sendCancel(executor.WorkerID, requestID, plugin_pb.WorkKind_WORK_KIND_EXECUTION, ctx.Err())
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

// HasCapableWorker checks if any non-stale worker has a capability for the given job type.
func (r *Plugin) HasCapableWorker(jobType string) bool {
	return r.registry.HasCapableWorker(jobType)
}

func (r *Plugin) ListWorkers() []*WorkerSession {
	return r.registry.List()
}

func (r *Plugin) ListKnownJobTypes() ([]JobTypeInfo, error) {
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

	jobTypeList := make([]string, 0, len(jobTypeSet))
	for jobType := range jobTypeSet {
		jobTypeList = append(jobTypeList, jobType)
	}
	sort.Strings(jobTypeList)

	result := make([]JobTypeInfo, 0, len(jobTypeList))
	workers := r.registry.List()

	// Pre-calculate the best capability for each job type from available workers.
	// Prefer capabilities with non-empty DisplayName, then higher Weight.
	jobTypeToCap := make(map[string]*plugin_pb.JobTypeCapability)
	for _, worker := range workers {
		for jobType, cap := range worker.Capabilities {
			if cap == nil {
				continue
			}
			existing, exists := jobTypeToCap[jobType]
			if !exists || existing == nil {
				jobTypeToCap[jobType] = cap
				continue
			}
			// Preserve existing if it has DisplayName but cap doesn't.
			if existing.DisplayName != "" && cap.DisplayName == "" {
				continue
			}
			// Prefer capabilities with a non-empty DisplayName.
			if existing.DisplayName == "" && cap.DisplayName != "" {
				jobTypeToCap[jobType] = cap
				continue
			}
			// If DisplayName statuses are equal, prefer higher Weight.
			if cap.Weight > existing.Weight {
				jobTypeToCap[jobType] = cap
			}
		}
	}

	for _, jobType := range jobTypeList {
		info := JobTypeInfo{JobType: jobType}

		// Get display name and weight from pre-calculated capabilities
		if cap, ok := jobTypeToCap[jobType]; ok && cap != nil {
			if cap.DisplayName != "" {
				info.DisplayName = cap.DisplayName
			}
			info.Weight = cap.Weight
		}

		// Default display name to job type if not set
		if info.DisplayName == "" {
			info.DisplayName = jobType
		}

		result = append(result, info)
	}

	// Sort by weight (descending) then by job type (ascending)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Weight != result[j].Weight {
			return result[i].Weight > result[j].Weight // higher weight first
		}
		return result[i].JobType < result[j].JobType // alphabetical as tiebreaker
	})

	return result, nil
}

// FilterProposalsWithActiveJobs drops proposals that are already assigned/running.
func (r *Plugin) FilterProposalsWithActiveJobs(jobType string, proposals []*plugin_pb.JobProposal) ([]*plugin_pb.JobProposal, int) {
	return r.filterProposalsWithActiveJobs(jobType, proposals)
}

func (r *Plugin) PickDetectorWorker(jobType string) (*WorkerSession, error) {
	return r.pickDetector(jobType, nil)
}

func (r *Plugin) PickExecutorWorker(jobType string) (*WorkerSession, error) {
	return r.registry.PickExecutor(jobType)
}

// pickDetector picks a detector that can actually be sent to: registered,
// with an open stream, and not the stream in avoid. The registry alone is
// not enough because a stream can close before its entry is removed.
func (r *Plugin) pickDetector(jobType string, avoid *streamSession) (*WorkerSession, error) {
	leasedWorkerID := r.getDetectorLease(jobType)
	if leasedWorkerID != "" {
		if worker, ok := r.registry.Get(leasedWorkerID); ok && r.sessionIsOpen(leasedWorkerID, avoid) {
			if capability := worker.Capabilities[jobType]; capability != nil && capability.CanDetect {
				return worker, nil
			}
		}
		r.clearDetectorLease(jobType, leasedWorkerID)
	}

	var excluded []string
	for {
		detector, err := r.registry.PickDetector(jobType, excluded...)
		if err != nil {
			return nil, err
		}
		if r.sessionIsOpen(detector.WorkerID, avoid) {
			r.setDetectorLease(jobType, detector.WorkerID)
			return detector, nil
		}
		excluded = append(excluded, detector.WorkerID)
	}
}

func (r *Plugin) sessionIsOpen(workerID string, avoid *streamSession) bool {
	session, ok := r.getSession(workerID)
	if !ok || session == avoid {
		return false
	}
	select {
	case <-session.done:
		return false
	default:
		return true
	}
}

func (r *Plugin) getDetectorLease(jobType string) string {
	r.detectorLeaseMu.Lock()
	defer r.detectorLeaseMu.Unlock()
	return r.detectorLeases[jobType]
}

func (r *Plugin) setDetectorLease(jobType string, workerID string) {
	r.detectorLeaseMu.Lock()
	defer r.detectorLeaseMu.Unlock()
	if jobType == "" || workerID == "" {
		return
	}
	r.detectorLeases[jobType] = workerID
}

func (r *Plugin) clearDetectorLease(jobType string, workerID string) {
	r.detectorLeaseMu.Lock()
	defer r.detectorLeaseMu.Unlock()

	current := r.detectorLeases[jobType]
	if current == "" {
		return
	}
	if workerID != "" && current != workerID {
		return
	}
	delete(r.detectorLeases, jobType)
}

func (r *Plugin) sendCancel(workerID, targetID string, kind plugin_pb.WorkKind, cause error) error {
	if strings.TrimSpace(workerID) == "" || strings.TrimSpace(targetID) == "" {
		return nil
	}

	requestID, err := newRequestID("cancel")
	if err != nil {
		requestID = ""
	}
	reason := "request canceled"
	if cause != nil {
		reason = cause.Error()
	}

	message := &plugin_pb.AdminToWorkerMessage{
		RequestId: requestID,
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_CancelRequest{
			CancelRequest: &plugin_pb.CancelRequest{
				TargetId:   targetID,
				TargetKind: kind,
				Reason:     reason,
			},
		},
	}
	err = r.sendToWorker(workerID, message)
	if err != nil {
		glog.V(1).Infof("Plugin failed to send cancel request to worker=%s target=%s: %v", workerID, targetID, err)
	}
	return err
}

func (r *Plugin) sendAdminHello(session *streamSession) error {
	msg := &plugin_pb.AdminToWorkerMessage{
		RequestId: "",
		SentAt:    timestamppb.Now(),
		Body: &plugin_pb.AdminToWorkerMessage_Hello{
			Hello: &plugin_pb.AdminHello{
				Accepted:                 true,
				Message:                  "plugin connected",
				HeartbeatIntervalSeconds: defaultHeartbeatInterval,
				ReconnectDelaySeconds:    defaultReconnectDelay,
			},
		},
	}
	return r.sendToSession(session, msg)
}

func (r *Plugin) sendLoop(
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
		case <-session.done:
			return nil
		case msg := <-session.outgoing:
			if msg == nil {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (r *Plugin) getSession(workerID string) (*streamSession, bool) {
	r.sessionsMu.RLock()
	session, ok := r.sessions[workerID]
	r.sessionsMu.RUnlock()
	return session, ok
}

func (r *Plugin) sendToWorker(workerID string, message *plugin_pb.AdminToWorkerMessage) error {
	session, ok := r.getSession(workerID)
	if !ok {
		return fmt.Errorf("worker %s is not connected", workerID)
	}
	return r.sendToSession(session, message)
}

func (r *Plugin) sendToSession(session *streamSession, message *plugin_pb.AdminToWorkerMessage) error {
	select {
	case <-r.shutdownCh:
		return fmt.Errorf("plugin is shutting down")
	case <-session.done:
		return fmt.Errorf("worker %s session is closed", session.workerID)
	case session.outgoing <- message:
		return nil
	case <-time.After(r.sendTimeout):
		return fmt.Errorf("timed out sending message to worker %s", session.workerID)
	}
}

func (r *Plugin) handleWorkerMessage(session *streamSession, message *plugin_pb.WorkerToAdminMessage) {
	if message == nil {
		return
	}
	workerID := session.workerID

	switch body := message.Body.(type) {
	case *plugin_pb.WorkerToAdminMessage_Hello:
		if !r.updateRegistryFromSession(session, func() { r.registry.UpsertFromHello(body.Hello) }) {
			return
		}
		r.wakeScheduler()
	case *plugin_pb.WorkerToAdminMessage_Heartbeat:
		if !r.updateRegistryFromSession(session, func() { r.registry.UpdateHeartbeat(workerID, body.Heartbeat) }) {
			return
		}
		r.resendUndeliveredCancels(workerID, body.Heartbeat)
		r.failExecutionsMissingFromHeartbeat(session, body.Heartbeat)
	case *plugin_pb.WorkerToAdminMessage_ConfigSchemaResponse:
		r.handleConfigSchemaResponse(body.ConfigSchemaResponse)
	case *plugin_pb.WorkerToAdminMessage_DetectionProposals:
		r.handleDetectionProposals(workerID, body.DetectionProposals)
	case *plugin_pb.WorkerToAdminMessage_DetectionComplete:
		r.handleDetectionComplete(workerID, body.DetectionComplete)
	case *plugin_pb.WorkerToAdminMessage_JobProgressUpdate:
		r.handleJobProgressUpdate(workerID, body.JobProgressUpdate)
	case *plugin_pb.WorkerToAdminMessage_JobCompleted:
		r.handleJobCompleted(body.JobCompleted)
	case *plugin_pb.WorkerToAdminMessage_Observations:
		r.observations.Record(workerID, body.Observations)
	case *plugin_pb.WorkerToAdminMessage_ObjectPreviewResponse:
		r.handleObjectPreviewResponse(body.ObjectPreviewResponse)
	case *plugin_pb.WorkerToAdminMessage_Acknowledge:
		if !body.Acknowledge.Accepted {
			glog.Warningf("Plugin worker %s rejected request %s: %s", workerID, body.Acknowledge.RequestId, body.Acknowledge.Message)
		}
	default:
		// Keep the transport open even if admin does not yet consume all message variants.
	}
}

func (r *Plugin) handleConfigSchemaResponse(response *plugin_pb.ConfigSchemaResponse) {
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

	r.safeSendSchemaResponse(response.RequestId, response)
}

func (r *Plugin) safeSendSchemaResponse(requestID string, response *plugin_pb.ConfigSchemaResponse) {
	r.pendingSchemaMu.Lock()
	ch := r.pendingSchema[requestID]
	r.pendingSchemaMu.Unlock()
	safeSendCh(ch, response, r.shutdownCh)
}

func safeSendCh[T any](ch chan T, val T, shutdownCh <-chan struct{}) {
	if ch == nil {
		return
	}
	defer func() {
		recover()
	}()
	select {
	case ch <- val:
	case <-shutdownCh:
	}
}

func (r *Plugin) ensureJobTypeConfigFromDescriptor(jobType string, descriptor *plugin_pb.JobTypeDescriptor) error {
	if descriptor == nil || strings.TrimSpace(jobType) == "" {
		return nil
	}

	workerDefaults := CloneConfigValueMap(descriptor.WorkerDefaultValues)
	if len(workerDefaults) == 0 && descriptor.WorkerConfigForm != nil {
		workerDefaults = CloneConfigValueMap(descriptor.WorkerConfigForm.DefaultValues)
	}

	adminDefaults := map[string]*plugin_pb.ConfigValue{}
	if descriptor.AdminConfigForm != nil {
		adminDefaults = CloneConfigValueMap(descriptor.AdminConfigForm.DefaultValues)
	}

	adminRuntime := &plugin_pb.AdminRuntimeConfig{}
	if descriptor.AdminRuntimeDefaults != nil {
		defaults := descriptor.AdminRuntimeDefaults
		adminRuntime = &plugin_pb.AdminRuntimeConfig{
			Enabled:                       defaults.Enabled,
			DetectionIntervalMinutes:      defaults.DetectionIntervalMinutes,
			DetectionTimeoutSeconds:       defaults.DetectionTimeoutSeconds,
			MaxJobsPerDetection:           defaults.MaxJobsPerDetection,
			GlobalExecutionConcurrency:    defaults.GlobalExecutionConcurrency,
			PerWorkerExecutionConcurrency: defaults.PerWorkerExecutionConcurrency,
			RetryLimit:                    defaults.RetryLimit,
			RetryBackoffSeconds:           defaults.RetryBackoffSeconds,
			JobTypeMaxRuntimeSeconds:      defaults.JobTypeMaxRuntimeSeconds,
			ExecutionTimeoutSeconds:       defaults.ExecutionTimeoutSeconds,
		}
	}

	cfg := &plugin_pb.PersistedJobTypeConfig{
		JobType:            jobType,
		DescriptorVersion:  descriptor.DescriptorVersion,
		AdminConfigValues:  adminDefaults,
		WorkerConfigValues: workerDefaults,
		AdminRuntime:       adminRuntime,
		UpdatedAt:          timestamppb.Now(),
		UpdatedBy:          "plugin",
	}

	// Check existence first to avoid calling configDefaultsProvider unnecessarily
	// (e.g., it may make a blocking gRPC call to fetch master config).
	existing, err := r.store.LoadJobTypeConfig(jobType)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}

	if r.configDefaultsProvider != nil {
		cfg = r.configDefaultsProvider(cfg)
	}

	_, err = r.store.SaveJobTypeConfigIfNotExists(cfg)
	return err
}

func (r *Plugin) handleDetectionProposals(workerID string, message *plugin_pb.DetectionProposals) {
	if message == nil || message.RequestId == "" {
		return
	}

	r.pendingDetectionMu.Lock()
	state := r.pendingDetection[message.RequestId]
	if state != nil {
		state.proposals = append(state.proposals, cloneJobProposals(message.Proposals)...)
	}
	r.pendingDetectionMu.Unlock()
	if state == nil {
		return
	}

	resolvedWorkerID := strings.TrimSpace(workerID)
	if resolvedWorkerID == "" {
		resolvedWorkerID = state.workerID
	}
	resolvedJobType := strings.TrimSpace(message.JobType)
	if resolvedJobType == "" {
		resolvedJobType = state.jobType
	}
	if resolvedJobType == "" {
		resolvedJobType = "unknown"
	}

	r.appendActivity(JobActivity{
		JobType:    resolvedJobType,
		RequestID:  message.RequestId,
		WorkerID:   resolvedWorkerID,
		Source:     "detector",
		Stage:      "proposals_batch",
		Message:    fmt.Sprintf("received %d proposal(s)", len(message.Proposals)),
		OccurredAt: timeToPtr(time.Now().UTC()),
		Details: map[string]interface{}{
			"batch_size": len(message.Proposals),
			"has_more":   message.HasMore,
		},
	})

	for _, proposal := range message.Proposals {
		if proposal == nil {
			continue
		}
		details := map[string]interface{}{
			"proposal_id": proposal.ProposalId,
			"dedupe_key":  proposal.DedupeKey,
			"priority":    proposal.Priority.String(),
			"summary":     proposal.Summary,
			"detail":      proposal.Detail,
			"labels":      proposal.Labels,
		}
		if params := configValueMapToPlain(proposal.Parameters); len(params) > 0 {
			details["parameters"] = params
		}

		messageText := strings.TrimSpace(proposal.Summary)
		if messageText == "" {
			messageText = fmt.Sprintf("proposal %s", strings.TrimSpace(proposal.ProposalId))
		}
		if messageText == "" {
			messageText = "proposal generated"
		}

		r.appendActivity(JobActivity{
			JobType:    resolvedJobType,
			RequestID:  message.RequestId,
			WorkerID:   resolvedWorkerID,
			Source:     "detector",
			Stage:      "proposal",
			Message:    messageText,
			OccurredAt: timeToPtr(time.Now().UTC()),
			Details:    details,
		})
	}
}

func (r *Plugin) handleDetectionComplete(workerID string, message *plugin_pb.DetectionComplete) {
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

	resolvedWorkerID := strings.TrimSpace(workerID)
	if resolvedWorkerID == "" {
		resolvedWorkerID = state.workerID
	}
	resolvedJobType := strings.TrimSpace(message.JobType)
	if resolvedJobType == "" {
		resolvedJobType = state.jobType
	}
	if resolvedJobType == "" {
		resolvedJobType = "unknown"
	}

	stage := "completed"
	messageText := "detection completed"
	if !message.Success {
		stage = "failed"
		messageText = strings.TrimSpace(message.ErrorMessage)
		if messageText == "" {
			messageText = "detection failed"
		}
	} else if message.SkippedReason != "" {
		stage = "skipped"
		messageText = message.SkippedReason
	}
	details := map[string]interface{}{
		"success":         message.Success,
		"total_proposals": message.TotalProposals,
	}
	if message.SkippedReason != "" {
		details["skipped_reason"] = message.SkippedReason
	}
	if message.EligibleAfter != nil {
		details["eligible_after"] = message.EligibleAfter.AsTime().UTC().Format(time.RFC3339)
	}
	r.appendActivity(JobActivity{
		JobType:    resolvedJobType,
		RequestID:  message.RequestId,
		WorkerID:   resolvedWorkerID,
		Source:     "detector",
		Stage:      stage,
		Message:    messageText,
		OccurredAt: timeToPtr(time.Now().UTC()),
		Details:    details,
	})

	r.safeSendDetectionComplete(message.RequestId, message)
}

func (r *Plugin) safeSendDetectionComplete(requestID string, message *plugin_pb.DetectionComplete) {
	r.pendingDetectionMu.Lock()
	state, found := r.pendingDetection[requestID]
	var ch chan *plugin_pb.DetectionComplete
	if found && state != nil {
		ch = state.complete
	}
	r.pendingDetectionMu.Unlock()

	if ch != nil {
		safeSendCh(ch, message, r.shutdownCh)
	}
}

func (r *Plugin) handleJobCompleted(completed *plugin_pb.JobCompleted) {
	if completed == nil || completed.JobType == "" {
		return
	}

	if completed.RequestId != "" {
		r.safeSendJobCompleted(completed.RequestId, completed)
	}
	r.jobsMu.RLock()
	ended := attemptEnded(r.jobs[completed.JobId], completed.RequestId)
	r.jobsMu.RUnlock()
	if ended {
		return
	}

	tracked := r.trackExecutionCompletion(completed)
	workerID := ""
	if tracked != nil && tracked.WorkerID != "" {
		workerID = tracked.WorkerID
	}

	r.trackWorkerActivities(completed.JobType, completed.JobId, completed.RequestId, workerID, completed.Activities)

	record := &JobRunRecord{
		RunID:       completed.RequestId,
		JobID:       completed.JobId,
		JobType:     completed.JobType,
		WorkerID:    "",
		Outcome:     RunOutcomeError,
		CompletedAt: timeToPtr(time.Now().UTC()),
	}
	if completed.CompletedAt != nil {
		record.CompletedAt = timeToPtr(completed.CompletedAt.AsTime().UTC())
	}
	if completed.Success {
		record.Outcome = RunOutcomeSuccess
		record.Message = "completed"
		if completed.Result != nil && completed.Result.Summary != "" {
			record.Message = completed.Result.Summary
		}
	} else {
		record.Outcome = RunOutcomeError
		record.Message = completed.ErrorMessage
	}

	if tracked != nil {
		if workerID != "" {
			record.WorkerID = workerID
		}
		if tracked.CreatedAt != nil && record.CompletedAt != nil && record.CompletedAt.After(*tracked.CreatedAt) {
			record.DurationMs = int64(record.CompletedAt.Sub(*tracked.CreatedAt) / time.Millisecond)
		}
	}

	if err := r.store.AppendRunRecord(completed.JobType, record); err != nil {
		glog.Warningf("Plugin failed to append run record for %s: %v", completed.JobType, err)
	}
}

func (r *Plugin) safeSendJobCompleted(requestID string, completed *plugin_pb.JobCompleted) {
	var ch chan *plugin_pb.JobCompleted
	r.pendingExecutionMu.Lock()
	if pending := r.pendingExecution[requestID]; pending != nil {
		ch = pending.completed
		pending.completionSent = true
	}
	r.pendingExecutionMu.Unlock()
	safeSendCh(ch, completed, r.shutdownCh)
}

// The registry entry for a worker always describes the stream that is
// currently sendable for it: every registry write for a worker happens under
// sessionsMu, together with or checked against r.sessions[workerID].

func (r *Plugin) putSession(session *streamSession) {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()
	r.publishSessionLocked(session)
}

// registerSession publishes a stream and its hello as one step so a second
// stream for the same worker cannot leave the registry describing the older
// hello while only the newer stream is sendable.
func (r *Plugin) registerSession(session *streamSession, hello *plugin_pb.WorkerHello) {
	r.sessionsMu.Lock()
	defer r.sessionsMu.Unlock()
	r.registry.UpsertFromHello(hello)
	r.publishSessionLocked(session)
}

// publishSessionLocked installs the stream and closes the one it replaces.
// Anything waiting on the old stream's done wakes to a registry and session
// map that already describe the replacement.
func (r *Plugin) publishSessionLocked(session *streamSession) {
	old, exists := r.sessions[session.workerID]
	r.sessions[session.workerID] = session
	if exists && old != session {
		old.close()
	}
}

// updateRegistryFromSession applies a registry write for a stream's worker
// only while that stream is the registered one; a superseded stream's late
// hello or heartbeat must not describe the live stream.
func (r *Plugin) updateRegistryFromSession(session *streamSession, update func()) bool {
	r.sessionsMu.RLock()
	defer r.sessionsMu.RUnlock()
	if current, ok := r.sessions[session.workerID]; !ok || current != session {
		return false
	}
	update()
	return true
}

// cleanupSession ends one stream. Only the stream currently registered for
// the worker takes the registry entry down with it; a stream that a reconnect
// already replaced must not evict its successor. The registry entry goes
// before done closes so nothing woken by the closure can still pick it.
func (r *Plugin) cleanupSession(session *streamSession) {
	workerID := session.workerID

	r.sessionsMu.Lock()
	current, exists := r.sessions[workerID]
	superseded := exists && current != session
	if exists && !superseded {
		delete(r.sessions, workerID)
	}
	if !superseded {
		r.registry.Remove(workerID)
	}
	session.close()
	r.sessionsMu.Unlock()

	if superseded {
		glog.V(1).Infof("Plugin worker stream closed: %s (replaced by a newer session)", workerID)
		return
	}
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

func (r *Plugin) loadJobTypeConfigPayload(jobType string) (
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
	return adminRuntime, CloneConfigValueMap(config.AdminConfigValues), CloneConfigValueMap(config.WorkerConfigValues), nil
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
		out = append(out, proto.Clone(proposal).(*plugin_pb.JobProposal))
	}
	return out
}

func (r *Plugin) loadLastSuccessfulRun(jobType string) *timestamppb.Timestamp {
	history, err := r.store.LoadRunHistory(jobType)
	if err != nil {
		glog.Warningf("Plugin failed to load run history for %s: %v", jobType, err)
		return nil
	}
	if history == nil || len(history.SuccessfulRuns) == 0 {
		return nil
	}

	var latest time.Time
	for i := range history.SuccessfulRuns {
		completedAt := history.SuccessfulRuns[i].CompletedAt
		if completedAt == nil || completedAt.IsZero() {
			continue
		}
		if latest.IsZero() || completedAt.After(latest) {
			latest = *completedAt
		}
	}
	if latest.IsZero() {
		return nil
	}
	return timestamppb.New(latest.UTC())
}

func (r *Plugin) loadLastCompletedRun(jobType string) *timestamppb.Timestamp {
	history, err := r.store.LoadRunHistory(jobType)
	if err != nil {
		glog.Warningf("Plugin failed to load run history for %s: %v", jobType, err)
		return nil
	}
	if history == nil {
		return nil
	}

	var latest time.Time
	for i := range history.SuccessfulRuns {
		completedAt := history.SuccessfulRuns[i].CompletedAt
		if completedAt == nil || completedAt.IsZero() {
			continue
		}
		if latest.IsZero() || completedAt.After(latest) {
			latest = *completedAt
		}
	}
	for i := range history.ErrorRuns {
		completedAt := history.ErrorRuns[i].CompletedAt
		if completedAt == nil || completedAt.IsZero() {
			continue
		}
		if latest.IsZero() || completedAt.After(latest) {
			latest = *completedAt
		}
	}
	if latest.IsZero() {
		return nil
	}
	return timestamppb.New(latest.UTC())
}

func CloneConfigValueMap(in map[string]*plugin_pb.ConfigValue) map[string]*plugin_pb.ConfigValue {
	if len(in) == 0 {
		return map[string]*plugin_pb.ConfigValue{}
	}
	out := make(map[string]*plugin_pb.ConfigValue, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		out[key] = proto.Clone(value).(*plugin_pb.ConfigValue)
	}
	return out
}

func (s *streamSession) close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

// WorkerConnectForTest simulates a worker connecting (test helper).
func (r *Plugin) WorkerConnectForTest(hello *plugin_pb.WorkerHello) {
	r.registry.UpsertFromHello(hello)
}

// WorkerDisconnectForTest simulates a worker disconnecting (test helper).
func (r *Plugin) WorkerDisconnectForTest(workerID string) {
	if session, ok := r.getSession(workerID); ok {
		r.cleanupSession(session)
		return
	}
	r.registry.Remove(workerID)
}
