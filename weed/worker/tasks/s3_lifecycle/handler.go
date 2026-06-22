package s3_lifecycle

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  jobType,
		Category: pluginworker.CategoryDefault,
		Aliases:  []string{"s3-lifecycle", "s3.lifecycle", "lifecycle"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewHandler(opts.GrpcDialOption), nil
		},
	})
}

// Handler is the worker-side runner for S3 object lifecycle expiration.
// One Execute call drives one bounded dailyrun.Run pass against the S3
// endpoints discovered from the master; admin caps concurrency at one
// job per worker so a fresh proposal only spawns a new run after the
// prior one exits.
type Handler struct {
	grpcDialOption grpc.DialOption
}

func NewHandler(grpcDialOption grpc.DialOption) *Handler {
	return &Handler{grpcDialOption: grpcDialOption}
}

func (h *Handler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "S3 Lifecycle",
		Description:             "Daily batch: scan the filer meta-log and delete objects whose lifecycle rule has fired.",
		Weight:                  20,
	}
}

func (h *Handler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "S3 Lifecycle",
		Description:       "Daily S3 object expiration scan with per-shard cursors and bounded retry.",
		Icon:              "fas fa-recycle",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "s3-lifecycle-admin",
			Title:       "S3 Lifecycle",
			Description: "Cluster-wide delete-throughput cap.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Cluster-wide delete-throughput cap.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        ClusterDeletesPerSecondAdminKey,
							Label:       "Cluster Delete Rate (per second)",
							Description: "Cluster-wide ceiling on lifecycle delete RPCs per second, divided evenly across active s3_lifecycle workers at job-dispatch time. 0 = unlimited.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        ClusterDeletesBurstAdminKey,
							Label:       "Cluster Delete Burst",
							Description: "Token-bucket burst capacity across the cluster (max simultaneous deletes). 0 = 2 × rate. Same allocation rule as the rate.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        MetaLogRetentionDaysAdminKey,
							Label:       "Meta-Log Retention (days)",
							Description: "How far back the filer's meta-log subscription can reach. Rules whose TTL exceeds this run via the walker; shrinking this value will trigger a one-time recovery walk on the next run for any rule that's now too old to replay. 0 = unbounded (no partition; every rule serviced by replay).",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        WalkerIntervalMinutesAdminKey,
							Label:       "Walker Interval (minutes)",
							Description: "Minimum time between steady-state walker fires per shard. Cold-start and rule-change recovery walks ignore this — they run unconditionally. 0 = fire on every run (use when the worker is scheduled at the desired walk cadence, e.g. hourly). Set to a positive value when the worker runs at a tighter cadence than the desired walk frequency, to avoid hammering filer with a full subtree scan per run.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				ClusterDeletesPerSecondAdminKey: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
				ClusterDeletesBurstAdminKey:     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
				MetaLogRetentionDaysAdminKey:    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
				WalkerIntervalMinutesAdminKey:   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
			},
		},
		// WorkerConfigForm intentionally absent — the prior
		// "Per-Run Time Limit (minutes)" knob duplicated the admin
		// scheduler's Execution Timeout (both wall-clock caps on the
		// same Execute call), and operators had to keep the two values
		// in agreement. Removed in favor of a single source of truth:
		// AdminRuntimeDefaults.ExecutionTimeoutSeconds below.
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			// On by default: S3 lifecycle is a standard bucket feature
			// (PutBucketLifecycleConfiguration is part of the S3 API),
			// and a bucket with rules set but no worker running silently
			// retains data past its declared expiration. Operators who
			// want the worker off can still disable it in the admin UI;
			// the default error is "data lingers" not "worker burns CPU
			// on empty rule sets" (the worker fast-exits with no
			// configured rules).
			Enabled:                  true,
			DetectionIntervalMinutes: 24 * 60, // daily
			DetectionTimeoutSeconds:  60,
			MaxJobsPerDetection:      1,
			// Effectively no per-pass wall-clock cap. Lifecycle is a
			// scheduled batch — its natural duration is "as long as it
			// takes to process today's events." The scheduler's global
			// 90s default would kill every real run, and a numeric
			// cap operators have to estimate (1h? 8h?) is a recurring
			// footgun: too low truncates a legitimate large-bucket
			// pass; too high makes the value meaningless.
			//
			// Use math.MaxInt32 seconds (~68 years) for both knobs to
			// say "no timeout in practice" in code-review-readable form.
			// Operators who genuinely want a cap can set one in the
			// admin UI; the underlying context.WithTimeout machinery
			// is unchanged.
			ExecutionTimeoutSeconds:  math.MaxInt32,
			JobTypeMaxRuntimeSeconds: math.MaxInt32,
		},
	}
}

func (h *Handler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil || sender == nil {
		return fmt.Errorf("detect: nil request or sender")
	}
	if request.JobType != "" && request.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by s3 lifecycle handler", request.JobType)
	}
	s3Endpoints := clusterS3Endpoints(request.ClusterContext)
	if len(s3Endpoints) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no s3 servers registered with master", nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true})
	}
	filerAddresses := []string{}
	if request.ClusterContext != nil {
		filerAddresses = append(filerAddresses, request.ClusterContext.FilerAddresses...)
	}
	if len(filerAddresses) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true})
	}

	// FilerAddresses are pb.ServerAddress strings (host:httpPort.grpcPort);
	// Execute dials filer_grpc_address verbatim, so resolve it to a gRPC address.
	filerGrpcAddress := pb.ServerAddress(filerAddresses[0]).ToGrpcAddress()
	proposal := &plugin_pb.JobProposal{
		JobType:    jobType,
		ProposalId: fmt.Sprintf("s3-lifecycle-%d", time.Now().UnixNano()),
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"filer_grpc_address": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: filerGrpcAddress}},
		},
	}
	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{proposal},
	}); err != nil {
		return err
	}
	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: 1,
	})
}

func (h *Handler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.Job == nil || sender == nil {
		return fmt.Errorf("execute: nil request/job/sender")
	}
	if request.Job.JobType != "" && request.Job.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by s3 lifecycle handler", request.Job.JobType)
	}
	cfg := ParseConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())
	s3Endpoints := clusterS3Endpoints(request.ClusterContext)
	if len(s3Endpoints) == 0 {
		return fmt.Errorf("execute: no s3 servers registered with master")
	}
	filerAddress := readString(request.Job.Parameters, "filer_grpc_address", "")
	if filerAddress == "" {
		return fmt.Errorf("execute: missing filer_grpc_address in job parameters")
	}

	// Run lifetime is bounded by the scheduler's Execution Timeout
	// (admin UI). The handler used to wrap ctx in another
	// context.WithTimeout(cfg.MaxRuntime), but that doubled the
	// concept — both knobs were wall-clock caps on the same Execute
	// call, and the smaller one always won (typically the 90s
	// scheduler default would clobber a 60-min worker setting).
	// Single source of truth is now AdminRuntimeDefaults.ExecutionTimeoutSeconds.
	runCtx := ctx

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId: request.Job.JobId, JobType: jobType,
		State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "starting",
		Message: fmt.Sprintf("scheduler workers=%d s3=%v", cfg.Workers, s3Endpoints),
	})

	dialCtx, dialCancel := context.WithTimeout(runCtx, 30*time.Second)
	filerConn, err := pb.GrpcDial(dialCtx, filerAddress, false, h.grpcDialOption)
	dialCancel()
	if err != nil {
		return fmt.Errorf("dial filer %s: %w", filerAddress, err)
	}
	defer filerConn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(filerConn)

	bucketsPath, err := lookupBucketsPath(runCtx, filerClient)
	if err != nil {
		return fmt.Errorf("buckets path: %w", err)
	}

	dialCtx, dialCancel = context.WithTimeout(runCtx, 30*time.Second)
	s3Conn, err := pb.GrpcDial(dialCtx, s3Endpoints[0], false, h.grpcDialOption)
	dialCancel()
	if err != nil {
		return fmt.Errorf("dial s3 %s: %w", s3Endpoints[0], err)
	}
	defer s3Conn.Close()
	rpc := s3_lifecycle_pb.NewSeaweedS3LifecycleInternalClient(s3Conn)

	if err := h.executeDailyReplay(runCtx, request, bucketsPath, filerClient, rpc, cfg, sender); err != nil {
		return err
	}

	return sendSuccessCompletion(request, sender)
}

const dailyReplaySuccessSummary = "s3 lifecycle daily replay completed"

// JobCompleted must set JobType to the handler's jobType constant;
// routed requests may leave request.Job.JobType empty, and the admin
// ignores completions with empty JobType.
func sendSuccessCompletion(request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: jobType,
		Success: true,
		Result: &plugin_pb.JobResult{
			Summary: dailyReplaySuccessSummary,
		},
	})
}

// executeDailyReplay runs one bounded daily-replay pass via
// dailyrun.Run. The walker fires inside runShard on rule-content edits
// and against the steady-state walk view; all rule kinds are serviced.
func (h *Handler) executeDailyReplay(ctx context.Context, request *plugin_pb.ExecuteJobRequest, bucketsPath string, filerClient filer_pb.SeaweedFilerClient, rpc s3_lifecycle_pb.SeaweedS3LifecycleInternalClient, cfg Config, sender pluginworker.ExecutionSender) error {
	eng := engine.New()
	inputs, parseErrors, err := scheduler.LoadCompileInputs(ctx, filerClient, bucketsPath)
	if err != nil {
		return fmt.Errorf("daily_replay: load lifecycle inputs: %w", err)
	}
	for _, pe := range parseErrors {
		glog.V(1).Infof("daily_replay: %s: %v", pe.Bucket, pe.Err)
	}
	eng.Compile(inputs, engine.CompileOptions{PriorStates: scheduler.AllActivePriorStates(inputs)})

	shards := make([]int, 0, s3lifecycle.ShardCount)
	for i := 0; i < s3lifecycle.ShardCount; i++ {
		shards = append(shards, i)
	}

	limiter, limiterDesc := buildLimiterFromClusterContext(request.GetClusterContext())

	// Reuse one LifecycleClient across the replay drain and the
	// walker's per-entry dispatch.
	client := lifecycleRPCAdapter{c: rpc}

	// Bucket list for the walker — derived from inputs so it matches
	// the snapshot the engine compiled.
	buckets := make([]string, 0, len(inputs))
	for _, in := range inputs {
		if in.Bucket != "" {
			buckets = append(buckets, in.Bucket)
		}
	}
	walkerListFn := dailyrun.FilerListFunc(filerClient, bucketsPath)
	// Share the limiter with processMatches so walker + replay can't
	// combine to burst past the cluster cap.
	walkerDispatch := &dailyrun.WalkerDispatcher{Client: client, Limiter: limiter}
	walker := dailyrun.WalkerFunc(func(walkCtx context.Context, view *engine.Snapshot, shardID int) error {
		return dailyrun.WalkBuckets(walkCtx, view, shardID, buckets, walkerListFn, walkerDispatch)
	})

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId: request.Job.JobId, JobType: jobType,
		State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "starting",
		Message: fmt.Sprintf("daily_replay shards=%d workers=%d rate=%s buckets=%d walker=on",
			len(shards), cfg.Workers, limiterDesc, len(buckets)),
	})

	runErr := dailyrun.Run(ctx, dailyrun.Config{
		Shards:          shards,
		BucketsPath:     bucketsPath,
		Engine:          eng,
		FilerClient:     filerClient,
		Client:          client,
		Persister:       &dailyrun.FilerCursorPersister{Store: dispatcher.NewFilerStoreClient(filerClient)},
		Lister:          dispatcher.NewFilerSiblingLister(filerClient, bucketsPath),
		Workers:         cfg.Workers,
		Limiter:         limiter,
		RetentionWindow: cfg.MetaLogRetention,
		Walker:          walker,
		WalkerInterval:  cfg.WalkerInterval,
		ClientName:      "worker-s3-lifecycle-daily",
	})
	if runErr != nil {
		glog.Warningf("daily_replay: %v", runErr)
		return runErr
	}
	return nil
}

// buildLimiterFromClusterContext parses the per-worker share the admin
// wrote into ClusterContext.Metadata (see weed/admin/plugin/plugin.go's
// s3_lifecycle injection) and returns a rate.Limiter, or nil when no
// rate cap applies. The description string is for the JobProgressUpdate
// so operators can see "rate=unlimited" / "rate=12.5/s burst=25" in
// the activity log.
//
// Tolerant of missing keys, empty strings, malformed numbers, and
// non-positive values — all treated as "no limit" rather than failing
// the run. The admin allocator is the single point that decides whether
// to populate these keys; the worker doesn't second-guess.
func buildLimiterFromClusterContext(cc *plugin_pb.ClusterContext) (*rate.Limiter, string) {
	if cc == nil || cc.Metadata == nil {
		return nil, "unlimited"
	}
	rps, ok := parsePositiveFloat(cc.Metadata[MetadataKeyDeletesPerSecond])
	if !ok {
		return nil, "unlimited"
	}
	burst, _ := parsePositiveInt(cc.Metadata[MetadataKeyDeletesBurst])
	if burst <= 0 {
		// Sensible default: enough headroom for one tick's worth of
		// throughput. Caller may also supply 0 to opt into this default.
		burst = int(rps * 2)
		if burst < 1 {
			burst = 1
		}
	}
	return rate.NewLimiter(rate.Limit(rps), burst), fmt.Sprintf("%.3g/s burst=%d", rps, burst)
}

func parsePositiveFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

func parsePositiveInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}

// clusterS3Endpoints returns the master-discovered S3 gRPC addresses for the
// cluster. The handler dials the first reachable one; the master refreshes
// the list on KeepConnected so a stale entry self-heals on the next run.
func clusterS3Endpoints(cc *plugin_pb.ClusterContext) []string {
	if cc == nil {
		return nil
	}
	out := make([]string, 0, len(cc.S3GrpcAddresses))
	for _, addr := range cc.S3GrpcAddresses {
		if addr != "" {
			out = append(out, addr)
		}
	}
	return out
}

type lifecycleRPCAdapter struct {
	c s3_lifecycle_pb.SeaweedS3LifecycleInternalClient
}

func (a lifecycleRPCAdapter) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	return a.c.LifecycleDelete(ctx, req)
}

func lookupBucketsPath(ctx context.Context, client filer_pb.SeaweedFilerClient) (string, error) {
	resp, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
	if err != nil {
		return "", err
	}
	if path := resp.GetDirBuckets(); path != "" {
		return path, nil
	}
	return "/buckets", nil
}

func readString(values map[string]*plugin_pb.ConfigValue, field, fallback string) string {
	v, ok := values[field]
	if !ok || v == nil {
		return fallback
	}
	if k, ok := v.Kind.(*plugin_pb.ConfigValue_StringValue); ok {
		return k.StringValue
	}
	return fallback
}

var _ pluginworker.JobHandler = (*Handler)(nil)
