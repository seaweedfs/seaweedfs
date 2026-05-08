package s3_lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
// One Execute call drives a long-running scheduler.Scheduler against the
// S3 endpoints discovered from the master; admin caps concurrency at one
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
			Description: "Cluster-wide controls for the lifecycle scheduler.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "How many pipeline goroutines split the 16-shard space.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "workers",
							Label:       "Worker Count",
							Description: "Number of pipeline goroutines per executing worker. Each owns a contiguous slice of [0, 16) shards. Default 1 = one goroutine handles all 16 shards.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 16}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"workers": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultWorkers}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "s3-lifecycle-worker",
			Title:       "S3 Lifecycle Worker",
			Description: "Operational tuning for the lifecycle pipeline.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "cadence",
					Title:       "Cadence",
					Description: "Tick intervals for the dispatch loop, durable checkpoint, and engine refresh; plus the wall-clock cap on each daily run.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "dispatch_tick_minutes",
							Label:       "Dispatch Tick (minutes)",
							Description: "How often each pipeline drains its schedule.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "checkpoint_tick_seconds",
							Label:       "Cursor Checkpoint Tick (seconds)",
							Description: "How often each pipeline persists its cursor map to the filer.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "refresh_interval_minutes",
							Label:       "Engine Refresh Interval (minutes)",
							Description: "How often the scheduler rebuilds the engine snapshot from bucket lifecycle configs.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "max_runtime_minutes",
							Label:       "Max Runtime (minutes)",
							Description: "Wall-clock cap on each run. Each daily run processes events for one day; the cursor persists across runs.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"dispatch_tick_minutes":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDispatchTickMinutes}},
				"checkpoint_tick_seconds":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultCheckpointTickSeconds}},
				"refresh_interval_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultRefreshIntervalMinutes}},
				"max_runtime_minutes":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxRuntimeMinutes}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			DetectionIntervalSeconds: 24 * 60 * 60, // daily
			DetectionTimeoutSeconds:  60,
			MaxJobsPerDetection:      1,
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
		filerAddresses = append(filerAddresses, request.ClusterContext.FilerGrpcAddresses...)
	}
	if len(filerAddresses) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true})
	}

	proposal := &plugin_pb.JobProposal{
		JobType:    jobType,
		ProposalId: fmt.Sprintf("s3-lifecycle-%d", time.Now().UnixNano()),
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"filer_grpc_address": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: filerAddresses[0]}},
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

	runCtx, cancel := context.WithTimeout(ctx, cfg.MaxRuntime)
	defer cancel()

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId: request.Job.JobId, JobType: jobType,
		State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "starting",
		Message: fmt.Sprintf("scheduler workers=%d s3=%v runtime=%s", cfg.Workers, s3Endpoints, cfg.MaxRuntime),
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

	sched := &scheduler.Scheduler{
		BucketsPath:     bucketsPath,
		Engine:          engine.New(),
		Persister:       &dispatcher.FilerPersister{Store: dispatcher.NewFilerStoreClient(filerClient)},
		Client:          lifecycleRPCAdapter{c: rpc},
		FilerClient:     filerClient,
		ClientID:        util.RandomInt32(),
		ClientName:      "worker-s3-lifecycle",
		Workers:         cfg.Workers,
		DispatchTick:    cfg.DispatchTick,
		CheckpointTick:  cfg.CheckpointTick,
		RefreshInterval: cfg.RefreshInterval,
	}
	if err := sched.Run(runCtx); err != nil {
		glog.Warningf("s3 lifecycle execute: %v", err)
		return err
	}
	return nil
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
