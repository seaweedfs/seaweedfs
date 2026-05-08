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
// configured S3 endpoint(s); admin caps concurrency at one job per worker
// so a fresh proposal only spawns a new run after the prior one exits.
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
		Description:             "Event-driven S3 object expiration: scans the filer meta-log and deletes objects whose lifecycle rule has fired.",
		Weight:                  20,
	}
}

func (h *Handler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "S3 Lifecycle",
		Description:       "Event-driven S3 object expiration with per-shard cursors and bounded retry.",
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
					Description: "Where to dispatch lifecycle deletes and how many workers handle the load.",
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
						{
							Name:        "s3_grpc_endpoints",
							Label:       "S3 gRPC Endpoints",
							Description: "Comma-separated host:port list of S3 server gRPC endpoints. The scheduler dials the first reachable one for LifecycleDelete RPCs.",
							Placeholder: "localhost:18333",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"workers":           {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultWorkers}},
				"s3_grpc_endpoints": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
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
					Description: "Tick intervals for the dispatch loop and durable checkpoint.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "dispatch_tick_ms",
							Label:       "Dispatch Tick (ms)",
							Description: "How often each pipeline drains its schedule.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 100}},
						},
						{
							Name:        "checkpoint_tick_ms",
							Label:       "Cursor Checkpoint Tick (ms)",
							Description: "How often each pipeline persists its cursor map to the filer.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1000}},
						},
						{
							Name:        "refresh_interval_ms",
							Label:       "Engine Refresh Interval (ms)",
							Description: "How often the scheduler rebuilds the engine snapshot from bucket lifecycle configs.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1000}},
						},
						{
							Name:        "event_budget",
							Label:       "Event Budget",
							Description: "Soft cap on events processed per pipeline iteration before it loops. 0 = unbounded (recommended for the daemon model).",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"dispatch_tick_ms":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDispatchTickMs}},
				"checkpoint_tick_ms":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultCheckpointTickMs}},
				"refresh_interval_ms": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultRefreshIntervalMs}},
				"event_budget":        {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultEventBudget}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			DetectionIntervalSeconds: 300, // 5 minutes; cheap, just emits one proposal
			DetectionTimeoutSeconds:  60,
			MaxJobsPerDetection:      1, // single daemon-style job
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
	cfg := ParseConfig(request.GetAdminConfigValues(), request.GetWorkerConfigValues())
	if len(cfg.S3GrpcEndpoints) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no s3 grpc endpoints configured", nil))
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
	if len(cfg.S3GrpcEndpoints) == 0 {
		return fmt.Errorf("execute: no s3 grpc endpoints configured")
	}
	filerAddress := readString(request.Job.Parameters, "filer_grpc_address", "")
	if filerAddress == "" {
		return fmt.Errorf("execute: missing filer_grpc_address in job parameters")
	}

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId: request.Job.JobId, JobType: jobType,
		State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "starting",
		Message: fmt.Sprintf("scheduler workers=%d s3=%v", cfg.Workers, cfg.S3GrpcEndpoints),
	})

	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	filerConn, err := pb.GrpcDial(dialCtx, filerAddress, false, h.grpcDialOption)
	dialCancel()
	if err != nil {
		return fmt.Errorf("dial filer %s: %w", filerAddress, err)
	}
	defer filerConn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(filerConn)

	bucketsPath, err := lookupBucketsPath(ctx, filerClient)
	if err != nil {
		return fmt.Errorf("buckets path: %w", err)
	}

	dialCtx, dialCancel = context.WithTimeout(ctx, 30*time.Second)
	s3Conn, err := pb.GrpcDial(dialCtx, cfg.S3GrpcEndpoints[0], false, h.grpcDialOption)
	dialCancel()
	if err != nil {
		return fmt.Errorf("dial s3 %s: %w", cfg.S3GrpcEndpoints[0], err)
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
		EventBudget:     cfg.EventBudget,
		DispatchTick:    cfg.DispatchTick,
		CheckpointTick:  cfg.CheckpointTick,
		RefreshInterval: cfg.RefreshInterval,
	}
	if err := sched.Run(ctx); err != nil {
		glog.Warningf("s3 lifecycle execute: %v", err)
		return err
	}
	return nil
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

var _ pluginworker.JobHandler = (*Handler)(nil)
