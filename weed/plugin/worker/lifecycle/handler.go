package lifecycle

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  jobType,
		Category: pluginworker.CategoryHeavy,
		Aliases:  []string{"lifecycle", "s3-lifecycle", "s3.lifecycle"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewHandler(opts.GrpcDialOption), nil
		},
	})
}

// Handler implements the JobHandler interface for S3 lifecycle management:
// object expiration, delete marker cleanup, and abort incomplete multipart uploads.
type Handler struct {
	grpcDialOption grpc.DialOption
}

const filerConnectTimeout = 5 * time.Second

// NewHandler creates a new handler for S3 lifecycle management.
func NewHandler(grpcDialOption grpc.DialOption) *Handler {
	return &Handler{grpcDialOption: grpcDialOption}
}

func (h *Handler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 4,
		DisplayName:             "S3 Lifecycle",
		Description:             "Manages S3 object lifecycle: expiration of objects based on TTL rules, delete marker cleanup, and abort of incomplete multipart uploads",
		Weight:                  40,
	}
}

func (h *Handler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "S3 Lifecycle Management",
		Description:       "Automated S3 object lifecycle management: expire objects by TTL rules, clean up expired delete markers, and abort stale multipart uploads",
		Icon:              "fas fa-hourglass-half",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "s3-lifecycle-admin",
			Title:       "S3 Lifecycle Admin Config",
			Description: "Admin-side controls for S3 lifecycle management scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Which buckets to include in lifecycle management.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "bucket_filter",
							Label:       "Bucket Filter",
							Description: "Wildcard pattern for bucket names to include (e.g. \"prod-*\"). Empty means all buckets.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "s3-lifecycle-worker",
			Title:       "S3 Lifecycle Worker Config",
			Description: "Worker-side controls for lifecycle execution behavior.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "execution",
					Title:       "Execution",
					Description: "Controls for lifecycle rule execution.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "batch_size",
							Label:       "Batch Size",
							Description: "Number of entries to process per filer listing page.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    configInt64(100),
							MaxValue:    configInt64(10000),
						},
						{
							Name:        "max_deletes_per_bucket",
							Label:       "Max Deletes Per Bucket",
							Description: "Maximum number of expired objects to delete per bucket in one execution run.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    configInt64(100),
							MaxValue:    configInt64(1000000),
						},
						{
							Name:        "dry_run",
							Label:       "Dry Run",
							Description: "When enabled, detect expired objects but do not delete them.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
						{
							Name:        "delete_marker_cleanup",
							Label:       "Delete Marker Cleanup",
							Description: "Remove expired delete markers that have no non-current versions.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
						{
							Name:        "abort_mpu_days",
							Label:       "Abort Incomplete MPU (days)",
							Description: "Abort incomplete multipart uploads older than this many days. 0 disables.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    configInt64(0),
							MaxValue:    configInt64(365),
						},
					},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      300, // 5 minutes
			DetectionTimeoutSeconds:       60,
			MaxJobsPerDetection:           100,
			GlobalExecutionConcurrency:    2,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           10,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"batch_size":             {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultBatchSize}},
			"max_deletes_per_bucket": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxDeletesPerBucket}},
			"dry_run":                {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: defaultDryRun}},
			"delete_marker_cleanup":  {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: defaultDeleteMarkerCleanup}},
			"abort_mpu_days":         {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultAbortMPUDaysDefault}},
		},
	}
}

func (h *Handler) Detect(ctx context.Context, req *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if req == nil {
		return fmt.Errorf("nil detection request")
	}

	config := ParseConfig(req.WorkerConfigValues)

	bucketFilter := readStringConfig(req.AdminConfigValues, "bucket_filter", "")

	filerAddresses := filerAddressesFromCluster(req.ClusterContext)
	if len(filerAddresses) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return sendEmptyDetection(sender)
	}

	_ = sender.SendActivity(pluginworker.BuildDetectorActivity("connecting", "connecting to filer", nil))

	filerClient, filerConn, err := connectToFiler(ctx, filerAddresses, h.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to any filer: %v", err)
	}
	defer filerConn.Close()

	maxResults := int(req.MaxResults)
	if maxResults <= 0 {
		maxResults = 100
	}

	_ = sender.SendActivity(pluginworker.BuildDetectorActivity("scanning", "scanning buckets for lifecycle rules", nil))
	proposals, err := h.detectBucketsWithLifecycleRules(ctx, filerClient, config, bucketFilter, maxResults)
	if err != nil {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("scan_error", fmt.Sprintf("error scanning buckets: %v", err), nil))
		return fmt.Errorf("detect lifecycle rules: %w", err)
	}

	_ = sender.SendActivity(pluginworker.BuildDetectorActivity("scan_complete",
		fmt.Sprintf("found %d bucket(s) with lifecycle rules", len(proposals)),
		map[string]*plugin_pb.ConfigValue{
			"buckets_found": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(proposals))}},
		}))

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   jobType,
		Proposals: proposals,
		HasMore:   len(proposals) >= maxResults,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *Handler) Execute(ctx context.Context, req *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if req == nil || req.Job == nil {
		return fmt.Errorf("nil execution request")
	}

	job := req.Job
	config := ParseConfig(req.WorkerConfigValues)

	bucket := readParamString(job.Parameters, "bucket")
	bucketsPath := readParamString(job.Parameters, "buckets_path")
	if bucket == "" || bucketsPath == "" {
		return fmt.Errorf("missing bucket or buckets_path parameter")
	}

	filerAddresses := filerAddressesFromCluster(req.ClusterContext)
	if len(filerAddresses) == 0 {
		return fmt.Errorf("no filer addresses in cluster context")
	}

	filerClient, filerConn, err := connectToFiler(ctx, filerAddresses, h.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to any filer: %v", err)
	}
	defer filerConn.Close()

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           job.JobId,
		JobType:         jobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "starting",
		Message:         fmt.Sprintf("executing lifecycle rules for bucket %s", bucket),
	})

	start := time.Now()
	result, execErr := h.executeLifecycleForBucket(ctx, filerClient, config, bucket, bucketsPath, sender, job.JobId)
	elapsed := time.Since(start)

	metrics := map[string]*plugin_pb.ConfigValue{
		MetricDurationMs: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: elapsed.Milliseconds()}},
	}
	if result != nil {
		metrics[MetricObjectsExpired] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: result.objectsExpired}}
		metrics[MetricObjectsScanned] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: result.objectsScanned}}
		metrics[MetricDeleteMarkersClean] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: result.deleteMarkersClean}}
		metrics[MetricErrors] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: result.errors}}
	}

	success := execErr == nil
	message := fmt.Sprintf("bucket %s: scanned %d objects, expired %d", bucket, result.objectsScanned, result.objectsExpired)
	if config.DryRun {
		message += " (dry run)"
	}
	if execErr != nil {
		message = fmt.Sprintf("lifecycle execution failed for bucket %s: %v", bucket, execErr)
	}

	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
	}

	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:        job.JobId,
		JobType:      jobType,
		Success:      success,
		ErrorMessage: errMsg,
		Result: &plugin_pb.JobResult{
			Summary:      message,
			OutputValues: metrics,
		},
		CompletedAt: timestamppb.Now(),
	})
}

func connectToFiler(ctx context.Context, addresses []string, dialOption grpc.DialOption) (filer_pb.SeaweedFilerClient, *grpc.ClientConn, error) {
	var lastErr error
	for _, addr := range addresses {
		// Addresses from ClusterContext use ServerAddress format
		// (e.g. "host:port.grpcPort"). Convert to the actual gRPC
		// address before dialing.
		grpcAddr := pb.ServerAddress(addr).ToGrpcAddress()
		connCtx, cancel := context.WithTimeout(ctx, filerConnectTimeout)
		conn, err := pb.GrpcDial(connCtx, grpcAddr, false, dialOption)
		cancel()
		if err != nil {
			lastErr = err
			glog.V(1).Infof("s3_lifecycle: failed to connect to filer %s (grpc %s): %v", addr, grpcAddr, err)
			continue
		}
		// Verify the connection with a ping.
		client := filer_pb.NewSeaweedFilerClient(conn)
		pingCtx, pingCancel := context.WithTimeout(ctx, filerConnectTimeout)
		_, pingErr := client.Ping(pingCtx, &filer_pb.PingRequest{})
		pingCancel()
		if pingErr != nil {
			_ = conn.Close()
			lastErr = pingErr
			glog.V(1).Infof("s3_lifecycle: filer %s ping failed: %v", grpcAddr, pingErr)
			continue
		}
		return client, conn, nil
	}
	return nil, nil, lastErr
}

func sendEmptyDetection(sender pluginworker.DetectionSender) error {
	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   jobType,
		Proposals: []*plugin_pb.JobProposal{},
		HasMore:   false,
	}); err != nil {
		return err
	}
	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: 0,
	})
}

func filerAddressesFromCluster(cc *plugin_pb.ClusterContext) []string {
	if cc == nil {
		return nil
	}
	var addrs []string
	for _, addr := range cc.FilerGrpcAddresses {
		if strings.TrimSpace(addr) != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func readParamString(params map[string]*plugin_pb.ConfigValue, key string) string {
	if params == nil {
		return ""
	}
	v := params[key]
	if v == nil {
		return ""
	}
	if sv, ok := v.Kind.(*plugin_pb.ConfigValue_StringValue); ok {
		return sv.StringValue
	}
	return ""
}

func configInt64(v int64) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}}
}
