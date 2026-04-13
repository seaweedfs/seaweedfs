package iceberg

import (
	"context"
	"fmt"
	"path"
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
		Aliases:  []string{"iceberg-maintenance", "iceberg.maintenance", "iceberg"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewHandler(opts.GrpcDialOption), nil
		},
	})
}

// Handler implements the JobHandler interface for Iceberg table maintenance:
// snapshot expiration, orphan file removal, and manifest rewriting.
type Handler struct {
	grpcDialOption grpc.DialOption
}

const filerConnectTimeout = 5 * time.Second

// NewHandler creates a new handler for iceberg table maintenance.
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
		DisplayName:             "Iceberg Maintenance",
		Description:             "Compacts data, rewrites delete files, expires snapshots, removes orphans, and rewrites manifests for Iceberg tables in S3 table buckets",
		Weight:                  50,
	}
}

func (h *Handler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "Iceberg Maintenance",
		Description:       "Automated maintenance for Iceberg tables: data compaction, delete-file rewrite, snapshot expiration, orphan removal, and manifest rewriting",
		Icon:              "fas fa-snowflake",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "iceberg-maintenance-admin",
			Title:       "Iceberg Maintenance Admin Config",
			Description: "Admin-side controls for Iceberg table maintenance scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Filters to restrict which tables are scanned for maintenance.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "bucket_filter",
							Label:       "Bucket Filter",
							Description: "Comma-separated wildcard patterns for table buckets (* and ? supported). Blank = all.",
							Placeholder: "prod-*, staging-*",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "namespace_filter",
							Label:       "Namespace Filter",
							Description: "Comma-separated wildcard patterns for namespaces (* and ? supported). Blank = all.",
							Placeholder: "analytics, events-*",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "table_filter",
							Label:       "Table Filter",
							Description: "Comma-separated wildcard patterns for table names (* and ? supported). Blank = all.",
							Placeholder: "clicks, orders-*",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
				{
					SectionId:   "resources",
					Title:       "Resource Groups",
					Description: "Controls for fair proposal distribution across buckets or namespaces.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "resource_group_by",
							Label:       "Group Proposals By",
							Description: "When set, detection emits proposals in round-robin order across the selected resource group.",
							Placeholder: "none, bucket, namespace, or bucket_namespace",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "max_tables_per_resource_group",
							Label:       "Max Tables Per Group",
							Description: "Optional cap on how many proposals a single resource group can receive in one detection run. Zero disables the cap.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"bucket_filter":                 {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"namespace_filter":              {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"table_filter":                  {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"resource_group_by":             {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: resourceGroupNone}},
				"max_tables_per_resource_group": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "iceberg-maintenance-worker",
			Title:       "Iceberg Maintenance Worker Config",
			Description: "Worker-side thresholds for maintenance operations.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "snapshots",
					Title:       "Snapshot Expiration",
					Description: "Controls for automatic snapshot cleanup.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "snapshot_retention_hours",
							Label:       "Retention (hours)",
							Description: "Expire snapshots older than this many hours.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "max_snapshots_to_keep",
							Label:       "Max Snapshots",
							Description: "Always keep at least this many most recent snapshots.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
				{
					SectionId:   "compaction",
					Title:       "Data Compaction",
					Description: "Controls for bin-packing or sorting small Parquet data files.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "target_file_size_mb",
							Label:       "Target File Size (MB)",
							Description: "Files smaller than this (in megabytes) are candidates for compaction.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "min_input_files",
							Label:       "Min Input Files",
							Description: "Minimum number of small files in a partition to trigger compaction.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
						},
						{
							Name:        "apply_deletes",
							Label:       "Apply Deletes",
							Description: "When true, compaction applies position and equality deletes to data files. When false, tables with delete manifests are skipped.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_BOOL,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TOGGLE,
						},
						{
							Name:        "rewrite_strategy",
							Label:       "Rewrite Strategy",
							Description: "binpack keeps the existing row order; sort rewrites each compaction bin using the Iceberg table sort order.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
							Placeholder: "binpack or sort",
						},
						{
							Name:        "sort_max_input_mb",
							Label:       "Sort Max Input (MB)",
							Description: "Optional hard cap for the total bytes in a sorted compaction bin. Zero = no extra cap beyond binning.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
				{
					SectionId:   "delete_rewrite",
					Title:       "Delete Rewrite",
					Description: "Controls for rewriting small position-delete files into fewer larger files.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "delete_target_file_size_mb",
							Label:       "Delete Target File Size (MB)",
							Description: "Target size for rewritten position-delete files.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "delete_min_input_files",
							Label:       "Delete Min Input Files",
							Description: "Minimum number of position-delete files in a group before rewrite is triggered.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
						},
						{
							Name:        "delete_max_file_group_size_mb",
							Label:       "Delete Max Group Size (MB)",
							Description: "Skip rewriting delete groups larger than this bound.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
						{
							Name:        "delete_max_output_files",
							Label:       "Delete Max Output Files",
							Description: "Maximum number of rewritten delete files a single group may produce.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
				{
					SectionId:   "orphans",
					Title:       "Orphan Removal",
					Description: "Controls for orphan file cleanup.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "orphan_older_than_hours",
							Label:       "Safety Window (hours)",
							Description: "Only remove orphan files older than this many hours.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
						},
					},
				},
				{
					SectionId:   "manifests",
					Title:       "Manifest Rewriting",
					Description: "Controls for merging small manifests.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "min_manifests_to_rewrite",
							Label:       "Min Manifests",
							Description: "Minimum number of manifests before rewriting is triggered.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
						},
					},
				},
				{
					SectionId:   "general",
					Title:       "General",
					Description: "General maintenance settings.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "max_commit_retries",
							Label:       "Max Commit Retries",
							Description: "Maximum number of commit retries on version conflict.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 20}},
						},
						{
							Name:        "operations",
							Label:       "Operations",
							Description: "Comma-separated list of operations to run: compact, rewrite_position_delete_files, expire_snapshots, remove_orphans, rewrite_manifests, or 'all'.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "where",
							Label:       "Where Filter",
							Description: "Optional partition filter for compact, rewrite_position_delete_files, and rewrite_manifests. Supports field = literal, field IN (...), and AND.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
							Placeholder: "region = 'us' AND dt IN ('2026-03-15')",
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"target_file_size_mb":           {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultTargetFileSizeMB}},
				"min_input_files":               {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinInputFiles}},
				"delete_target_file_size_mb":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteTargetFileSizeMB}},
				"delete_min_input_files":        {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMinInputFiles}},
				"delete_max_file_group_size_mb": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMaxGroupSizeMB}},
				"delete_max_output_files":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMaxOutputFiles}},
				"min_manifests_to_rewrite":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinManifestsToRewrite}},
				"snapshot_retention_hours":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultSnapshotRetentionHours}},
				"max_snapshots_to_keep":         {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxSnapshotsToKeep}},
				"orphan_older_than_hours":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultOrphanOlderThanHours}},
				"max_commit_retries":            {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxCommitRetries}},
				"operations":                    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultOperations}},
				"apply_deletes":                 {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true}},
				"rewrite_strategy":              {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultRewriteStrategy}},
				"sort_max_input_mb":             {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
				"where":                         {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       false, // disabled by default
			DetectionIntervalSeconds:      3600,  // 1 hour
			DetectionTimeoutSeconds:       300,
			MaxJobsPerDetection:           100,
			GlobalExecutionConcurrency:    4,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           60,
			JobTypeMaxRuntimeSeconds:      3600, // 1 hour max
			ExecutionTimeoutSeconds:       3600,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"target_file_size_mb":           {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultTargetFileSizeMB}},
			"min_input_files":               {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinInputFiles}},
			"delete_target_file_size_mb":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteTargetFileSizeMB}},
			"delete_min_input_files":        {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMinInputFiles}},
			"delete_max_file_group_size_mb": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMaxGroupSizeMB}},
			"delete_max_output_files":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultDeleteMaxOutputFiles}},
			"snapshot_retention_hours":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultSnapshotRetentionHours}},
			"max_snapshots_to_keep":         {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxSnapshotsToKeep}},
			"orphan_older_than_hours":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultOrphanOlderThanHours}},
			"max_commit_retries":            {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxCommitRetries}},
			"operations":                    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultOperations}},
			"apply_deletes":                 {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: true}},
			"rewrite_strategy":              {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultRewriteStrategy}},
			"sort_max_input_mb":             {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
			"where":                         {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
		},
	}
}

func (h *Handler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by iceberg maintenance handler", request.JobType)
	}

	workerConfig := ParseConfig(request.GetWorkerConfigValues())
	ops, err := parseOperations(workerConfig.Operations)
	if err != nil {
		return fmt.Errorf("invalid operations config: %w", err)
	}
	if err := validateWhereOperations(workerConfig.Where, ops); err != nil {
		return fmt.Errorf("invalid where config: %w", err)
	}

	// Detection interval is managed by the scheduler via AdminRuntimeDefaults.DetectionIntervalSeconds.

	// Get filer addresses from cluster context
	filerAddresses := make([]string, 0)
	if request.ClusterContext != nil {
		filerAddresses = append(filerAddresses, request.ClusterContext.FilerGrpcAddresses...)
	}
	if len(filerAddresses) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return h.sendEmptyDetection(sender)
	}

	// Read scope filters
	bucketFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "bucket_filter", ""))
	namespaceFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "namespace_filter", ""))
	tableFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "table_filter", ""))
	resourceGroups, err := readResourceGroupConfig(request.GetAdminConfigValues())
	if err != nil {
		return fmt.Errorf("invalid admin resource group config: %w", err)
	}

	// Connect to filer — try each address until one succeeds.
	filerAddress, conn, err := h.connectToFiler(ctx, filerAddresses)
	if err != nil {
		return fmt.Errorf("connect to filer: %w", err)
	}
	defer conn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(conn)

	maxResults := int(request.MaxResults)
	scanLimit := maxResults
	if resourceGroups.enabled() {
		scanLimit = 0
	}
	tables, err := h.scanTablesForMaintenance(ctx, filerClient, workerConfig, bucketFilter, namespaceFilter, tableFilter, scanLimit)
	if err != nil {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("scan_error", fmt.Sprintf("error scanning tables: %v", err), nil))
		return fmt.Errorf("scan tables: %w", err)
	}

	_ = sender.SendActivity(pluginworker.BuildDetectorActivity("scan_complete",
		fmt.Sprintf("found %d table(s) needing maintenance", len(tables)),
		map[string]*plugin_pb.ConfigValue{
			"tables_found": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(tables))}},
		}))

	tables, hasMore := selectTablesByResourceGroup(tables, resourceGroups, maxResults)

	proposals := make([]*plugin_pb.JobProposal, 0, len(tables))
	for _, t := range tables {
		proposal := h.buildMaintenanceProposal(t, filerAddress, resourceGroupKey(t, resourceGroups.GroupBy))
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   jobType,
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *Handler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by iceberg maintenance handler", request.Job.JobType)
	}
	canonicalJobType := request.Job.JobType
	if canonicalJobType == "" {
		canonicalJobType = jobType
	}

	params := request.Job.Parameters
	bucketName := readStringConfig(params, "bucket_name", "")
	namespace := readStringConfig(params, "namespace", "")
	tableName := readStringConfig(params, "table_name", "")
	tablePath := readStringConfig(params, "table_path", "")
	filerAddress := readStringConfig(params, "filer_address", "")

	if bucketName == "" || namespace == "" || tableName == "" || filerAddress == "" {
		return fmt.Errorf("missing required parameters: bucket_name=%q, namespace=%q, table_name=%q, filer_address=%q", bucketName, namespace, tableName, filerAddress)
	}
	// Reject path traversal in bucket/namespace/table names.
	for _, name := range []string{bucketName, namespace, tableName} {
		if strings.Contains(name, "..") || strings.ContainsAny(name, "/\\") {
			return fmt.Errorf("invalid name %q: must not contain path separators or '..'", name)
		}
	}
	if tablePath == "" {
		tablePath = path.Join(namespace, tableName)
	}
	// Sanitize tablePath to prevent directory traversal.
	tablePath = path.Clean(tablePath)
	expected := path.Join(namespace, tableName)
	if tablePath != expected && !strings.HasPrefix(tablePath, expected+"/") {
		return fmt.Errorf("invalid table_path %q: must be %q or a subpath", tablePath, expected)
	}

	workerConfig := ParseConfig(request.GetWorkerConfigValues())
	ops, opsErr := parseOperations(workerConfig.Operations)
	if opsErr != nil {
		return fmt.Errorf("invalid operations config: %w", opsErr)
	}
	if err := validateWhereOperations(workerConfig.Where, ops); err != nil {
		return fmt.Errorf("invalid where config: %w", err)
	}

	// Send initial progress
	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         canonicalJobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         fmt.Sprintf("maintenance job accepted for %s/%s/%s", bucketName, namespace, tableName),
		Activities: []*plugin_pb.ActivityEvent{
			pluginworker.BuildExecutorActivity("assigned", fmt.Sprintf("maintenance job accepted for %s/%s/%s", bucketName, namespace, tableName)),
		},
	}); err != nil {
		return err
	}

	// Connect to filer
	conn, err := h.dialFiler(ctx, filerAddress)
	if err != nil {
		return fmt.Errorf("connect to filer %s: %w", filerAddress, err)
	}
	defer conn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(conn)

	var results []string
	var lastErr error
	totalOps := len(ops)
	completedOps := 0
	allMetrics := make(map[string]int64)

	// Execute operations in canonical maintenance order as defined by
	// parseOperations.
	for _, op := range ops {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		progress := float64(completedOps) / float64(totalOps) * 100
		if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         canonicalJobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           op,
			Message:         fmt.Sprintf("running %s", op),
			Activities: []*plugin_pb.ActivityEvent{
				pluginworker.BuildExecutorActivity(op, fmt.Sprintf("starting %s for %s/%s/%s", op, bucketName, namespace, tableName)),
			},
		}); err != nil {
			return err
		}

		var opResult string
		var opErr error
		var opMetrics map[string]int64

		switch op {
		case "compact":
			opResult, opMetrics, opErr = h.compactDataFiles(ctx, filerClient, bucketName, tablePath, workerConfig, func(binIdx, totalBins int) {
				binProgress := progress + float64(binIdx+1)/float64(totalBins)*(100.0/float64(totalOps))
				_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
					JobId:           request.Job.JobId,
					JobType:         canonicalJobType,
					State:           plugin_pb.JobState_JOB_STATE_RUNNING,
					ProgressPercent: binProgress,
					Stage:           fmt.Sprintf("compact bin %d/%d", binIdx+1, totalBins),
					Message:         fmt.Sprintf("compacting bin %d of %d", binIdx+1, totalBins),
				})
			})
		case "rewrite_position_delete_files":
			opResult, opMetrics, opErr = h.rewritePositionDeleteFiles(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "expire_snapshots":
			opResult, opMetrics, opErr = h.expireSnapshots(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "remove_orphans":
			opResult, opMetrics, opErr = h.removeOrphans(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "rewrite_manifests":
			opResult, opMetrics, opErr = h.rewriteManifests(ctx, filerClient, bucketName, tablePath, workerConfig)
		default:
			glog.Warningf("unknown maintenance operation: %s", op)
			continue
		}

		// Accumulate per-operation metrics with dot-prefixed keys
		for k, v := range opMetrics {
			allMetrics[op+"."+k] = v
		}

		completedOps++
		if opErr != nil {
			glog.Warningf("iceberg maintenance %s failed for %s/%s/%s: %v", op, bucketName, namespace, tableName, opErr)
			results = append(results, fmt.Sprintf("%s: error: %v", op, opErr))
			lastErr = opErr
		} else {
			results = append(results, fmt.Sprintf("%s: %s", op, opResult))
		}
	}

	resultSummary := strings.Join(results, "; ")
	success := lastErr == nil

	// Build OutputValues with base table info + per-operation metrics
	outputValues := map[string]*plugin_pb.ConfigValue{
		"bucket":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: bucketName}},
		"namespace": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: namespace}},
		"table":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: tableName}},
	}
	for k, v := range allMetrics {
		outputValues[k] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}}
	}

	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: canonicalJobType,
		Success: success,
		ErrorMessage: func() string {
			if lastErr != nil {
				return lastErr.Error()
			}
			return ""
		}(),
		Result: &plugin_pb.JobResult{
			Summary:      resultSummary,
			OutputValues: outputValues,
		},
		Activities: []*plugin_pb.ActivityEvent{
			pluginworker.BuildExecutorActivity("completed", resultSummary),
		},
		CompletedAt: timestamppb.Now(),
	})
}

func (h *Handler) sendEmptyDetection(sender pluginworker.DetectionSender) error {
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

func (h *Handler) dialFiler(ctx context.Context, address string) (*grpc.ClientConn, error) {
	opCtx, opCancel := context.WithTimeout(ctx, filerConnectTimeout)
	defer opCancel()

	conn, err := pb.GrpcDial(opCtx, pb.ServerAddress(address).ToGrpcAddress(), false, h.grpcDialOption)
	if err != nil {
		return nil, err
	}

	client := filer_pb.NewSeaweedFilerClient(conn)
	if _, err := client.Ping(opCtx, &filer_pb.PingRequest{}); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// connectToFiler tries each filer address in order and returns the first
// address whose gRPC connection and Ping request succeed.
func (h *Handler) connectToFiler(ctx context.Context, addresses []string) (string, *grpc.ClientConn, error) {
	var lastErr error
	for _, addr := range addresses {
		conn, err := h.dialFiler(ctx, addr)
		if err != nil {
			lastErr = fmt.Errorf("filer %s: %w", addr, err)
			continue
		}
		return addr, conn, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no filer addresses provided")
	}
	return "", nil, lastErr
}

// Ensure Handler implements JobHandler.
var _ pluginworker.JobHandler = (*Handler)(nil)
