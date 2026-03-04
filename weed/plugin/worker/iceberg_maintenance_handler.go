package pluginworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	icebergMaintenanceJobType = "iceberg_maintenance"

	defaultSnapshotRetentionHours = 168 // 7 days
	defaultMaxSnapshotsToKeep     = 5
	defaultOrphanOlderThanHours   = 72
	defaultMaxCommitRetries       = 5
	defaultTargetFileSizeBytes    = 256 * 1024 * 1024
	defaultMinInputFiles          = 5
	defaultOperations             = "all"
)

// IcebergMaintenanceHandler implements the JobHandler interface for
// Iceberg table maintenance: snapshot expiration, orphan file removal,
// and manifest rewriting.
type IcebergMaintenanceHandler struct {
	grpcDialOption grpc.DialOption
}

// NewIcebergMaintenanceHandler creates a new handler for iceberg table maintenance.
func NewIcebergMaintenanceHandler(grpcDialOption grpc.DialOption) *IcebergMaintenanceHandler {
	return &IcebergMaintenanceHandler{grpcDialOption: grpcDialOption}
}

// icebergMaintenanceConfig holds parsed worker config values.
type icebergMaintenanceConfig struct {
	SnapshotRetentionHours int64
	MaxSnapshotsToKeep     int64
	OrphanOlderThanHours   int64
	MaxCommitRetries       int64
	TargetFileSizeBytes    int64
	MinInputFiles          int64
	Operations             string
}

// tableInfo captures metadata about a table for detection/execution.
type tableInfo struct {
	BucketName string
	Namespace  string
	TableName  string
	TablePath  string // namespace/tableName
	Metadata   table.Metadata
}

func (h *IcebergMaintenanceHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 icebergMaintenanceJobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 4,
		DisplayName:             "Iceberg Maintenance",
		Description:             "Compacts, expires snapshots, removes orphans, and rewrites manifests for Iceberg tables in S3 table buckets",
		Weight:                  50,
	}
}

func (h *IcebergMaintenanceHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           icebergMaintenanceJobType,
		DisplayName:       "Iceberg Maintenance",
		Description:       "Automated maintenance for Iceberg tables: snapshot expiration, orphan removal, manifest rewriting",
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
							Description: "Only maintain tables in this table bucket (blank = all).",
							Placeholder: "all buckets",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "namespace_filter",
							Label:       "Namespace Filter",
							Description: "Only maintain tables in this namespace (blank = all).",
							Placeholder: "all namespaces",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
						{
							Name:        "table_filter",
							Label:       "Table Filter",
							Description: "Only maintain this specific table (blank = all).",
							Placeholder: "all tables",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"bucket_filter":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"namespace_filter": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
				"table_filter":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""}},
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
					Description: "Controls for bin-packing small Parquet data files.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "target_file_size_bytes",
							Label:       "Target File Size (bytes)",
							Description: "Files smaller than this are candidates for compaction.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1024 * 1024}},
						},
						{
							Name:        "min_input_files",
							Label:       "Min Input Files",
							Description: "Minimum number of small files in a partition to trigger compaction.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
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
							Description: "Comma-separated list of operations to run: compact, expire_snapshots, remove_orphans, rewrite_manifests, or 'all'.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"target_file_size_bytes":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultTargetFileSizeBytes}},
				"min_input_files":          {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinInputFiles}},
				"snapshot_retention_hours": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultSnapshotRetentionHours}},
				"max_snapshots_to_keep":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxSnapshotsToKeep}},
				"orphan_older_than_hours":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultOrphanOlderThanHours}},
				"max_commit_retries":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxCommitRetries}},
				"operations":               {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultOperations}},
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
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"target_file_size_bytes":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultTargetFileSizeBytes}},
			"min_input_files":          {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMinInputFiles}},
			"snapshot_retention_hours": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultSnapshotRetentionHours}},
			"max_snapshots_to_keep":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxSnapshotsToKeep}},
			"orphan_older_than_hours":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultOrphanOlderThanHours}},
			"max_commit_retries":       {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: defaultMaxCommitRetries}},
			"operations":               {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultOperations}},
		},
	}
}

func (h *IcebergMaintenanceHandler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender DetectionSender) error {
	if request == nil {
		return fmt.Errorf("run detection request is nil")
	}
	if sender == nil {
		return fmt.Errorf("detection sender is nil")
	}
	if request.JobType != "" && request.JobType != icebergMaintenanceJobType {
		return fmt.Errorf("job type %q is not handled by iceberg maintenance handler", request.JobType)
	}

	workerConfig := parseIcebergMaintenanceConfig(request.GetWorkerConfigValues())

	if shouldSkipDetectionByInterval(request.GetLastSuccessfulRun(), 0) {
		return h.sendEmptyDetection(sender)
	}

	// Get filer addresses from cluster context
	filerAddresses := make([]string, 0)
	if request.ClusterContext != nil {
		filerAddresses = append(filerAddresses, request.ClusterContext.FilerGrpcAddresses...)
	}
	if len(filerAddresses) == 0 {
		_ = sender.SendActivity(buildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return h.sendEmptyDetection(sender)
	}

	// Read scope filters
	bucketFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "bucket_filter", ""))
	namespaceFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "namespace_filter", ""))
	tableFilter := strings.TrimSpace(readStringConfig(request.GetAdminConfigValues(), "table_filter", ""))

	// Connect to filer to scan table buckets
	filerAddress := filerAddresses[0]
	conn, err := grpc.NewClient(filerAddress, h.grpcDialOption)
	if err != nil {
		return fmt.Errorf("connect to filer %s: %w", filerAddress, err)
	}
	defer conn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(conn)

	tables, err := h.scanTablesForMaintenance(ctx, filerClient, workerConfig, bucketFilter, namespaceFilter, tableFilter)
	if err != nil {
		_ = sender.SendActivity(buildDetectorActivity("scan_error", fmt.Sprintf("error scanning tables: %v", err), nil))
		return fmt.Errorf("scan tables: %w", err)
	}

	_ = sender.SendActivity(buildDetectorActivity("scan_complete",
		fmt.Sprintf("found %d table(s) needing maintenance", len(tables)),
		map[string]*plugin_pb.ConfigValue{
			"tables_found": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(tables))}},
		}))

	maxResults := int(request.MaxResults)
	hasMore := false
	if maxResults > 0 && len(tables) > maxResults {
		hasMore = true
		tables = tables[:maxResults]
	}

	proposals := make([]*plugin_pb.JobProposal, 0, len(tables))
	for _, t := range tables {
		proposal := h.buildMaintenanceProposal(t, filerAddress)
		proposals = append(proposals, proposal)
	}

	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   icebergMaintenanceJobType,
		Proposals: proposals,
		HasMore:   hasMore,
	}); err != nil {
		return err
	}

	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        icebergMaintenanceJobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

func (h *IcebergMaintenanceHandler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender ExecutionSender) error {
	if request == nil || request.Job == nil {
		return fmt.Errorf("execute request/job is nil")
	}
	if sender == nil {
		return fmt.Errorf("execution sender is nil")
	}
	if request.Job.JobType != "" && request.Job.JobType != icebergMaintenanceJobType {
		return fmt.Errorf("job type %q is not handled by iceberg maintenance handler", request.Job.JobType)
	}

	params := request.Job.Parameters
	bucketName := readStringConfig(params, "bucket_name", "")
	namespace := readStringConfig(params, "namespace", "")
	tableName := readStringConfig(params, "table_name", "")
	tablePath := readStringConfig(params, "table_path", "")
	filerAddress := readStringConfig(params, "filer_address", "")

	if bucketName == "" || tableName == "" || filerAddress == "" {
		return fmt.Errorf("missing required parameters: bucket_name=%q, table_name=%q, filer_address=%q", bucketName, tableName, filerAddress)
	}

	workerConfig := parseIcebergMaintenanceConfig(request.GetWorkerConfigValues())
	ops := parseOperations(workerConfig.Operations)

	// Send initial progress
	if err := sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           request.Job.JobId,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_ASSIGNED,
		ProgressPercent: 0,
		Stage:           "assigned",
		Message:         fmt.Sprintf("maintenance job accepted for %s/%s/%s", bucketName, namespace, tableName),
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("assigned", fmt.Sprintf("maintenance job accepted for %s/%s/%s", bucketName, namespace, tableName)),
		},
	}); err != nil {
		return err
	}

	// Connect to filer
	conn, err := grpc.NewClient(filerAddress, h.grpcDialOption)
	if err != nil {
		return fmt.Errorf("connect to filer %s: %w", filerAddress, err)
	}
	defer conn.Close()
	filerClient := filer_pb.NewSeaweedFilerClient(conn)

	var results []string
	var lastErr error
	totalOps := len(ops)
	completedOps := 0

	// Execute operations in correct Iceberg maintenance order:
	// expire_snapshots → remove_orphans → rewrite_manifests
	for _, op := range ops {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		progress := float64(completedOps) / float64(totalOps) * 100
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           request.Job.JobId,
			JobType:         request.Job.JobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           op,
			Message:         fmt.Sprintf("running %s", op),
			Activities: []*plugin_pb.ActivityEvent{
				buildExecutorActivity(op, fmt.Sprintf("starting %s for %s/%s/%s", op, bucketName, namespace, tableName)),
			},
		})

		var opResult string
		var opErr error

		switch op {
		case "compact":
			opResult, opErr = h.compactDataFiles(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "expire_snapshots":
			opResult, opErr = h.expireSnapshots(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "remove_orphans":
			opResult, opErr = h.removeOrphans(ctx, filerClient, bucketName, tablePath, workerConfig)
		case "rewrite_manifests":
			opResult, opErr = h.rewriteManifests(ctx, filerClient, bucketName, tablePath, workerConfig)
		default:
			glog.Warningf("unknown maintenance operation: %s", op)
			continue
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

	return sender.SendCompleted(&plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: request.Job.JobType,
		Success: success,
		ErrorMessage: func() string {
			if lastErr != nil {
				return lastErr.Error()
			}
			return ""
		}(),
		Result: &plugin_pb.JobResult{
			Summary: resultSummary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"bucket":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: bucketName}},
				"namespace": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: namespace}},
				"table":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: tableName}},
			},
		},
		Activities: []*plugin_pb.ActivityEvent{
			buildExecutorActivity("completed", resultSummary),
		},
		CompletedAt: timestamppb.Now(),
	})
}

// scanTablesForMaintenance enumerates table buckets and their tables,
// evaluating which ones need maintenance based on metadata thresholds.
func (h *IcebergMaintenanceHandler) scanTablesForMaintenance(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	config icebergMaintenanceConfig,
	bucketFilter, namespaceFilter, tableFilter string,
) ([]tableInfo, error) {
	var tables []tableInfo

	// List entries under /buckets to find table buckets
	bucketsPath := s3tables.TablesPath
	bucketEntries, err := listFilerEntries(ctx, filerClient, bucketsPath, "")
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	for _, bucketEntry := range bucketEntries {
		if !bucketEntry.IsDirectory || !s3tables.IsTableBucketEntry(bucketEntry) {
			continue
		}
		bucketName := bucketEntry.Name
		if bucketFilter != "" && bucketName != bucketFilter {
			continue
		}

		// List namespaces within the bucket
		bucketPath := path.Join(bucketsPath, bucketName)
		nsEntries, err := listFilerEntries(ctx, filerClient, bucketPath, "")
		if err != nil {
			glog.Warningf("iceberg maintenance: failed to list namespaces in bucket %s: %v", bucketName, err)
			continue
		}

		for _, nsEntry := range nsEntries {
			if !nsEntry.IsDirectory {
				continue
			}
			nsName := nsEntry.Name
			if namespaceFilter != "" && nsName != namespaceFilter {
				continue
			}
			// Skip internal directories
			if strings.HasPrefix(nsName, ".") {
				continue
			}

			// List tables within the namespace
			nsPath := path.Join(bucketPath, nsName)
			tableEntries, err := listFilerEntries(ctx, filerClient, nsPath, "")
			if err != nil {
				glog.Warningf("iceberg maintenance: failed to list tables in %s/%s: %v", bucketName, nsName, err)
				continue
			}

			for _, tableEntry := range tableEntries {
				if !tableEntry.IsDirectory {
					continue
				}
				tblName := tableEntry.Name
				if tableFilter != "" && tblName != tableFilter {
					continue
				}

				// Check if this entry has table metadata
				metadataBytes, ok := tableEntry.Extended[s3tables.ExtendedKeyMetadata]
				if !ok || len(metadataBytes) == 0 {
					continue
				}

				// Parse the internal metadata to get FullMetadata
				var internalMeta struct {
					Metadata *struct {
						FullMetadata json.RawMessage `json:"fullMetadata,omitempty"`
					} `json:"metadata,omitempty"`
				}
				if err := json.Unmarshal(metadataBytes, &internalMeta); err != nil {
					glog.V(2).Infof("iceberg maintenance: skipping %s/%s/%s: cannot parse metadata: %v", bucketName, nsName, tblName, err)
					continue
				}
				if internalMeta.Metadata == nil || len(internalMeta.Metadata.FullMetadata) == 0 {
					continue
				}

				icebergMeta, err := table.ParseMetadataBytes(internalMeta.Metadata.FullMetadata)
				if err != nil {
					glog.V(2).Infof("iceberg maintenance: skipping %s/%s/%s: cannot parse iceberg metadata: %v", bucketName, nsName, tblName, err)
					continue
				}

				if needsMaintenance(icebergMeta, config) {
					tables = append(tables, tableInfo{
						BucketName: bucketName,
						Namespace:  nsName,
						TableName:  tblName,
						TablePath:  path.Join(nsName, tblName),
						Metadata:   icebergMeta,
					})
				}
			}
		}
	}

	return tables, nil
}

// needsMaintenance checks if a table needs any maintenance based on
// metadata-only thresholds (no manifest reading).
func needsMaintenance(meta table.Metadata, config icebergMaintenanceConfig) bool {
	snapshots := meta.Snapshots()
	if len(snapshots) == 0 {
		return false
	}

	// Check snapshot count
	if int64(len(snapshots)) > config.MaxSnapshotsToKeep {
		return true
	}

	// Check oldest snapshot age
	retentionMs := config.SnapshotRetentionHours * 3600 * 1000
	nowMs := time.Now().UnixMilli()
	for _, snap := range snapshots {
		if nowMs-snap.TimestampMs > retentionMs {
			return true
		}
	}

	return false
}

// buildMaintenanceProposal creates a JobProposal for a table needing maintenance.
func (h *IcebergMaintenanceHandler) buildMaintenanceProposal(t tableInfo, filerAddress string) *plugin_pb.JobProposal {
	dedupeKey := fmt.Sprintf("iceberg_maintenance:%s/%s/%s", t.BucketName, t.Namespace, t.TableName)

	snapshotCount := len(t.Metadata.Snapshots())
	summary := fmt.Sprintf("Maintain %s/%s/%s (%d snapshots)", t.BucketName, t.Namespace, t.TableName, snapshotCount)

	return &plugin_pb.JobProposal{
		ProposalId: fmt.Sprintf("iceberg-%s-%s-%s-%d", t.BucketName, t.Namespace, t.TableName, time.Now().UnixMilli()),
		DedupeKey:  dedupeKey,
		JobType:    icebergMaintenanceJobType,
		Priority:   plugin_pb.JobPriority_JOB_PRIORITY_NORMAL,
		Summary:    summary,
		Parameters: map[string]*plugin_pb.ConfigValue{
			"bucket_name":   {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: t.BucketName}},
			"namespace":     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: t.Namespace}},
			"table_name":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: t.TableName}},
			"table_path":    {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: t.TablePath}},
			"filer_address": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: filerAddress}},
		},
		Labels: map[string]string{
			"bucket":    t.BucketName,
			"namespace": t.Namespace,
			"table":     t.TableName,
		},
	}
}

// ---------------------------------------------------------------------------
// Operation: Expire Snapshots
// ---------------------------------------------------------------------------

// expireSnapshots removes old snapshots from the table metadata and cleans up
// their manifest list files.
func (h *IcebergMaintenanceHandler) expireSnapshots(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config icebergMaintenanceConfig,
) (string, error) {
	// Load current metadata
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", fmt.Errorf("load metadata: %w", err)
	}

	snapshots := meta.Snapshots()
	if len(snapshots) == 0 {
		return "no snapshots", nil
	}

	// Determine which snapshots to expire
	currentSnap := meta.CurrentSnapshot()
	var currentSnapID int64
	if currentSnap != nil {
		currentSnapID = currentSnap.SnapshotID
	}

	retentionMs := config.SnapshotRetentionHours * 3600 * 1000
	nowMs := time.Now().UnixMilli()

	// Sort snapshots by timestamp (most recent first) for keep-count logic
	type snapInfo struct {
		snap   table.Snapshot
		age    int64
		expire bool
	}
	infos := make([]snapInfo, 0, len(snapshots))
	for _, s := range snapshots {
		infos = append(infos, snapInfo{snap: s, age: nowMs - s.TimestampMs})
	}

	// Mark expired snapshots, but never the current snapshot
	var toExpire []int64
	kept := 0
	for i := len(infos) - 1; i >= 0; i-- {
		si := &infos[i]
		if si.snap.SnapshotID == currentSnapID {
			kept++
			continue
		}
		// Count how many we'd keep if we don't expire this one
		wouldKeep := kept + 1
		for j := i - 1; j >= 0; j-- {
			wouldKeep++
		}
		if wouldKeep <= int(config.MaxSnapshotsToKeep) {
			// Would go below minimum, keep it
			kept++
			continue
		}
		if si.age > retentionMs {
			si.expire = true
			toExpire = append(toExpire, si.snap.SnapshotID)
		} else {
			kept++
		}
	}

	if len(toExpire) == 0 {
		return "no snapshots expired", nil
	}

	// Collect manifest list files to clean up after commit
	var manifestListsToDelete []string
	for _, si := range infos {
		if si.expire && si.snap.ManifestList != "" {
			manifestListsToDelete = append(manifestListsToDelete, si.snap.ManifestList)
		}
	}

	// Use MetadataBuilder to remove snapshots and create new metadata
	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		return builder.RemoveSnapshots(toExpire)
	})
	if err != nil {
		return "", fmt.Errorf("commit snapshot expiration: %w", err)
	}

	// Clean up expired manifest list files (best-effort)
	for _, mlPath := range manifestListsToDelete {
		fileName := path.Base(mlPath)
		metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
		if delErr := deleteFilerFile(ctx, filerClient, metaDir, fileName); delErr != nil {
			glog.Warningf("iceberg maintenance: failed to delete expired manifest list %s: %v", mlPath, delErr)
		}
	}

	return fmt.Sprintf("expired %d snapshot(s)", len(toExpire)), nil
}

// ---------------------------------------------------------------------------
// Operation: Remove Orphans
// ---------------------------------------------------------------------------

// removeOrphans finds and deletes unreferenced files from the table's
// metadata/ and data/ directories.
func (h *IcebergMaintenanceHandler) removeOrphans(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config icebergMaintenanceConfig,
) (string, error) {
	// Load current metadata
	meta, _, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", fmt.Errorf("load metadata: %w", err)
	}

	// Collect all referenced files from all snapshots
	referencedFiles := make(map[string]struct{})

	for _, snap := range meta.Snapshots() {
		if snap.ManifestList != "" {
			referencedFiles[snap.ManifestList] = struct{}{}

			// Read manifest list to find manifest files
			manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, snap.ManifestList)
			if err != nil {
				glog.V(2).Infof("iceberg maintenance: cannot read manifest list %s: %v", snap.ManifestList, err)
				continue
			}

			manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
			if err != nil {
				glog.V(2).Infof("iceberg maintenance: cannot parse manifest list %s: %v", snap.ManifestList, err)
				continue
			}

			for _, mf := range manifests {
				referencedFiles[mf.FilePath()] = struct{}{}

				// Read each manifest to find data files
				manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
				if err != nil {
					glog.V(2).Infof("iceberg maintenance: cannot read manifest %s: %v", mf.FilePath(), err)
					continue
				}

				entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), false)
				if err != nil {
					glog.V(2).Infof("iceberg maintenance: cannot parse manifest %s: %v", mf.FilePath(), err)
					continue
				}

				for _, entry := range entries {
					referencedFiles[entry.DataFile().FilePath()] = struct{}{}
				}
			}
		}
	}

	// Also reference the current metadata files
	for mle := range meta.PreviousFiles() {
		referencedFiles[mle.MetadataFile] = struct{}{}
	}

	// List actual files on filer in metadata/ and data/ directories
	tableBasePath := path.Join(s3tables.TablesPath, bucketName, tablePath)
	safetyThreshold := time.Now().Add(-time.Duration(config.OrphanOlderThanHours) * time.Hour)
	orphanCount := 0

	for _, subdir := range []string{"metadata", "data"} {
		dirPath := path.Join(tableBasePath, subdir)
		entries, err := listFilerEntries(ctx, filerClient, dirPath, "")
		if err != nil {
			glog.V(2).Infof("iceberg maintenance: cannot list %s: %v", dirPath, err)
			continue
		}

		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}

			// Check if file is referenced (try multiple path formats)
			filePath := path.Join(dirPath, entry.Name)
			relPath := path.Join(subdir, entry.Name)
			isReferenced := false
			for ref := range referencedFiles {
				if strings.HasSuffix(ref, "/"+entry.Name) || ref == filePath || ref == relPath || strings.HasSuffix(ref, entry.Name) {
					isReferenced = true
					break
				}
			}

			if isReferenced {
				continue
			}

			// Check safety window
			mtime := time.Unix(0, 0)
			if entry.Attributes != nil {
				mtime = time.Unix(entry.Attributes.Mtime, 0)
			}
			if mtime.After(safetyThreshold) {
				continue
			}

			// Delete orphan
			if delErr := deleteFilerFile(ctx, filerClient, dirPath, entry.Name); delErr != nil {
				glog.Warningf("iceberg maintenance: failed to delete orphan %s/%s: %v", dirPath, entry.Name, delErr)
			} else {
				orphanCount++
			}
		}
	}

	return fmt.Sprintf("removed %d orphan file(s)", orphanCount), nil
}

// ---------------------------------------------------------------------------
// Operation: Rewrite Manifests
// ---------------------------------------------------------------------------

// rewriteManifests merges small manifests into fewer, larger ones.
func (h *IcebergMaintenanceHandler) rewriteManifests(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config icebergMaintenanceConfig,
) (string, error) {
	// Load current metadata
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", fmt.Errorf("load metadata: %w", err)
	}

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return "no current snapshot", nil
	}

	// Read manifest list
	manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, currentSnap.ManifestList)
	if err != nil {
		return "", fmt.Errorf("read manifest list: %w", err)
	}

	manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
	if err != nil {
		return "", fmt.Errorf("parse manifest list: %w", err)
	}

	if len(manifests) < int(config.MinInputFiles) {
		return fmt.Sprintf("only %d manifests, below threshold of %d", len(manifests), config.MinInputFiles), nil
	}

	// Collect all entries from all data manifests
	var allEntries []iceberg.ManifestEntry
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
		if err != nil {
			return "", fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			return "", fmt.Errorf("parse manifest %s: %w", mf.FilePath(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		return "no data entries to rewrite", nil
	}

	// Write a single merged manifest
	spec := meta.PartitionSpec()
	schema := meta.CurrentSchema()
	version := meta.Version()
	snapshotID := currentSnap.SnapshotID

	var manifestBuf bytes.Buffer
	mergedManifest, err := iceberg.WriteManifest(
		fmt.Sprintf("metadata/merged-%d-%d.avro", snapshotID, time.Now().UnixMilli()),
		&manifestBuf,
		version,
		spec,
		schema,
		snapshotID,
		allEntries,
	)
	if err != nil {
		return "", fmt.Errorf("write merged manifest: %w", err)
	}

	// Save the merged manifest file to filer
	manifestFileName := fmt.Sprintf("merged-%d-%d.avro", snapshotID, time.Now().UnixMilli())
	metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestFileName, manifestBuf.Bytes()); err != nil {
		return "", fmt.Errorf("save merged manifest: %w", err)
	}

	// Write new manifest list referencing the merged manifest
	// Also include any delete manifests that were not rewritten
	var newManifests []iceberg.ManifestFile
	newManifests = append(newManifests, mergedManifest)
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			newManifests = append(newManifests, mf)
		}
	}

	var manifestListBuf bytes.Buffer
	parentSnap := currentSnap.ParentSnapshotID
	seqNum := currentSnap.SequenceNumber
	err = iceberg.WriteManifestList(version, &manifestListBuf, snapshotID, parentSnap, &seqNum, 0, newManifests)
	if err != nil {
		return "", fmt.Errorf("write manifest list: %w", err)
	}

	// Save new manifest list
	manifestListFileName := fmt.Sprintf("snap-%d-%d.avro", snapshotID, time.Now().UnixMilli())
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestListFileName, manifestListBuf.Bytes()); err != nil {
		return "", fmt.Errorf("save manifest list: %w", err)
	}

	// Create new snapshot with the rewritten manifest list
	manifestListLocation := path.Join("metadata", manifestListFileName)
	newSnapshotID := time.Now().UnixMilli()

	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		newSnapshot := &table.Snapshot{
			SnapshotID:       newSnapshotID,
			ParentSnapshotID: &snapshotID,
			SequenceNumber:   currentSnap.SequenceNumber + 1,
			TimestampMs:      time.Now().UnixMilli(),
			ManifestList:     manifestListLocation,
			Summary: &table.Summary{
				Operation:  table.OpReplace,
				Properties: map[string]string{"maintenance": "rewrite_manifests"},
			},
			SchemaID: func() *int {
				id := schema.ID
				return &id
			}(),
		}
		if err := builder.AddSnapshot(newSnapshot); err != nil {
			return err
		}
		return builder.SetSnapshotRef(
			table.MainBranch,
			newSnapshotID,
			table.BranchRef,
		)
	})
	if err != nil {
		return "", fmt.Errorf("commit manifest rewrite: %w", err)
	}

	return fmt.Sprintf("rewrote %d manifests into 1 (%d entries)", len(manifests), len(allEntries)), nil
}

// ---------------------------------------------------------------------------
// Commit Protocol with Retry
// ---------------------------------------------------------------------------

// commitWithRetry implements optimistic concurrency for metadata updates.
// It reads the current metadata, applies the mutation, writes a new metadata
// file, and updates the table entry. On version conflict, it retries.
func (h *IcebergMaintenanceHandler) commitWithRetry(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath, currentMetadataFileName string,
	config icebergMaintenanceConfig,
	mutate func(currentMeta table.Metadata, builder *table.MetadataBuilder) error,
) error {
	maxRetries := int(config.MaxCommitRetries)
	if maxRetries <= 0 {
		maxRetries = defaultMaxCommitRetries
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int64N(int64(25 * time.Millisecond)))
			time.Sleep(time.Duration(50*attempt)*time.Millisecond + jitter)
		}

		// Load current metadata
		meta, metaFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
		if err != nil {
			return fmt.Errorf("load metadata (attempt %d): %w", attempt, err)
		}

		// Build new metadata
		location := meta.Location()
		builder, err := table.MetadataBuilderFromBase(meta, location)
		if err != nil {
			return fmt.Errorf("create metadata builder (attempt %d): %w", attempt, err)
		}

		// Apply the mutation
		if err := mutate(meta, builder); err != nil {
			return fmt.Errorf("apply mutation (attempt %d): %w", attempt, err)
		}

		if !builder.HasChanges() {
			return nil // nothing to commit
		}

		newMeta, err := builder.Build()
		if err != nil {
			return fmt.Errorf("build metadata (attempt %d): %w", attempt, err)
		}

		// Serialize
		metadataBytes, err := json.Marshal(newMeta)
		if err != nil {
			return fmt.Errorf("marshal metadata (attempt %d): %w", attempt, err)
		}

		// Determine new metadata file name
		newVersion := extractMetadataVersion(metaFileName) + 1
		newMetadataFileName := fmt.Sprintf("v%d.metadata.json", newVersion)

		// Save new metadata file
		metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
		if err := saveFilerFile(ctx, filerClient, metaDir, newMetadataFileName, metadataBytes); err != nil {
			return fmt.Errorf("save metadata file (attempt %d): %w", attempt, err)
		}

		// Update the table entry's xattr with new metadata
		tableDir := path.Join(s3tables.TablesPath, bucketName, tablePath)
		err = updateTableMetadataXattr(ctx, filerClient, tableDir, metadataBytes)
		if err != nil {
			// On conflict, clean up the new metadata file and retry
			_ = deleteFilerFile(ctx, filerClient, metaDir, newMetadataFileName)
			if attempt < maxRetries-1 {
				glog.V(1).Infof("iceberg maintenance: version conflict on %s/%s, retrying (attempt %d)", bucketName, tablePath, attempt)
				continue
			}
			return fmt.Errorf("update table xattr (attempt %d): %w", attempt, err)
		}

		return nil
	}

	return fmt.Errorf("exceeded max commit retries (%d)", maxRetries)
}

// ---------------------------------------------------------------------------
// Filer I/O Helpers
// ---------------------------------------------------------------------------

// listFilerEntries lists all entries in a directory.
func listFilerEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, prefix string) ([]*filer_pb.Entry, error) {
	var entries []*filer_pb.Entry
	var lastFileName string
	limit := uint32(10000)

	for {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             prefix,
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "",
			Limit:              limit,
		})
		if err != nil {
			return entries, nil // directory may not exist
		}

		count := 0
		for {
			entry, err := resp.Recv()
			if err != nil {
				break
			}
			if entry.Entry != nil {
				entries = append(entries, entry.Entry)
				lastFileName = entry.Entry.Name
				count++
			}
		}

		if count < int(limit) {
			break
		}
	}

	return entries, nil
}

// loadCurrentMetadata loads and parses the current Iceberg metadata from the table entry's xattr.
func loadCurrentMetadata(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string) (table.Metadata, string, error) {
	parts := strings.SplitN(tablePath, "/", 2)
	var dir, name string
	if len(parts) == 2 {
		dir = path.Join(s3tables.TablesPath, bucketName, parts[0])
		name = parts[1]
	} else {
		dir = path.Join(s3tables.TablesPath, bucketName)
		name = tablePath
	}

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, "", fmt.Errorf("lookup table entry %s/%s: %w", dir, name, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, "", fmt.Errorf("table entry not found: %s/%s", dir, name)
	}

	metadataBytes, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok || len(metadataBytes) == 0 {
		return nil, "", fmt.Errorf("no metadata xattr on table entry %s/%s", dir, name)
	}

	// Parse internal metadata to extract FullMetadata
	var internalMeta struct {
		MetadataVersion int `json:"metadataVersion"`
		Metadata        *struct {
			FullMetadata json.RawMessage `json:"fullMetadata,omitempty"`
		} `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(metadataBytes, &internalMeta); err != nil {
		return nil, "", fmt.Errorf("unmarshal internal metadata: %w", err)
	}
	if internalMeta.Metadata == nil || len(internalMeta.Metadata.FullMetadata) == 0 {
		return nil, "", fmt.Errorf("no fullMetadata in table xattr")
	}

	meta, err := table.ParseMetadataBytes(internalMeta.Metadata.FullMetadata)
	if err != nil {
		return nil, "", fmt.Errorf("parse iceberg metadata: %w", err)
	}

	metadataFileName := fmt.Sprintf("v%d.metadata.json", internalMeta.MetadataVersion)
	return meta, metadataFileName, nil
}

// loadFileByIcebergPath loads a file from the filer given an Iceberg-style path.
// Paths may be absolute filer paths or relative (metadata/..., data/...).
func loadFileByIcebergPath(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath, icebergPath string) ([]byte, error) {
	// Normalize the path - Iceberg paths can be:
	// - relative: "metadata/snap-123.avro"
	// - absolute: "/buckets/mybucket/ns/table/metadata/snap-123.avro"
	// - location-based: "s3://bucket/ns/table/metadata/snap-123.avro"
	fileName := path.Base(icebergPath)
	var dir string

	if strings.Contains(icebergPath, "metadata/") {
		dir = path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	} else if strings.Contains(icebergPath, "data/") {
		dir = path.Join(s3tables.TablesPath, bucketName, tablePath, "data")
	} else {
		// Default to metadata directory
		dir = path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	}

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      fileName,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup %s/%s: %w", dir, fileName, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, fmt.Errorf("file not found: %s/%s", dir, fileName)
	}

	return resp.Entry.Content, nil
}

// saveFilerFile saves a file to the filer.
func saveFilerFile(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, fileName string, content []byte) error {
	resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name: fileName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0644),
				FileSize: uint64(len(content)),
			},
			Content: content,
		},
	})
	if err != nil {
		return fmt.Errorf("create entry %s/%s: %w", dir, fileName, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("create entry %s/%s: %s", dir, fileName, resp.Error)
	}
	return nil
}

// deleteFilerFile deletes a file from the filer.
func deleteFilerFile(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, fileName string) error {
	return filer_pb.DoRemove(ctx, client, dir, fileName, false, false, true, false, nil)
}

// updateTableMetadataXattr updates the table entry's metadata xattr with
// the new Iceberg metadata. It reads the existing xattr, updates the
// fullMetadata field, and writes it back.
func updateTableMetadataXattr(ctx context.Context, client filer_pb.SeaweedFilerClient, tableDir string, newFullMetadata []byte) error {
	tableName := path.Base(tableDir)
	parentDir := path.Dir(tableDir)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parentDir,
		Name:      tableName,
	})
	if err != nil {
		return fmt.Errorf("lookup table entry: %w", err)
	}
	if resp == nil || resp.Entry == nil {
		return fmt.Errorf("table entry not found")
	}

	existingXattr, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok {
		return fmt.Errorf("no metadata xattr on table entry")
	}

	// Parse existing xattr, update fullMetadata
	var internalMeta map[string]json.RawMessage
	if err := json.Unmarshal(existingXattr, &internalMeta); err != nil {
		return fmt.Errorf("unmarshal existing xattr: %w", err)
	}

	// Update the metadata.fullMetadata field
	var metadataObj map[string]json.RawMessage
	if raw, ok := internalMeta["metadata"]; ok {
		if err := json.Unmarshal(raw, &metadataObj); err != nil {
			return fmt.Errorf("unmarshal metadata object: %w", err)
		}
	} else {
		metadataObj = make(map[string]json.RawMessage)
	}
	metadataObj["fullMetadata"] = newFullMetadata
	metadataJSON, err := json.Marshal(metadataObj)
	if err != nil {
		return fmt.Errorf("marshal metadata object: %w", err)
	}
	internalMeta["metadata"] = metadataJSON

	// Increment version
	if versionRaw, ok := internalMeta["metadataVersion"]; ok {
		var version int
		if err := json.Unmarshal(versionRaw, &version); err == nil {
			version++
			versionJSON, _ := json.Marshal(version)
			internalMeta["metadataVersion"] = versionJSON
		}
	}

	// Update modifiedAt
	modifiedAt, _ := json.Marshal(time.Now().Format(time.RFC3339Nano))
	internalMeta["modifiedAt"] = modifiedAt

	updatedXattr, err := json.Marshal(internalMeta)
	if err != nil {
		return fmt.Errorf("marshal updated xattr: %w", err)
	}

	resp.Entry.Extended[s3tables.ExtendedKeyMetadata] = updatedXattr
	updateResp, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: parentDir,
		Entry:     resp.Entry,
	})
	if err != nil {
		return fmt.Errorf("update table entry: %w", err)
	}
	if updateResp != nil {
		// success
	}
	return nil
}

// ---------------------------------------------------------------------------
// Config & Utility Helpers
// ---------------------------------------------------------------------------

func parseIcebergMaintenanceConfig(values map[string]*plugin_pb.ConfigValue) icebergMaintenanceConfig {
	return icebergMaintenanceConfig{
		SnapshotRetentionHours: readInt64Config(values, "snapshot_retention_hours", defaultSnapshotRetentionHours),
		MaxSnapshotsToKeep:     readInt64Config(values, "max_snapshots_to_keep", defaultMaxSnapshotsToKeep),
		OrphanOlderThanHours:   readInt64Config(values, "orphan_older_than_hours", defaultOrphanOlderThanHours),
		MaxCommitRetries:       readInt64Config(values, "max_commit_retries", defaultMaxCommitRetries),
		TargetFileSizeBytes:    readInt64Config(values, "target_file_size_bytes", defaultTargetFileSizeBytes),
		MinInputFiles:          readInt64Config(values, "min_input_files", defaultMinInputFiles),
		Operations:             readStringConfig(values, "operations", defaultOperations),
	}
}

// parseOperations returns the ordered list of maintenance operations to execute.
// Order follows Iceberg best practices: compact → expire_snapshots → remove_orphans → rewrite_manifests
func parseOperations(ops string) []string {
	ops = strings.TrimSpace(strings.ToLower(ops))
	if ops == "" || ops == "all" {
		return []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}
	}

	allOps := []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}
	requested := make(map[string]struct{})
	for _, op := range strings.Split(ops, ",") {
		op = strings.TrimSpace(op)
		if op != "" {
			requested[op] = struct{}{}
		}
	}

	var result []string
	for _, op := range allOps {
		if _, ok := requested[op]; ok {
			result = append(result, op)
		}
	}
	return result
}

func extractMetadataVersion(metadataFileName string) int {
	// Parse "v3.metadata.json" → 3
	name := strings.TrimPrefix(metadataFileName, "v")
	name = strings.TrimSuffix(name, ".metadata.json")
	var version int
	fmt.Sscanf(name, "%d", &version)
	return version
}

func (h *IcebergMaintenanceHandler) sendEmptyDetection(sender DetectionSender) error {
	if err := sender.SendProposals(&plugin_pb.DetectionProposals{
		JobType:   icebergMaintenanceJobType,
		Proposals: []*plugin_pb.JobProposal{},
		HasMore:   false,
	}); err != nil {
		return err
	}
	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        icebergMaintenanceJobType,
		Success:        true,
		TotalProposals: 0,
	})
}

// Ensure IcebergMaintenanceHandler implements JobHandler.
var _ JobHandler = (*IcebergMaintenanceHandler)(nil)
