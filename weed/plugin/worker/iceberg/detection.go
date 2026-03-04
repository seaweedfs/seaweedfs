package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// tableInfo captures metadata about a table for detection/execution.
type tableInfo struct {
	BucketName string
	Namespace  string
	TableName  string
	TablePath  string // namespace/tableName
	Metadata   table.Metadata
}

// scanTablesForMaintenance enumerates table buckets and their tables,
// evaluating which ones need maintenance based on metadata thresholds.
func (h *Handler) scanTablesForMaintenance(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	config Config,
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
func needsMaintenance(meta table.Metadata, config Config) bool {
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
func (h *Handler) buildMaintenanceProposal(t tableInfo, filerAddress string) *plugin_pb.JobProposal {
	dedupeKey := fmt.Sprintf("iceberg_maintenance:%s/%s/%s", t.BucketName, t.Namespace, t.TableName)

	snapshotCount := len(t.Metadata.Snapshots())
	summary := fmt.Sprintf("Maintain %s/%s/%s (%d snapshots)", t.BucketName, t.Namespace, t.TableName, snapshotCount)

	return &plugin_pb.JobProposal{
		ProposalId: fmt.Sprintf("iceberg-%s-%s-%s-%d", t.BucketName, t.Namespace, t.TableName, time.Now().UnixMilli()),
		DedupeKey:  dedupeKey,
		JobType:    jobType,
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
