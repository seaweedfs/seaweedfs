package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// tableInfo captures metadata about a table for detection/execution.
type tableInfo struct {
	BucketName       string
	Namespace        string
	TableName        string
	TablePath        string // namespace/tableName
	MetadataFileName string
	Metadata         table.Metadata
}

// scanTablesForMaintenance enumerates table buckets and their tables,
// evaluating which ones need maintenance based on metadata thresholds.
// When limit > 0 the scan stops after collecting limit+1 results so the
// caller can determine whether more tables remain (HasMore).
func (h *Handler) scanTablesForMaintenance(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	config Config,
	bucketFilter, namespaceFilter, tableFilter string,
	limit int,
) ([]tableInfo, error) {
	var tables []tableInfo
	ops, err := parseOperations(config.Operations)
	if err != nil {
		return nil, fmt.Errorf("parse operations: %w", err)
	}

	// Compile wildcard matchers once (nil = match all)
	bucketMatchers := wildcard.CompileWildcardMatchers(bucketFilter)
	nsMatchers := wildcard.CompileWildcardMatchers(namespaceFilter)
	tableMatchers := wildcard.CompileWildcardMatchers(tableFilter)

	bucketsPath := s3tables.TablesPath
	bucketEntries, err := listFilerEntries(ctx, filerClient, bucketsPath, "")
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	for _, bucketEntry := range bucketEntries {
		select {
		case <-ctx.Done():
			return tables, ctx.Err()
		default:
		}

		if !bucketEntry.IsDirectory || !s3tables.IsTableBucketEntry(bucketEntry) {
			continue
		}
		bucketName := bucketEntry.Name
		if !wildcard.MatchesAnyWildcard(bucketMatchers, bucketName) {
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
			select {
			case <-ctx.Done():
				return tables, ctx.Err()
			default:
			}

			if !nsEntry.IsDirectory {
				continue
			}
			nsName := nsEntry.Name
			if !wildcard.MatchesAnyWildcard(nsMatchers, nsName) {
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
				if !wildcard.MatchesAnyWildcard(tableMatchers, tblName) {
					continue
				}

				// Check if this entry has table metadata
				metadataBytes, ok := tableEntry.Extended[s3tables.ExtendedKeyMetadata]
				if !ok || len(metadataBytes) == 0 {
					continue
				}

				// Parse the internal metadata to get FullMetadata
				var internalMeta struct {
					MetadataLocation string `json:"metadataLocation,omitempty"`
					Metadata         *struct {
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

				tablePath := path.Join(nsName, tblName)
				metadataFileName := metadataFileNameFromLocation(internalMeta.MetadataLocation, bucketName, tablePath)
				needsWork, err := h.tableNeedsMaintenance(ctx, filerClient, bucketName, tablePath, icebergMeta, metadataFileName, config, ops)
				if err != nil {
					glog.V(2).Infof("iceberg maintenance: skipping %s/%s/%s: cannot evaluate maintenance need: %v", bucketName, nsName, tblName, err)
					continue
				}
				if needsWork {
					tables = append(tables, tableInfo{
						BucketName:       bucketName,
						Namespace:        nsName,
						TableName:        tblName,
						TablePath:        tablePath,
						MetadataFileName: metadataFileName,
						Metadata:         icebergMeta,
					})
					if limit > 0 && len(tables) > limit {
						return tables, nil
					}
				}
			}
		}
	}

	return tables, nil
}

func normalizeDetectionConfig(config Config) Config {
	normalized := config
	if normalized.TargetFileSizeBytes <= 0 {
		normalized.TargetFileSizeBytes = defaultTargetFileSizeMB * 1024 * 1024
	}
	if normalized.MinInputFiles < 2 {
		normalized.MinInputFiles = defaultMinInputFiles
	}
	if normalized.MinManifestsToRewrite < minManifestsToRewrite {
		normalized.MinManifestsToRewrite = minManifestsToRewrite
	}
	if normalized.OrphanOlderThanHours <= 0 {
		normalized.OrphanOlderThanHours = defaultOrphanOlderThanHours
	}
	return normalized
}

func (h *Handler) tableNeedsMaintenance(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	meta table.Metadata,
	metadataFileName string,
	config Config,
	ops []string,
) (bool, error) {
	config = normalizeDetectionConfig(config)

	// Evaluate the metadata-only expiration check first so large tables do not
	// pay for manifest reads when snapshot expiry already makes them eligible.
	for _, op := range ops {
		if op == "expire_snapshots" && needsMaintenance(meta, config) {
			return true, nil
		}
	}

	loadManifests := func() ([]iceberg.ManifestFile, error) {
		return loadCurrentManifests(ctx, filerClient, bucketName, tablePath, meta)
	}

	var currentManifests []iceberg.ManifestFile
	var manifestsErr error
	manifestsLoaded := false
	getCurrentManifests := func() ([]iceberg.ManifestFile, error) {
		if manifestsLoaded {
			return currentManifests, manifestsErr
		}
		currentManifests, manifestsErr = loadManifests()
		manifestsLoaded = true
		return currentManifests, manifestsErr
	}
	var opEvalErrors []string

	for _, op := range ops {
		switch op {
		case "expire_snapshots":
			// Handled by the metadata-only check above.
			continue
		case "compact":
			manifests, err := getCurrentManifests()
			if err != nil {
				opEvalErrors = append(opEvalErrors, fmt.Sprintf("%s: %v", op, err))
				continue
			}
			eligible, err := hasEligibleCompaction(ctx, filerClient, bucketName, tablePath, manifests, config)
			if err != nil {
				opEvalErrors = append(opEvalErrors, fmt.Sprintf("%s: %v", op, err))
				continue
			}
			if eligible {
				return true, nil
			}
		case "rewrite_manifests":
			manifests, err := getCurrentManifests()
			if err != nil {
				opEvalErrors = append(opEvalErrors, fmt.Sprintf("%s: %v", op, err))
				continue
			}
			if countDataManifests(manifests) >= config.MinManifestsToRewrite {
				return true, nil
			}
		case "remove_orphans":
			if metadataFileName == "" {
				_, currentMetadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
				if err != nil {
					opEvalErrors = append(opEvalErrors, fmt.Sprintf("%s: %v", op, err))
					continue
				}
				metadataFileName = currentMetadataFileName
			}
			orphanCandidates, err := collectOrphanCandidates(ctx, filerClient, bucketName, tablePath, meta, metadataFileName, config.OrphanOlderThanHours)
			if err != nil {
				opEvalErrors = append(opEvalErrors, fmt.Sprintf("%s: %v", op, err))
				continue
			}
			if len(orphanCandidates) > 0 {
				return true, nil
			}
		}
	}

	if len(opEvalErrors) > 0 {
		return false, fmt.Errorf("evaluate maintenance operations: %s", strings.Join(opEvalErrors, "; "))
	}

	return false, nil
}

func metadataFileNameFromLocation(location, bucketName, tablePath string) string {
	if location == "" {
		return ""
	}
	return path.Base(normalizeIcebergPath(location, bucketName, tablePath))
}

func countDataManifests(manifests []iceberg.ManifestFile) int64 {
	var count int64
	for _, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			count++
		}
	}
	return count
}

func loadCurrentManifests(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	meta table.Metadata,
) ([]iceberg.ManifestFile, error) {
	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return nil, nil
	}

	manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, currentSnap.ManifestList)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
	if err != nil {
		return nil, fmt.Errorf("parse manifest list: %w", err)
	}
	return manifests, nil
}

func hasEligibleCompaction(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	manifests []iceberg.ManifestFile,
	config Config,
) (bool, error) {
	if len(manifests) == 0 {
		return false, nil
	}

	minInputFiles, err := compactionMinInputFiles(config.MinInputFiles)
	if err != nil {
		return false, err
	}

	var dataManifests []iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		dataManifests = append(dataManifests, mf)
	}
	if len(dataManifests) == 0 {
		return false, nil
	}

	var allEntries []iceberg.ManifestEntry
	for _, mf := range dataManifests {
		manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
		if err != nil {
			return false, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			return false, fmt.Errorf("parse manifest %s: %w", mf.FilePath(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	bins := buildCompactionBins(allEntries, config.TargetFileSizeBytes, minInputFiles)
	return len(bins) > 0, nil
}

func compactionMinInputFiles(minInputFiles int64) (int, error) {
	// Ensure the configured value is positive and fits into the platform's int type
	if minInputFiles <= 0 {
		return 0, fmt.Errorf("min input files must be positive, got %d", minInputFiles)
	}
	maxInt := int64(^uint(0) >> 1)
	if minInputFiles > maxInt {
		return 0, fmt.Errorf("min input files %d exceeds platform int size", minInputFiles)
	}
	return int(minInputFiles), nil
}

// needsMaintenance checks whether snapshot expiration work is needed based on
// metadata-only thresholds.
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
