package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// errStalePlan is returned by a commit mutation when the table head has
// advanced since planning. The caller should not retry the same plan.
var errStalePlan = errors.New("stale plan: table head changed since planning")

// errMetadataVersionConflict is returned when the xattr update detects a
// concurrent metadata version change (compare-and-swap failure).
var errMetadataVersionConflict = errors.New("metadata version conflict")

// ---------------------------------------------------------------------------
// Operation: Expire Snapshots
// ---------------------------------------------------------------------------

// expireSnapshots removes old snapshots from the table metadata and cleans up
// their manifest list files.
func (h *Handler) expireSnapshots(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
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

	// Sort snapshots by timestamp descending (most recent first) so that
	// the keep-count logic always preserves the newest snapshots.
	sorted := make([]table.Snapshot, len(snapshots))
	copy(sorted, snapshots)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TimestampMs > sorted[j].TimestampMs
	})

	// Walk from newest to oldest. Keep the first MaxSnapshotsToKeep entries
	// plus the current snapshot unconditionally; expire the rest if they
	// exceed the retention window.
	var toExpire []int64
	var kept int64
	for _, snap := range sorted {
		if snap.SnapshotID == currentSnapID {
			kept++
			continue
		}
		age := nowMs - snap.TimestampMs
		if kept < config.MaxSnapshotsToKeep {
			kept++
			continue
		}
		if age > retentionMs {
			toExpire = append(toExpire, snap.SnapshotID)
		} else {
			kept++
		}
	}

	if len(toExpire) == 0 {
		return "no snapshots expired", nil
	}

	// Collect manifest list files to clean up after commit
	expireSet := make(map[int64]struct{}, len(toExpire))
	for _, id := range toExpire {
		expireSet[id] = struct{}{}
	}
	var manifestListsToDelete []string
	for _, snap := range sorted {
		if _, ok := expireSet[snap.SnapshotID]; ok && snap.ManifestList != "" {
			manifestListsToDelete = append(manifestListsToDelete, snap.ManifestList)
		}
	}

	// Use MetadataBuilder to remove snapshots and create new metadata
	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		// Guard: verify table head hasn't changed since we planned
		cs := currentMeta.CurrentSnapshot()
		if (cs == nil) != (currentSnapID == 0) || (cs != nil && cs.SnapshotID != currentSnapID) {
			return errStalePlan
		}
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
func (h *Handler) removeOrphans(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
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
		fileEntries, err := walkFilerEntries(ctx, filerClient, dirPath)
		if err != nil {
			glog.V(2).Infof("iceberg maintenance: cannot walk %s: %v", dirPath, err)
			continue
		}

		for _, fe := range fileEntries {
			entry := fe.Entry
			// Build relative path from the table base (e.g. "data/region=us/file.parquet")
			fullPath := path.Join(fe.Dir, entry.Name)
			relPath := strings.TrimPrefix(fullPath, tableBasePath+"/")

			isReferenced := false
			for ref := range referencedFiles {
				if ref == relPath || strings.HasSuffix(ref, "/"+entry.Name) {
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
			if delErr := deleteFilerFile(ctx, filerClient, fe.Dir, entry.Name); delErr != nil {
				glog.Warningf("iceberg maintenance: failed to delete orphan %s/%s: %v", fe.Dir, entry.Name, delErr)
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
func (h *Handler) rewriteManifests(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
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

	if int64(len(manifests)) < config.MinInputFiles {
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

	manifestFileName := fmt.Sprintf("merged-%d-%d.avro", snapshotID, time.Now().UnixMilli())
	manifestPath := path.Join("metadata", manifestFileName)
	metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")

	var manifestBuf bytes.Buffer
	mergedManifest, err := iceberg.WriteManifest(
		manifestPath,
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
		// Guard: verify table head hasn't advanced since we planned.
		// The merged manifest and manifest list were built against snapshotID;
		// if the head moved, they reference stale state.
		cs := currentMeta.CurrentSnapshot()
		if cs == nil || cs.SnapshotID != snapshotID {
			return errStalePlan
		}

		newSnapshot := &table.Snapshot{
			SnapshotID:       newSnapshotID,
			ParentSnapshotID: &snapshotID,
			SequenceNumber:   cs.SequenceNumber + 1,
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
func (h *Handler) commitWithRetry(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath, currentMetadataFileName string,
	config Config,
	mutate func(currentMeta table.Metadata, builder *table.MetadataBuilder) error,
) error {
	maxRetries := config.MaxCommitRetries
	if maxRetries <= 0 || maxRetries > 20 {
		maxRetries = defaultMaxCommitRetries
	}

	for attempt := int64(0); attempt < maxRetries; attempt++ {
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
		currentVersion := extractMetadataVersion(metaFileName)
		newVersion := currentVersion + 1
		newMetadataFileName := fmt.Sprintf("v%d.metadata.json", newVersion)

		// Save new metadata file
		metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
		if err := saveFilerFile(ctx, filerClient, metaDir, newMetadataFileName, metadataBytes); err != nil {
			return fmt.Errorf("save metadata file (attempt %d): %w", attempt, err)
		}

		// Update the table entry's xattr with new metadata (CAS on version)
		tableDir := path.Join(s3tables.TablesPath, bucketName, tablePath)
		err = updateTableMetadataXattr(ctx, filerClient, tableDir, currentVersion, metadataBytes)
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
