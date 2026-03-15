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
) (string, map[string]int64, error) {
	start := time.Now()
	// Load current metadata
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("load metadata: %w", err)
	}

	snapshots := meta.Snapshots()
	if len(snapshots) == 0 {
		return "no snapshots", nil, nil
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

	// Walk from newest to oldest. The current snapshot is always kept.
	// Among the remaining, keep up to MaxSnapshotsToKeep-1 (since current
	// counts toward the quota). Expire the rest only if they exceed the
	// retention window; snapshots within the window are kept regardless.
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
		return "no snapshots expired", nil, nil
	}

	// Split snapshots into expired and kept sets
	expireSet := make(map[int64]struct{}, len(toExpire))
	for _, id := range toExpire {
		expireSet[id] = struct{}{}
	}
	var expiredSnaps, keptSnaps []table.Snapshot
	for _, snap := range sorted {
		if _, ok := expireSet[snap.SnapshotID]; ok {
			expiredSnaps = append(expiredSnaps, snap)
		} else {
			keptSnaps = append(keptSnaps, snap)
		}
	}

	// Collect all files referenced by each set before modifying metadata.
	// This lets us determine which files become unreferenced.
	expiredFiles, err := collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, expiredSnaps)
	if err != nil {
		return "", nil, fmt.Errorf("collect expired snapshot files: %w", err)
	}
	keptFiles, err := collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, keptSnaps)
	if err != nil {
		return "", nil, fmt.Errorf("collect kept snapshot files: %w", err)
	}

	// Normalize kept file paths for consistent comparison
	normalizedKept := make(map[string]struct{}, len(keptFiles))
	for f := range keptFiles {
		normalizedKept[normalizeIcebergPath(f, bucketName, tablePath)] = struct{}{}
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
		return "", nil, fmt.Errorf("commit snapshot expiration: %w", err)
	}

	// Delete files exclusively referenced by expired snapshots (best-effort)
	tableBasePath := path.Join(s3tables.TablesPath, bucketName, tablePath)
	deletedCount := 0
	for filePath := range expiredFiles {
		normalized := normalizeIcebergPath(filePath, bucketName, tablePath)
		if _, stillReferenced := normalizedKept[normalized]; stillReferenced {
			continue
		}
		dir := path.Join(tableBasePath, path.Dir(normalized))
		fileName := path.Base(normalized)
		if delErr := deleteFilerFile(ctx, filerClient, dir, fileName); delErr != nil {
			glog.Warningf("iceberg maintenance: failed to delete unreferenced file %s: %v", filePath, delErr)
		} else {
			deletedCount++
		}
	}

	metrics := map[string]int64{
		"snapshots_expired": int64(len(toExpire)),
		"files_deleted":     int64(deletedCount),
		"duration_ms":       time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf("expired %d snapshot(s), deleted %d unreferenced file(s)", len(toExpire), deletedCount), metrics, nil
}

// collectSnapshotFiles returns all file paths (manifest lists, manifest files,
// data files) referenced by the given snapshots. It returns an error if any
// manifest list or manifest cannot be read/parsed, to prevent delete decisions
// based on incomplete reference data.
func collectSnapshotFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	snapshots []table.Snapshot,
) (map[string]struct{}, error) {
	files := make(map[string]struct{})
	for _, snap := range snapshots {
		if snap.ManifestList == "" {
			continue
		}
		files[snap.ManifestList] = struct{}{}

		manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, snap.ManifestList)
		if err != nil {
			return nil, fmt.Errorf("read manifest list %s: %w", snap.ManifestList, err)
		}
		manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
		if err != nil {
			return nil, fmt.Errorf("parse manifest list %s: %w", snap.ManifestList, err)
		}

		for _, mf := range manifests {
			files[mf.FilePath()] = struct{}{}

			manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
			}
			entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), false)
			if err != nil {
				return nil, fmt.Errorf("parse manifest %s: %w", mf.FilePath(), err)
			}
			for _, entry := range entries {
				files[entry.DataFile().FilePath()] = struct{}{}
			}
		}
	}
	return files, nil
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
) (string, map[string]int64, error) {
	start := time.Now()
	// Load current metadata
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("load metadata: %w", err)
	}

	// Collect all referenced files from all snapshots
	referencedFiles, err := collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, meta.Snapshots())
	if err != nil {
		return "", nil, fmt.Errorf("collect referenced files: %w", err)
	}

	// Reference the active metadata file so it is not treated as orphan
	referencedFiles[path.Join("metadata", metadataFileName)] = struct{}{}

	// Also reference the current metadata files
	for mle := range meta.PreviousFiles() {
		referencedFiles[mle.MetadataFile] = struct{}{}
	}

	// Precompute a normalized lookup set so orphan checks are O(1) per file.
	normalizedRefs := make(map[string]struct{}, len(referencedFiles))
	for ref := range referencedFiles {
		normalizedRefs[ref] = struct{}{}
		normalizedRefs[normalizeIcebergPath(ref, bucketName, tablePath)] = struct{}{}
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

			_, isReferenced := normalizedRefs[relPath]

			if isReferenced {
				continue
			}

			// Check safety window — skip entries with unknown age
			if entry.Attributes == nil {
				continue
			}
			mtime := time.Unix(entry.Attributes.Mtime, 0)
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

	metrics := map[string]int64{
		"orphans_removed": int64(orphanCount),
		"duration_ms":     time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf("removed %d orphan file(s)", orphanCount), metrics, nil
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
) (string, map[string]int64, error) {
	start := time.Now()
	// Load current metadata
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("load metadata: %w", err)
	}

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return "no current snapshot", nil, nil
	}

	// Read manifest list
	manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, currentSnap.ManifestList)
	if err != nil {
		return "", nil, fmt.Errorf("read manifest list: %w", err)
	}

	manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
	if err != nil {
		return "", nil, fmt.Errorf("parse manifest list: %w", err)
	}

	if int64(len(manifests)) < config.MinManifestsToRewrite {
		return fmt.Sprintf("only %d manifests, below threshold of %d", len(manifests), config.MinManifestsToRewrite), nil, nil
	}

	// Collect all entries from data manifests, grouped by partition spec ID
	// so we write one merged manifest per spec (required for spec-evolved tables).
	type specEntries struct {
		specID  int32
		spec    iceberg.PartitionSpec
		entries []iceberg.ManifestEntry
	}
	specMap := make(map[int32]*specEntries)

	// Build a lookup from spec ID to PartitionSpec
	specByID := make(map[int]iceberg.PartitionSpec)
	for _, ps := range meta.PartitionSpecs() {
		specByID[ps.ID()] = ps
	}

	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
		if err != nil {
			return "", nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			return "", nil, fmt.Errorf("parse manifest %s: %w", mf.FilePath(), err)
		}

		sid := mf.PartitionSpecID()
		se, ok := specMap[sid]
		if !ok {
			ps, found := specByID[int(sid)]
			if !found {
				return "", nil, fmt.Errorf("partition spec %d not found in table metadata", sid)
			}
			se = &specEntries{specID: sid, spec: ps}
			specMap[sid] = se
		}
		se.entries = append(se.entries, entries...)
	}

	if len(specMap) == 0 {
		return "no data entries to rewrite", nil, nil
	}

	schema := meta.CurrentSchema()
	version := meta.Version()
	snapshotID := currentSnap.SnapshotID
	newSnapshotID := time.Now().UnixMilli()
	newSeqNum := currentSnap.SequenceNumber + 1
	metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")

	// Track written artifacts so we can clean them up if the commit fails.
	type artifact struct {
		dir, fileName string
	}
	var writtenArtifacts []artifact
	committed := false

	defer func() {
		if committed || len(writtenArtifacts) == 0 {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, a := range writtenArtifacts {
			if err := deleteFilerFile(cleanupCtx, filerClient, a.dir, a.fileName); err != nil {
				glog.Warningf("iceberg rewrite-manifests: failed to clean up artifact %s/%s: %v", a.dir, a.fileName, err)
			}
		}
	}()

	// Write one merged manifest per partition spec
	var newManifests []iceberg.ManifestFile
	totalEntries := 0
	for _, se := range specMap {
		totalEntries += len(se.entries)
		manifestFileName := fmt.Sprintf("merged-%d-spec%d-%d.avro", newSnapshotID, se.specID, time.Now().UnixMilli())
		manifestPath := path.Join("metadata", manifestFileName)

		var manifestBuf bytes.Buffer
		mergedManifest, err := iceberg.WriteManifest(
			manifestPath,
			&manifestBuf,
			version,
			se.spec,
			schema,
			newSnapshotID,
			se.entries,
		)
		if err != nil {
			return "", nil, fmt.Errorf("write merged manifest for spec %d: %w", se.specID, err)
		}

		if err := saveFilerFile(ctx, filerClient, metaDir, manifestFileName, manifestBuf.Bytes()); err != nil {
			return "", nil, fmt.Errorf("save merged manifest for spec %d: %w", se.specID, err)
		}
		writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestFileName})
		newManifests = append(newManifests, mergedManifest)
	}

	// Include any delete manifests that were not rewritten
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			newManifests = append(newManifests, mf)
		}
	}

	var manifestListBuf bytes.Buffer
	err = iceberg.WriteManifestList(version, &manifestListBuf, newSnapshotID, &snapshotID, &newSeqNum, 0, newManifests)
	if err != nil {
		return "", nil, fmt.Errorf("write manifest list: %w", err)
	}

	// Save new manifest list
	manifestListFileName := fmt.Sprintf("snap-%d-%d.avro", newSnapshotID, time.Now().UnixMilli())
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestListFileName, manifestListBuf.Bytes()); err != nil {
		return "", nil, fmt.Errorf("save manifest list: %w", err)
	}
	writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestListFileName})

	// Create new snapshot with the rewritten manifest list
	manifestListLocation := path.Join("metadata", manifestListFileName)

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
		return "", nil, fmt.Errorf("commit manifest rewrite: %w", err)
	}

	committed = true
	metrics := map[string]int64{
		"manifests_rewritten": int64(len(manifests)),
		"entries_total":       int64(totalEntries),
		"duration_ms":         time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf("rewrote %d manifests into %d (%d entries)", len(manifests), len(specMap), totalEntries), metrics, nil
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
			backoff := time.Duration(50*(1<<(attempt-1))) * time.Millisecond // exponential: 50ms, 100ms, 200ms, ...
			const maxBackoff = 5 * time.Second
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			jitter := time.Duration(rand.Int64N(int64(backoff) / 5)) // 0–20% of backoff
			timer := time.NewTimer(backoff + jitter)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}

		// Load current metadata
		meta, metaFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
		if err != nil {
			return fmt.Errorf("load metadata (attempt %d): %w", attempt, err)
		}

		// Build new metadata — pass the current metadata file path so the
		// metadata log correctly records where the previous version lives.
		currentMetaFilePath := path.Join("metadata", metaFileName)
		builder, err := table.MetadataBuilderFromBase(meta, currentMetaFilePath)
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

		// Determine new metadata file name. Include a timestamp suffix so
		// concurrent writers stage to distinct files instead of clobbering.
		currentVersion := extractMetadataVersion(metaFileName)
		newVersion := currentVersion + 1
		newMetadataFileName := fmt.Sprintf("v%d-%d.metadata.json", newVersion, time.Now().UnixNano())

		// Save new metadata file
		metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
		if err := saveFilerFile(ctx, filerClient, metaDir, newMetadataFileName, metadataBytes); err != nil {
			return fmt.Errorf("save metadata file (attempt %d): %w", attempt, err)
		}

		// Update the table entry's xattr with new metadata (CAS on version)
		tableDir := path.Join(s3tables.TablesPath, bucketName, tablePath)
		newMetadataLocation := path.Join("metadata", newMetadataFileName)
		err = updateTableMetadataXattr(ctx, filerClient, tableDir, currentVersion, metadataBytes, newMetadataLocation)
		if err != nil {
			// Use a detached context for cleanup so staged files are removed
			// even if the original context was canceled.
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if !errors.Is(err, errMetadataVersionConflict) {
				// Non-conflict error (permissions, transport, etc.): fail immediately.
				_ = deleteFilerFile(cleanupCtx, filerClient, metaDir, newMetadataFileName)
				cleanupCancel()
				return fmt.Errorf("update table xattr (attempt %d): %w", attempt, err)
			}
			// Version conflict: clean up the new metadata file and retry
			_ = deleteFilerFile(cleanupCtx, filerClient, metaDir, newMetadataFileName)
			cleanupCancel()
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
