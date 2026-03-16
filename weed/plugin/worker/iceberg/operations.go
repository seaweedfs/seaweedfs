package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
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

	plannedSnapshotIDs := snapshotIDSet(snapshots)
	plannedRefs := snapshotRefs(meta)
	toExpire, refsToRemove, err := planSnapshotExpiration(meta, config, start)
	if err != nil {
		return "", nil, fmt.Errorf("plan snapshot expiration: %w", err)
	}

	// Split snapshots into expired and kept sets
	expireSet := make(map[int64]struct{}, len(toExpire))
	for _, id := range toExpire {
		expireSet[id] = struct{}{}
	}
	var expiredSnaps, keptSnaps []table.Snapshot
	for _, snap := range snapshots {
		if _, ok := expireSet[snap.SnapshotID]; ok {
			expiredSnaps = append(expiredSnaps, snap)
		} else {
			keptSnaps = append(keptSnaps, snap)
		}
	}

	expiredFiles := make(map[string]struct{})
	keptFiles := make(map[string]struct{})
	if len(toExpire) > 0 {
		// Collect all files referenced by each set before modifying metadata.
		// This lets us determine which files become unreferenced.
		expiredFiles, err = collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, expiredSnaps)
		if err != nil {
			return "", nil, fmt.Errorf("collect expired snapshot files: %w", err)
		}
		keptFiles, err = collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, keptSnaps)
		if err != nil {
			return "", nil, fmt.Errorf("collect kept snapshot files: %w", err)
		}
	}

	// Normalize kept file paths for consistent comparison
	normalizedKept := make(map[string]struct{}, len(keptFiles))
	for f := range keptFiles {
		normalizedKept[normalizeIcebergPath(f, bucketName, tablePath)] = struct{}{}
	}

	if len(toExpire) > 0 || len(refsToRemove) > 0 {
		// Use MetadataBuilder to remove refs/snapshots and create new metadata.
		err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
			if !sameSnapshotSet(currentMeta, plannedSnapshotIDs) || !sameSnapshotRefs(currentMeta, plannedRefs) {
				return errStalePlan
			}
			for _, refName := range refsToRemove {
				if err := builder.RemoveSnapshotRef(refName); err != nil {
					return err
				}
			}
			if len(toExpire) == 0 {
				return nil
			}
			return builder.RemoveSnapshots(toExpire)
		})
		if err != nil {
			return "", nil, fmt.Errorf("commit snapshot expiration: %w", err)
		}
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

	metadataFilesDeleted := 0
	if config.CleanExpiredMetadata {
		metadataFilesDeleted, err = cleanupExpiredMetadataFiles(ctx, filerClient, bucketName, tablePath)
		if err != nil {
			return "", nil, fmt.Errorf("clean expired metadata: %w", err)
		}
	}

	if len(toExpire) == 0 && len(refsToRemove) == 0 && metadataFilesDeleted == 0 {
		return "no snapshots expired", nil, nil
	}

	metrics := map[string]int64{
		MetricSnapshotsExpired:     int64(len(toExpire)),
		MetricSnapshotRefsRemoved:  int64(len(refsToRemove)),
		MetricFilesDeleted:         int64(deletedCount),
		MetricMetadataFilesDeleted: int64(metadataFilesDeleted),
		MetricDurationMs:           time.Since(start).Milliseconds(),
	}

	var summaryParts []string
	if len(toExpire) > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("expired %d snapshot(s)", len(toExpire)))
	}
	if len(refsToRemove) > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("removed %d snapshot ref(s)", len(refsToRemove)))
	}
	if len(toExpire) > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("deleted %d unreferenced file(s)", deletedCount))
	}
	if metadataFilesDeleted > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("deleted %d expired metadata file(s)", metadataFilesDeleted))
	}
	return strings.Join(summaryParts, ", "), metrics, nil
}

func planSnapshotExpiration(meta table.Metadata, config Config, now time.Time) ([]int64, []string, error) {
	retentionMs := config.SnapshotRetentionHours * 3600 * 1000
	minSnapshotsToKeep := int(config.MaxSnapshotsToKeep)
	if minSnapshotsToKeep < 1 {
		minSnapshotsToKeep = 1
	}

	snapsToKeep := make(map[int64]struct{})
	var refsToRemove []string
	nowMs := now.UnixMilli()
	refCount := 0

	for refName, ref := range meta.Refs() {
		refCount++
		snap := meta.SnapshotByID(ref.SnapshotID)
		if snap == nil {
			return nil, nil, fmt.Errorf("snapshot ref %q points to missing snapshot %d", refName, ref.SnapshotID)
		}

		maxRefAgeMs := retentionMs
		if ref.MaxRefAgeMs != nil {
			maxRefAgeMs = *ref.MaxRefAgeMs
		}
		if refName != table.MainBranch && nowMs-snap.TimestampMs > maxRefAgeMs {
			refsToRemove = append(refsToRemove, refName)
			continue
		}

		if ref.SnapshotRefType != table.BranchRef {
			snapsToKeep[ref.SnapshotID] = struct{}{}
			continue
		}

		maxSnapshotAgeMs := retentionMs
		if ref.MaxSnapshotAgeMs != nil {
			maxSnapshotAgeMs = *ref.MaxSnapshotAgeMs
		}
		refMinSnapshotsToKeep := minSnapshotsToKeep
		if ref.MinSnapshotsToKeep != nil && *ref.MinSnapshotsToKeep > 0 {
			refMinSnapshotsToKeep = *ref.MinSnapshotsToKeep
		}

		numSnapshots := 0
		snapshotID := ref.SnapshotID
		for {
			snap = meta.SnapshotByID(snapshotID)
			if snap == nil {
				return nil, nil, fmt.Errorf("snapshot %d not found while traversing ref %q", snapshotID, refName)
			}

			snapshotAgeMs := nowMs - snap.TimestampMs
			if snapshotAgeMs > maxSnapshotAgeMs && numSnapshots >= refMinSnapshotsToKeep {
				break
			}

			snapsToKeep[snap.SnapshotID] = struct{}{}
			if snap.ParentSnapshotID == nil {
				break
			}

			snapshotID = *snap.ParentSnapshotID
			numSnapshots++
		}
	}

	if refCount == 0 {
		if currentSnap := meta.CurrentSnapshot(); currentSnap != nil {
			snapsToKeep[currentSnap.SnapshotID] = struct{}{}
		}
	}

	var toExpire []int64
	for _, snap := range meta.Snapshots() {
		if _, ok := snapsToKeep[snap.SnapshotID]; !ok {
			toExpire = append(toExpire, snap.SnapshotID)
		}
	}

	return toExpire, refsToRemove, nil
}

func snapshotIDSet(snapshots []table.Snapshot) map[int64]struct{} {
	ids := make(map[int64]struct{}, len(snapshots))
	for _, snap := range snapshots {
		ids[snap.SnapshotID] = struct{}{}
	}
	return ids
}

func sameSnapshotSet(meta table.Metadata, expected map[int64]struct{}) bool {
	current := meta.Snapshots()
	if len(current) != len(expected) {
		return false
	}
	for _, snap := range current {
		if _, ok := expected[snap.SnapshotID]; !ok {
			return false
		}
	}
	return true
}

func snapshotRefs(meta table.Metadata) map[string]table.SnapshotRef {
	refs := make(map[string]table.SnapshotRef)
	for refName, ref := range meta.Refs() {
		refs[refName] = ref
	}
	return refs
}

func sameSnapshotRefs(meta table.Metadata, expected map[string]table.SnapshotRef) bool {
	current := snapshotRefs(meta)
	if len(current) != len(expected) {
		return false
	}
	for refName, expectedRef := range expected {
		currentRef, ok := current[refName]
		if !ok || !currentRef.Equals(expectedRef) {
			return false
		}
	}
	return true
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

	orphanCandidates, err := collectOrphanCandidates(ctx, filerClient, bucketName, tablePath, meta, metadataFileName, config.OrphanOlderThanHours)
	if err != nil {
		return "", nil, fmt.Errorf("collect orphan candidates: %w", err)
	}

	if config.RemoveOrphansDryRun {
		metrics := map[string]int64{
			MetricOrphansFound:   int64(len(orphanCandidates)),
			MetricOrphansRemoved: 0,
			MetricDurationMs:     time.Since(start).Milliseconds(),
		}
		return fmt.Sprintf("found %d orphan file(s) (dry run)", len(orphanCandidates)), metrics, nil
	}

	orphanCount := 0
	for _, candidate := range orphanCandidates {
		if delErr := deleteFilerFile(ctx, filerClient, candidate.Dir, candidate.Entry.Name); delErr != nil {
			glog.Warningf("iceberg maintenance: failed to delete orphan %s/%s: %v", candidate.Dir, candidate.Entry.Name, delErr)
		} else {
			orphanCount++
		}
	}

	metrics := map[string]int64{
		MetricOrphansFound:   int64(len(orphanCandidates)),
		MetricOrphansRemoved: int64(orphanCount),
		MetricDurationMs:     time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf("removed %d orphan file(s)", orphanCount), metrics, nil
}

func cleanupExpiredMetadataFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
) (int, error) {
	meta, currentMetadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return 0, fmt.Errorf("load metadata: %w", err)
	}

	referencedFiles := map[string]struct{}{
		path.Join("metadata", currentMetadataFileName): {},
	}
	for entry := range meta.PreviousFiles() {
		referencedFiles[normalizeIcebergPath(entry.MetadataFile, bucketName, tablePath)] = struct{}{}
	}

	currentVersion := extractMetadataVersion(currentMetadataFileName)
	metadataDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	entries, err := listFilerEntries(ctx, filerClient, metadataDir, "")
	if err != nil {
		return 0, fmt.Errorf("list metadata dir: %w", err)
	}

	deleted := 0
	for _, entry := range entries {
		if entry == nil || entry.IsDirectory || !strings.HasSuffix(entry.Name, ".metadata.json") {
			continue
		}

		relPath := path.Join("metadata", entry.Name)
		if _, ok := referencedFiles[relPath]; ok {
			continue
		}
		if extractMetadataVersion(entry.Name) >= currentVersion {
			continue
		}
		if err := deleteFilerFile(ctx, filerClient, metadataDir, entry.Name); err != nil {
			glog.Warningf("iceberg maintenance: failed to delete expired metadata file %s/%s: %v", metadataDir, entry.Name, err)
			continue
		}
		deleted++
	}

	return deleted, nil
}

func collectOrphanCandidates(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	meta table.Metadata,
	metadataFileName string,
	orphanOlderThanHours int64,
) ([]filerFileEntry, error) {
	referencedFiles, err := collectSnapshotFiles(ctx, filerClient, bucketName, tablePath, meta.Snapshots())
	if err != nil {
		return nil, fmt.Errorf("collect referenced files: %w", err)
	}

	referencedFiles[path.Join("metadata", metadataFileName)] = struct{}{}
	for mle := range meta.PreviousFiles() {
		referencedFiles[mle.MetadataFile] = struct{}{}
	}

	normalizedRefs := make(map[string]struct{}, len(referencedFiles))
	for ref := range referencedFiles {
		normalizedRefs[ref] = struct{}{}
		normalizedRefs[normalizeIcebergPath(ref, bucketName, tablePath)] = struct{}{}
	}

	tableBasePath := path.Join(s3tables.TablesPath, bucketName, tablePath)
	safetyThreshold := time.Now().Add(-time.Duration(orphanOlderThanHours) * time.Hour)
	var candidates []filerFileEntry

	for _, subdir := range []string{"metadata", "data"} {
		dirPath := path.Join(tableBasePath, subdir)
		fileEntries, err := walkFilerEntries(ctx, filerClient, dirPath)
		if err != nil {
			glog.V(2).Infof("iceberg maintenance: cannot walk %s: %v", dirPath, err)
			continue
		}

		for _, fe := range fileEntries {
			entry := fe.Entry
			fullPath := path.Join(fe.Dir, entry.Name)
			relPath := strings.TrimPrefix(fullPath, tableBasePath+"/")
			if _, isReferenced := normalizedRefs[relPath]; isReferenced {
				continue
			}
			if entry.Attributes == nil {
				continue
			}
			if time.Unix(entry.Attributes.Mtime, 0).After(safetyThreshold) {
				continue
			}
			candidates = append(candidates, fe)
		}
	}

	return candidates, nil
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

	// Separate data manifests from delete manifests. Only data manifests
	// are candidates for rewriting; delete manifests are carried forward.
	var dataManifests []iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			dataManifests = append(dataManifests, mf)
		}
	}

	if int64(len(dataManifests)) < config.MinManifestsToRewrite {
		return fmt.Sprintf("only %d data manifests, below threshold of %d", len(dataManifests), config.MinManifestsToRewrite), nil, nil
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

	for _, mf := range dataManifests {
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
	artifactSuffix := compactRandomSuffix()
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
		manifestFileName := fmt.Sprintf("merged-%d-%s-spec%d.avro", newSnapshotID, artifactSuffix, se.specID)
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
	manifestListFileName := fmt.Sprintf("snap-%d-%s.avro", newSnapshotID, artifactSuffix)
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
		MetricManifestsRewritten: int64(len(dataManifests)),
		MetricEntriesTotal:       int64(totalEntries),
		MetricDurationMs:         time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf("rewrote %d manifests into %d (%d entries)", len(dataManifests), len(specMap), totalEntries), metrics, nil
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
