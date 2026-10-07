package iceberg

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// compactionBin groups small data files from the same partition and spec for merging.
type compactionBin struct {
	PartitionKey string
	Partition    map[int]any
	SpecID       int32
	Entries      []iceberg.ManifestEntry
	TotalSize    int64
}

// compactDataFiles reads manifests to find small Parquet data files, groups
// them by partition, reads and merges them using parquet-go, and commits new
// manifest entries.
func (h *Handler) compactDataFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
	onProgress func(binIdx, totalBins int),
) (string, map[string]int64, error) {
	start := time.Now()
	state, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("load metadata: %w", err)
	}
	meta, dataPath := state.Metadata, state.DataPath
	predicate, err := parsePartitionPredicate(config.Where, meta)
	if err != nil {
		return "", nil, err
	}

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return "no current snapshot", nil, nil
	}

	rewritePlan, err := resolveCompactionRewritePlan(config, meta)
	if err != nil {
		return "", nil, fmt.Errorf("resolve rewrite strategy: %w", err)
	}

	// Read manifest list
	manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, currentSnap.ManifestList)
	if err != nil {
		return "", nil, fmt.Errorf("read manifest list: %w", err)
	}
	manifests, err := s3tables.ReadManifestList(manifestListData)
	if err != nil {
		return "", nil, fmt.Errorf("parse manifest list: %w", err)
	}

	// Separate data manifests from delete manifests.
	var dataManifests, deleteManifests []iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.ManifestContent() == iceberg.ManifestContentData {
			dataManifests = append(dataManifests, mf)
		} else {
			deleteManifests = append(deleteManifests, mf)
		}
	}

	// If delete manifests exist and apply_deletes is disabled (or not yet
	// implemented for this code path), skip compaction to avoid producing
	// incorrect results by dropping deletes.
	if len(deleteManifests) > 0 && !config.ApplyDeletes {
		return "compaction skipped: delete manifests present and apply_deletes is disabled", nil, nil
	}

	specsByID := specByID(meta)
	schema := meta.CurrentSchema()

	// Collect data file entries from data manifests
	var allEntries []iceberg.ManifestEntry
	for _, mf := range dataManifests {
		manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, mf.FilePath())
		if err != nil {
			return "", nil, fmt.Errorf("read manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := s3tables.ReadManifest(mf, manifestData, true, specsByID, schema)
		if err != nil {
			return "", nil, fmt.Errorf("parse manifest %s: %w", mf.FilePath(), err)
		}
		allEntries = append(allEntries, entries...)
	}

	// Collect delete entries if we need to apply deletes
	var positionDeletes map[string][]int64
	var eqDeleteGroups []equalityDeleteGroup
	var allDeleteEntries []iceberg.ManifestEntry
	var dvPaths map[string]bool
	if config.ApplyDeletes && len(deleteManifests) > 0 {
		for _, mf := range deleteManifests {
			manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, mf.FilePath())
			if err != nil {
				return "", nil, fmt.Errorf("read delete manifest %s: %w", mf.FilePath(), err)
			}
			entries, err := s3tables.ReadManifest(mf, manifestData, true, specsByID, schema)
			if err != nil {
				return "", nil, fmt.Errorf("parse delete manifest %s: %w", mf.FilePath(), err)
			}
			allDeleteEntries = append(allDeleteEntries, entries...)
		}

		var err error
		dvPaths, err = deletionVectorPaths(allDeleteEntries, bucketName, dataPath)
		if err != nil {
			return "", nil, err
		}

		// Separate position and equality deletes, filtering by partition
		// predicate so out-of-scope deletes don't affect the merge.
		var posDeleteEntries, eqDeleteEntries []iceberg.ManifestEntry
		for _, entry := range allDeleteEntries {
			if entry.DataFile().FileFormat() == iceberg.PuffinFile {
				continue
			}
			if predicate != nil {
				spec, ok := specsByID[int(entry.DataFile().SpecID())]
				if !ok {
					continue
				}
				match, err := predicate.Matches(spec, entry.DataFile().Partition())
				if err != nil {
					return "", nil, err
				}
				if !match {
					continue
				}
			}
			switch entry.DataFile().ContentType() {
			case iceberg.EntryContentPosDeletes:
				posDeleteEntries = append(posDeleteEntries, entry)
			case iceberg.EntryContentEqDeletes:
				eqDeleteEntries = append(eqDeleteEntries, entry)
			}
		}

		if len(posDeleteEntries) > 0 {
			positionDeletes, err = collectPositionDeletes(ctx, filerClient, bucketName, dataPath, posDeleteEntries)
			if err != nil {
				return "", nil, fmt.Errorf("collect position deletes: %w", err)
			}
		}

		if len(eqDeleteEntries) > 0 {
			eqDeleteGroups, err = collectEqualityDeletes(ctx, filerClient, bucketName, dataPath, eqDeleteEntries, schema)
			if err != nil {
				return "", nil, fmt.Errorf("collect equality deletes: %w", err)
			}
		}
	}

	candidateEntries, err := excludeDeletionVectorFiles(allEntries, dvPaths, bucketName, dataPath)
	if err != nil {
		return "", nil, err
	}
	if predicate != nil {
		filtered := make([]iceberg.ManifestEntry, 0, len(candidateEntries))
		for _, entry := range candidateEntries {
			spec, ok := specsByID[int(entry.DataFile().SpecID())]
			if !ok {
				continue
			}
			match, err := predicate.Matches(spec, entry.DataFile().Partition())
			if err != nil {
				return "", nil, err
			}
			if match {
				filtered = append(filtered, entry)
			}
		}
		candidateEntries = filtered
	}

	minInputFiles, err := compactionMinInputFiles(config.MinInputFiles)
	if err != nil {
		return "", nil, err
	}

	// Build compaction bins: group small data files by partition.
	targetSize := compactionTargetSizeForPlan(config, rewritePlan)
	bins := buildCompactionBins(candidateEntries, targetSize, minInputFiles)
	initialBinCount := len(bins)
	bins = filterCompactionBinsByPlan(bins, config, rewritePlan)
	if len(bins) == 0 {
		return compactionNoEligibleMessage(config, rewritePlan, initialBinCount), nil, nil
	}

	// Build a lookup from spec ID to PartitionSpec for per-bin manifest writing.
	specLookup := specsByID

	version := meta.Version()
	snapshotID := currentSnap.SnapshotID
	// v3 row lineage: the snapshot starts at the table's next-row-id, and the
	// merged files carry every row's _row_id and _last_updated_sequence_number
	// (row_lineage.go).
	lineage := hasRowLineage(version)
	firstRowID := rowLineageFirstRowID(meta)

	// Compute the snapshot ID for the commit up front so all manifest entries
	// reference the same snapshot that will actually be committed.
	newSnapID := time.Now().UnixMilli()
	// Random suffix for artifact filenames to avoid collisions between
	// concurrent compaction runs on different tables sharing a timestamp.
	artifactSuffix := compactRandomSuffix()

	// Process each bin: read source Parquet files, merge, write output
	var newManifestEntries []iceberg.ManifestEntry
	var deletedManifestEntries []iceberg.ManifestEntry
	var summary snapshotSummary
	totalMerged := 0

	entrySeqNum := func(entry iceberg.ManifestEntry) *int64 {
		seqNum := entry.SequenceNum()
		if seqNum < 0 {
			return nil
		}
		return &seqNum
	}

	entryFileSeqNum := func(entry iceberg.ManifestEntry) *int64 {
		if fileSeqNum := entry.FileSequenceNum(); fileSeqNum != nil {
			value := *fileSeqNum
			return &value
		}
		return entrySeqNum(entry)
	}

	metaDir := path.Join(s3tables.TablesPath, bucketName, dataPath, "metadata")
	dataDir := path.Join(s3tables.TablesPath, bucketName, dataPath, "data")

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
		// Use a detached context so cleanup completes even if ctx was canceled.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, a := range writtenArtifacts {
			if err := deleteFilerFile(cleanupCtx, filerClient, a.dir, a.fileName); err != nil {
				glog.Warningf("iceberg compact: failed to clean up artifact %s/%s: %v", a.dir, a.fileName, err)
			}
		}
	}()

	for binIdx, bin := range bins {
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		default:
		}

		mergedFileName := fmt.Sprintf("compact-%d-%d-%s-%d.parquet", snapshotID, newSnapID, artifactSuffix, binIdx)
		mergedFilePath := absoluteIcebergPath(bucketName, dataPath, "data", mergedFileName)

		var mergedData []byte
		var recordCount int64
		if rewritePlan != nil && rewritePlan.strategy == "sort" {
			mergedData, recordCount, err = mergeParquetFilesSorted(ctx, filerClient, bucketName, dataPath, bin.Entries, positionDeletes, eqDeleteGroups, schema, rewritePlan, lineage)
		} else {
			mergedData, recordCount, err = mergeParquetFiles(ctx, filerClient, bucketName, dataPath, bin.Entries, positionDeletes, eqDeleteGroups, schema, lineage)
		}
		if err != nil {
			glog.Warningf("iceberg compact: failed to merge bin %d (%d files): %v", binIdx, len(bin.Entries), err)
			goto binDone
		}

		// Write merged file to filer
		if err := ensureFilerDir(ctx, filerClient, dataDir); err != nil {
			return "", nil, fmt.Errorf("ensure data dir: %w", err)
		}
		if err := saveFilerFile(ctx, filerClient, dataDir, mergedFileName, mergedData); err != nil {
			return "", nil, fmt.Errorf("save merged file: %w", err)
		}

		// Use the partition spec matching this bin's spec ID
		{
			binSpec, ok := specLookup[int(bin.SpecID)]
			if !ok {
				glog.Warningf("iceberg compact: spec %d not found for bin %d, skipping", bin.SpecID, binIdx)
				_ = deleteFilerFile(ctx, filerClient, dataDir, mergedFileName)
				goto binDone
			}

			// Create new DataFile entry for the merged file
			dfBuilder, err := iceberg.NewDataFileBuilder(
				binSpec,
				iceberg.EntryContentData,
				mergedFilePath,
				iceberg.ParquetFile,
				bin.Partition,
				nil, nil,
				recordCount,
				int64(len(mergedData)),
			)
			if err != nil {
				glog.Warningf("iceberg compact: failed to build data file entry for bin %d: %v", binIdx, err)
				_ = deleteFilerFile(ctx, filerClient, dataDir, mergedFileName)
				goto binDone
			}
			writtenArtifacts = append(writtenArtifacts, artifact{dir: dataDir, fileName: mergedFileName})

			mergedDataFile := dfBuilder.Build()
			summary.addFile(mergedDataFile)
			newEntry := iceberg.NewManifestEntry(
				iceberg.EntryStatusADDED,
				&newSnapID,
				nil, nil,
				mergedDataFile,
			)
			newManifestEntries = append(newManifestEntries, newEntry)

			// Mark original entries as deleted
			for _, entry := range bin.Entries {
				summary.removeFile(entry.DataFile())
				delEntry := iceberg.NewManifestEntry(
					iceberg.EntryStatusDELETED,
					&newSnapID,
					entrySeqNum(entry), entryFileSeqNum(entry),
					entry.DataFile(),
				)
				deletedManifestEntries = append(deletedManifestEntries, delEntry)
			}

			totalMerged += len(bin.Entries)
		}

	binDone:
		if onProgress != nil {
			onProgress(binIdx, len(bins))
		}
	}

	if len(newManifestEntries) == 0 {
		return "no bins successfully compacted", nil, nil
	}

	// Build entries for the new manifests:
	// - ADDED entries for merged files
	// - DELETED entries for original files
	// - EXISTING entries for files that weren't compacted
	compactedPaths := make(map[string]struct{})
	for _, entry := range deletedManifestEntries {
		compactedPaths[entry.DataFile().FilePath()] = struct{}{}
	}

	// Group all manifest entries by spec ID for per-spec manifest writing.
	type specEntries struct {
		specID  int32
		entries []iceberg.ManifestEntry
	}
	specEntriesMap := make(map[int32]*specEntries)

	addToSpec := func(specID int32, entry iceberg.ManifestEntry) {
		se, ok := specEntriesMap[specID]
		if !ok {
			se = &specEntries{specID: specID}
			specEntriesMap[specID] = se
		}
		se.entries = append(se.entries, entry)
	}

	// New and deleted entries carry the spec ID from their bin
	for _, entry := range newManifestEntries {
		addToSpec(entry.DataFile().SpecID(), entry)
	}
	for _, entry := range deletedManifestEntries {
		addToSpec(entry.DataFile().SpecID(), entry)
	}

	// Existing entries that weren't compacted
	for _, entry := range allEntries {
		if _, compacted := compactedPaths[entry.DataFile().FilePath()]; !compacted {
			existingEntry := iceberg.NewManifestEntry(
				iceberg.EntryStatusEXISTING,
				func() *int64 { id := entry.SnapshotID(); return &id }(),
				entrySeqNum(entry), entryFileSeqNum(entry),
				entry.DataFile(),
			)
			addToSpec(entry.DataFile().SpecID(), existingEntry)
		}
	}

	// Write one manifest per spec ID, iterating in sorted order for
	// deterministic manifest list construction.
	sortedSpecIDs := make([]int32, 0, len(specEntriesMap))
	for sid := range specEntriesMap {
		sortedSpecIDs = append(sortedSpecIDs, sid)
	}
	sort.Slice(sortedSpecIDs, func(i, j int) bool { return sortedSpecIDs[i] < sortedSpecIDs[j] })

	var allManifests []iceberg.ManifestFile
	for _, sid := range sortedSpecIDs {
		se := specEntriesMap[sid]
		ps, ok := specLookup[int(se.specID)]
		if !ok {
			return "", nil, fmt.Errorf("partition spec %d not found in table metadata", se.specID)
		}

		var manifestBuf bytes.Buffer
		manifestFileName := fmt.Sprintf("compact-%d-%s-spec%d.avro", newSnapID, artifactSuffix, se.specID)
		newManifest, err := iceberg.WriteManifest(
			absoluteIcebergPath(bucketName, dataPath, "metadata", manifestFileName),
			&manifestBuf,
			version,
			ps,
			schema,
			newSnapID,
			se.entries,
		)
		if err != nil {
			return "", nil, fmt.Errorf("write compact manifest for spec %d: %w", se.specID, err)
		}

		if err := saveFilerFile(ctx, filerClient, metaDir, manifestFileName, manifestBuf.Bytes()); err != nil {
			return "", nil, fmt.Errorf("save compact manifest for spec %d: %w", se.specID, err)
		}
		writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestFileName})
		allManifests = append(allManifests, newManifest)
	}

	// Carry forward delete manifests only if deletes were NOT applied.
	// When deletes were applied, they've been consumed during the merge.
	// Position deletes reference specific data files — if all those files
	// were compacted, the deletes are fully consumed. Equality deletes
	// apply broadly, so they're only consumed if all data files were compacted.
	carryDeletes := !config.ApplyDeletes || (len(positionDeletes) == 0 && len(eqDeleteGroups) == 0)
	if !carryDeletes {
		// Deletes still apply to any file that wasn't compacted.
		for _, entry := range allEntries {
			if _, compacted := compactedPaths[entry.DataFile().FilePath()]; !compacted {
				carryDeletes = true
				break
			}
		}
	}
	if carryDeletes {
		allManifests = append(allManifests, deleteManifests...)
	} else {
		// All files were compacted, so the deletes leave the table with them.
		for _, entry := range allDeleteEntries {
			summary.removeFile(entry.DataFile())
		}
	}

	// Write new manifest list
	var manifestListBuf bytes.Buffer
	seqNum := currentSnap.SequenceNumber + 1
	addedRows, err := writeManifestList(version, &manifestListBuf, newSnapID, &snapshotID, &seqNum, firstRowID, allManifests)
	if err != nil {
		return "", nil, fmt.Errorf("write compact manifest list: %w", err)
	}

	manifestListFileName := fmt.Sprintf("snap-%d-%s.avro", newSnapID, artifactSuffix)
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestListFileName, manifestListBuf.Bytes()); err != nil {
		return "", nil, fmt.Errorf("save compact manifest list: %w", err)
	}
	writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestListFileName})

	// Commit: add new snapshot and update main branch ref
	manifestListLocation := absoluteIcebergPath(bucketName, dataPath, "metadata", manifestListFileName)
	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, state.MetadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		// Guard: verify table head hasn't advanced since we planned.
		cs, err := checkCommitPlan(currentMeta, snapshotID, version, firstRowID)
		if err != nil {
			return err
		}

		newSnapshot := &table.Snapshot{
			SnapshotID:       newSnapID,
			ParentSnapshotID: &snapshotID,
			SequenceNumber:   seqNum,
			TimestampMs:      newSnapID,
			ManifestList:     manifestListLocation,
			Summary: summary.build(table.OpReplace, cs, map[string]string{
				"maintenance":      "compact_data_files",
				"merged-files":     fmt.Sprintf("%d", totalMerged),
				"new-files":        fmt.Sprintf("%d", len(newManifestEntries)),
				"compaction-bins":  fmt.Sprintf("%d", len(bins)),
				"rewrite-strategy": rewritePlan.strategy,
			}),
			SchemaID: func() *int {
				id := schema.ID
				return &id
			}(),
		}
		if rewritePlan != nil && rewritePlan.strategy == "sort" {
			newSnapshot.Summary.Properties["sort-fields"] = rewritePlan.summaryLabel()
		}
		setRowLineage(newSnapshot, version, firstRowID, addedRows)
		if err := builder.AddSnapshot(newSnapshot); err != nil {
			return err
		}
		return builder.SetSnapshotRef(table.MainBranch, newSnapID, table.BranchRef)
	})
	if err != nil {
		return "", nil, fmt.Errorf("commit compaction: %w", err)
	}

	committed = true
	metrics := map[string]int64{
		MetricFilesMerged:  int64(totalMerged),
		MetricFilesWritten: int64(len(newManifestEntries)),
		MetricBins:         int64(len(bins)),
		MetricDurationMs:   time.Since(start).Milliseconds(),
	}
	result := fmt.Sprintf("compacted %d files into %d (across %d bins)", totalMerged, len(newManifestEntries), len(bins))
	if rewritePlan != nil && rewritePlan.strategy == "sort" {
		result = fmt.Sprintf("%s using %s", result, rewritePlan.summaryLabel())
	}
	return result, metrics, nil
}

// buildCompactionBins groups small data files by partition for bin-packing.
// A file is "small" if it's below targetSize. A bin must have at least
// minFiles entries to be worth compacting.
func buildCompactionBins(entries []iceberg.ManifestEntry, targetSize int64, minFiles int) []compactionBin {
	if minFiles < 2 {
		minFiles = 2
	}

	// Group entries by spec ID + partition key so that files from different
	// partition specs are never mixed in the same compaction bin.
	groups := make(map[string]*compactionBin)
	for _, entry := range entries {
		df := entry.DataFile()
		// DuckDB writes "parquet" lowercase.
		if !strings.EqualFold(string(df.FileFormat()), string(iceberg.ParquetFile)) {
			continue
		}
		if df.FileSizeBytes() >= targetSize {
			continue
		}

		partKey := partitionKey(df.Partition())
		groupKey := fmt.Sprintf("spec%d\x00%s", df.SpecID(), partKey)
		bin, ok := groups[groupKey]
		if !ok {
			bin = &compactionBin{
				PartitionKey: partKey,
				Partition:    df.Partition(),
				SpecID:       df.SpecID(),
			}
			groups[groupKey] = bin
		}
		bin.Entries = append(bin.Entries, entry)
		bin.TotalSize += df.FileSizeBytes()
	}

	// Filter to bins with enough files, splitting oversized bins
	var result []compactionBin
	for _, bin := range groups {
		if len(bin.Entries) < minFiles {
			continue
		}
		if bin.TotalSize <= targetSize {
			result = append(result, *bin)
		} else {
			result = append(result, splitOversizedBin(*bin, targetSize, minFiles)...)
		}
	}

	// Sort by spec ID then partition key for deterministic order
	sort.Slice(result, func(i, j int) bool {
		if result[i].SpecID != result[j].SpecID {
			return result[i].SpecID < result[j].SpecID
		}
		return result[i].PartitionKey < result[j].PartitionKey
	})

	return result
}

// splitOversizedBin splits a bin whose total size exceeds targetSize into
// sub-bins that stay under targetSize. Bins that cannot reach minFiles
// without violating targetSize are left uncompacted rather than merged into
// oversized bins.
func splitOversizedBin(bin compactionBin, targetSize int64, minFiles int) []compactionBin {
	// Sort largest-first for better packing.
	sorted := make([]iceberg.ManifestEntry, len(bin.Entries))
	copy(sorted, bin.Entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].DataFile().FileSizeBytes() > sorted[j].DataFile().FileSizeBytes()
	})

	var bins []compactionBin
	current := compactionBin{
		PartitionKey: bin.PartitionKey,
		Partition:    bin.Partition,
		SpecID:       bin.SpecID,
	}
	for _, entry := range sorted {
		if current.TotalSize > 0 && current.TotalSize+entry.DataFile().FileSizeBytes() > targetSize {
			bins = append(bins, current)
			current = compactionBin{
				PartitionKey: bin.PartitionKey,
				Partition:    bin.Partition,
				SpecID:       bin.SpecID,
			}
		}
		current.Entries = append(current.Entries, entry)
		current.TotalSize += entry.DataFile().FileSizeBytes()
	}
	if len(current.Entries) > 0 {
		bins = append(bins, current)
	}

	var valid []compactionBin
	var pending []compactionBin
	for _, candidate := range bins {
		if len(candidate.Entries) >= minFiles {
			valid = append(valid, candidate)
			continue
		}
		pending = append(pending, candidate)
	}

	// Try to fold entries from underfilled bins into valid bins when they fit.
	for _, runt := range pending {
		for _, entry := range runt.Entries {
			bestIdx := -1
			bestRemaining := int64(-1)
			entrySize := entry.DataFile().FileSizeBytes()
			for i := range valid {
				remaining := targetSize - valid[i].TotalSize - entrySize
				if remaining < 0 {
					continue
				}
				if bestIdx == -1 || remaining < bestRemaining {
					bestIdx = i
					bestRemaining = remaining
				}
			}
			if bestIdx >= 0 {
				valid[bestIdx].Entries = append(valid[bestIdx].Entries, entry)
				valid[bestIdx].TotalSize += entrySize
			}
		}
	}

	if len(valid) == 0 {
		return nil
	}
	return valid
}

// partitionKey creates a string key from a partition map for grouping.
// Values are JSON-encoded to avoid ambiguity when values contain commas or '='.
func partitionKey(partition map[int]any) string {
	if len(partition) == 0 {
		return "__unpartitioned__"
	}

	// Sort field IDs for deterministic key
	ids := make([]int, 0, len(partition))
	for id := range partition {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	var parts []string
	for _, id := range ids {
		v, err := json.Marshal(partition[id])
		if err != nil {
			v = []byte(fmt.Sprintf("%x", fmt.Sprintf("%v", partition[id])))
		}
		parts = append(parts, fmt.Sprintf("%d=%s", id, v))
	}
	return strings.Join(parts, "\x00")
}

// deletionVectorPaths names the data files a Puffin deletion-vector entry
// covers, normalized to merge-time keys. Merging a covered file needs a
// Puffin reader to apply the vector; without one the merge would resurrect
// deleted rows, so the file must stay out of compaction bins.
func deletionVectorPaths(entries []iceberg.ManifestEntry, bucketName, dataPath string) (map[string]bool, error) {
	paths := make(map[string]bool)
	for _, entry := range entries {
		df := entry.DataFile()
		if df.ContentType() != iceberg.EntryContentPosDeletes || df.FileFormat() != iceberg.PuffinFile {
			continue
		}
		ref := df.ReferencedDataFile()
		if ref == nil || *ref == "" {
			return nil, fmt.Errorf("deletion vector %s has no referenced data file", df.FilePath())
		}
		normalized, err := normalizeIcebergPath(*ref, bucketName, dataPath)
		if err != nil {
			return nil, fmt.Errorf("resolve deletion vector source %s: %w", *ref, err)
		}
		paths[normalized] = true
	}
	return paths, nil
}

// excludeDeletionVectorFiles drops entries a deletion vector covers.
func excludeDeletionVectorFiles(entries []iceberg.ManifestEntry, dvPaths map[string]bool, bucketName, dataPath string) ([]iceberg.ManifestEntry, error) {
	if len(dvPaths) == 0 {
		return entries, nil
	}
	kept := make([]iceberg.ManifestEntry, 0, len(entries))
	for _, entry := range entries {
		normalized, err := normalizeIcebergPath(entry.DataFile().FilePath(), bucketName, dataPath)
		if err != nil {
			return nil, fmt.Errorf("resolve data file %s: %w", entry.DataFile().FilePath(), err)
		}
		if !dvPaths[normalized] {
			kept = append(kept, entry)
		}
	}
	return kept, nil
}

// collectPositionDeletes reads position delete Parquet files and returns a map
// from normalized data file path to sorted row positions that should be deleted.
// Paths are normalized so that absolute S3 URLs and relative paths match.
func collectPositionDeletes(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath string,
	deleteEntries []iceberg.ManifestEntry,
) (map[string][]int64, error) {
	result := make(map[string][]int64)
	for _, entry := range deleteEntries {
		df := entry.DataFile()
		if df.ContentType() != iceberg.EntryContentPosDeletes || df.FileFormat() == iceberg.PuffinFile {
			continue // Puffin is a deletion vector, not a row-level delete file
		}
		fileDeletes, err := readPositionDeleteFile(ctx, filerClient, bucketName, dataPath, entry.DataFile().FilePath())
		if err != nil {
			return nil, fmt.Errorf("read position delete file %s: %w", entry.DataFile().FilePath(), err)
		}
		for filePath, positions := range fileDeletes {
			normalized, err := normalizeIcebergPath(filePath, bucketName, dataPath)
			if err != nil {
				return nil, fmt.Errorf("resolve deleted file %s: %w", filePath, err)
			}
			result[normalized] = append(result[normalized], positions...)
		}
	}
	// Sort positions for each file (binary search during filtering)
	for filePath := range result {
		sort.Slice(result[filePath], func(i, j int) bool {
			return result[filePath][i] < result[filePath][j]
		})
	}
	return result, nil
}

// readPositionDeleteFile reads a position delete Parquet file and returns a map
// from data file path to row positions. The file must have columns "file_path"
// (string) and "pos" (int32 or int64).
func readPositionDeleteFile(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath, filePath string,
) (map[string][]int64, error) {
	data, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, filePath)
	if err != nil {
		return nil, err
	}
	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	pqSchema := reader.Schema()
	filePathIdx := -1
	posIdx := -1
	for i, col := range pqSchema.Columns() {
		name := strings.Join(col, ".")
		switch name {
		case "file_path":
			filePathIdx = i
		case "pos":
			posIdx = i
		}
	}
	if filePathIdx < 0 || posIdx < 0 {
		return nil, fmt.Errorf("position delete file %s missing required columns (file_path=%d, pos=%d)", filePath, filePathIdx, posIdx)
	}

	result := make(map[string][]int64)
	rows := make([]parquet.Row, 256)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		n, readErr := reader.ReadRows(rows)
		for i := 0; i < n; i++ {
			row := rows[i]
			fp := row[filePathIdx].String()
			pos := row[posIdx].Int64()
			result[fp] = append(result[fp], pos)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, readErr
		}
	}
	return result, nil
}

// equalityDeleteGroup holds a set of delete keys for a specific set of equality field IDs.
// Different equality delete files may use different field IDs, so deletes are grouped.
type equalityDeleteGroup struct {
	FieldIDs []int
	Keys     map[string]struct{}
}

// collectEqualityDeletes reads equality delete Parquet files and returns groups
// of delete keys, one per distinct set of equality field IDs. This correctly
// handles the case where different delete files use different equality columns.
func collectEqualityDeletes(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath string,
	deleteEntries []iceberg.ManifestEntry,
	schema *iceberg.Schema,
) ([]equalityDeleteGroup, error) {
	type groupState struct {
		fieldIDs []int
		keys     map[string]struct{}
	}
	groups := make(map[string]*groupState)

	for _, entry := range deleteEntries {
		if entry.DataFile().ContentType() != iceberg.EntryContentEqDeletes {
			continue
		}
		eqFieldIDs := entry.DataFile().EqualityFieldIDs()
		if len(eqFieldIDs) == 0 {
			continue
		}

		groupKey := fmt.Sprint(eqFieldIDs)
		gs, ok := groups[groupKey]
		if !ok {
			gs = &groupState{fieldIDs: eqFieldIDs, keys: make(map[string]struct{})}
			groups[groupKey] = gs
		}

		keys, err := readEqualityDeleteFile(ctx, filerClient, bucketName, dataPath, entry.DataFile().FilePath(), eqFieldIDs, schema)
		if err != nil {
			return nil, fmt.Errorf("read equality delete file %s: %w", entry.DataFile().FilePath(), err)
		}
		for k := range keys {
			gs.keys[k] = struct{}{}
		}
	}

	result := make([]equalityDeleteGroup, 0, len(groups))
	for _, gs := range groups {
		result = append(result, equalityDeleteGroup{FieldIDs: gs.fieldIDs, Keys: gs.keys})
	}
	return result, nil
}

// readEqualityDeleteFile reads an equality delete Parquet file and returns a set
// of composite keys built from the specified field IDs. The Iceberg schema is used
// to map field IDs to column names, which are then looked up in the Parquet schema.
func readEqualityDeleteFile(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath, filePath string,
	fieldIDs []int,
	icebergSchema *iceberg.Schema,
) (map[string]struct{}, error) {
	data, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, filePath)
	if err != nil {
		return nil, err
	}
	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()

	colIndices, err := resolveEqualityColIndices(reader.Schema(), fieldIDs, icebergSchema)
	if err != nil {
		return nil, fmt.Errorf("resolve columns in %s: %w", filePath, err)
	}

	result := make(map[string]struct{})
	rows := make([]parquet.Row, 256)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		n, readErr := reader.ReadRows(rows)
		for i := 0; i < n; i++ {
			key := buildEqualityKey(rows[i], colIndices)
			result[key] = struct{}{}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, readErr
		}
	}
	return result, nil
}

// buildEqualityKey builds a composite string key from specific column values
// in a row. Each value is serialized as "kind:length:value" to avoid ambiguity
// between types (e.g., int 123 vs string "123") and to prevent collisions from
// values containing separator characters.
func buildEqualityKey(row parquet.Row, colIndices []int) string {
	if len(colIndices) == 1 {
		v := row[colIndices[0]]
		s := v.String()
		return fmt.Sprintf("%d:%d:%s", v.Kind(), len(s), s)
	}
	var b strings.Builder
	for _, idx := range colIndices {
		v := row[idx]
		s := v.String()
		fmt.Fprintf(&b, "%d:%d:%s", v.Kind(), len(s), s)
	}
	return b.String()
}

// resolveEqualityColIndices maps Iceberg field IDs to Parquet column indices.
func resolveEqualityColIndices(pqSchema *parquet.Schema, fieldIDs []int, icebergSchema *iceberg.Schema) ([]int, error) {
	if len(fieldIDs) == 0 {
		return nil, nil
	}

	colNameToIdx := make(map[string]int)
	for i, col := range pqSchema.Columns() {
		colNameToIdx[strings.Join(col, ".")] = i
	}

	indices := make([]int, len(fieldIDs))
	for i, fid := range fieldIDs {
		field, ok := icebergSchema.FindFieldByID(fid)
		if !ok {
			return nil, fmt.Errorf("field ID %d not found in iceberg schema", fid)
		}
		idx, ok := colNameToIdx[field.Name]
		if !ok {
			return nil, fmt.Errorf("column %q (field ID %d) not found in parquet schema", field.Name, fid)
		}
		indices[i] = idx
	}
	return indices, nil
}

// mergeParquetFiles reads multiple small Parquet files and merges them into
// a single Parquet file, optionally filtering out rows matching position or
// equality deletes. Files are processed one at a time to keep memory usage
// proportional to a single input file plus the output buffer.
func mergeParquetFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath string,
	entries []iceberg.ManifestEntry,
	positionDeletes map[string][]int64,
	eqDeleteGroups []equalityDeleteGroup,
	icebergSchema *iceberg.Schema,
	lineage bool,
) ([]byte, int64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("no entries to merge")
	}

	// Load the first file to obtain the schema for the writer.
	firstData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, entries[0].DataFile().FilePath())
	if err != nil {
		return nil, 0, fmt.Errorf("read parquet file %s: %w", entries[0].DataFile().FilePath(), err)
	}
	firstReader := parquet.NewReader(bytes.NewReader(firstData))
	parquetSchema := firstReader.Schema()
	if parquetSchema == nil {
		firstReader.Close()
		return nil, 0, fmt.Errorf("no parquet schema found in %s", entries[0].DataFile().FilePath())
	}

	resolvedEqGroups, err := resolveEqualityDeleteGroupsForSchema(parquetSchema, eqDeleteGroups, icebergSchema)
	if err != nil {
		firstReader.Close()
		return nil, 0, fmt.Errorf("resolve equality columns: %w", err)
	}

	// On a v3 table the output is the first file's columns plus the lineage
	// columns (row_lineage.go).
	outputSchema := parquetSchema
	var layout *lineageLayout
	if lineage {
		if layout, err = newLineageLayout(parquetSchema); err != nil {
			firstReader.Close()
			return nil, 0, fmt.Errorf("schema of %s: %w", entries[0].DataFile().FilePath(), err)
		}
		outputSchema = layout.schema
	}

	var outputBuf bytes.Buffer
	writer := parquet.NewWriter(&outputBuf, outputSchema)

	drainReader := func(reader *parquet.Reader, entry iceberg.ManifestEntry) (int64, error) {
		source := entry.DataFile().FilePath()
		return copyFileRows(ctx, reader, entry, layout, bucketName, dataPath, positionDeletes, resolvedEqGroups, eqDeleteGroups, icebergSchema, func(filtered []parquet.Row) error {
			if _, err := writer.WriteRows(filtered); err != nil {
				return fmt.Errorf("write rows from %s: %w", source, err)
			}
			return nil
		})
	}

	// Drain the first file.
	totalRows, err := drainReader(firstReader, entries[0])
	if err != nil {
		writer.Close()
		return nil, 0, err
	}
	firstData = nil // allow GC

	// Process remaining files one at a time.
	for _, entry := range entries[1:] {
		select {
		case <-ctx.Done():
			writer.Close()
			return nil, 0, ctx.Err()
		default:
		}

		data, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, entry.DataFile().FilePath())
		if err != nil {
			writer.Close()
			return nil, 0, fmt.Errorf("read parquet file %s: %w", entry.DataFile().FilePath(), err)
		}

		reader := parquet.NewReader(bytes.NewReader(data))
		// A v3 merge matches files with their lineage columns set aside.
		if layout == nil && !schemasEqual(parquetSchema, reader.Schema()) {
			reader.Close()
			writer.Close()
			return nil, 0, fmt.Errorf("schema mismatch in %s: cannot merge files with different schemas", entry.DataFile().FilePath())
		}

		rowsWritten, err := drainReader(reader, entry)
		if err != nil {
			writer.Close()
			return nil, 0, err
		}
		totalRows += rowsWritten
		// data goes out of scope here, eligible for GC before next iteration.
	}

	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close writer: %w", err)
	}

	return outputBuf.Bytes(), totalRows, nil
}

type resolvedEqDeleteGroup struct {
	colIndices []int
	keys       map[string]struct{}
}

func resolveEqualityDeleteGroupsForSchema(
	parquetSchema *parquet.Schema,
	eqDeleteGroups []equalityDeleteGroup,
	icebergSchema *iceberg.Schema,
) ([]resolvedEqDeleteGroup, error) {
	if len(eqDeleteGroups) == 0 || icebergSchema == nil {
		return nil, nil
	}

	resolved := make([]resolvedEqDeleteGroup, 0, len(eqDeleteGroups))
	for _, g := range eqDeleteGroups {
		indices, err := resolveEqualityColIndices(parquetSchema, g.FieldIDs, icebergSchema)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, resolvedEqDeleteGroup{colIndices: indices, keys: g.Keys})
	}
	return resolved, nil
}

func visitFilteredParquetRows(
	ctx context.Context,
	reader *parquet.Reader,
	source, bucketName, dataPath string,
	positionDeletes map[string][]int64,
	resolvedEqGroups []resolvedEqDeleteGroup,
	onRows func(rows []parquet.Row, positions []int64) error,
) (int64, error) {
	defer reader.Close()

	rows := make([]parquet.Row, 256)
	filteredRows := make([]parquet.Row, 0, len(rows))
	// each kept row's position in the file before any delete: what a v3
	// _row_id is inherited from
	filteredPositions := make([]int64, 0, len(rows))
	normalizedSource, err := normalizeIcebergPath(source, bucketName, dataPath)
	if err != nil {
		return 0, err
	}
	posDeletes := positionDeletes[normalizedSource]
	posDeleteIdx := 0
	var absolutePos int64
	var totalRows int64

	for {
		select {
		case <-ctx.Done():
			return totalRows, ctx.Err()
		default:
		}

		n, readErr := reader.ReadRows(rows)
		if n > 0 {
			filteredRows = filteredRows[:0]
			filteredPositions = filteredPositions[:0]
			for i := 0; i < n; i++ {
				rowPos := absolutePos + int64(i)
				for posDeleteIdx < len(posDeletes) && posDeletes[posDeleteIdx] < rowPos {
					posDeleteIdx++
				}
				if posDeleteIdx < len(posDeletes) && posDeletes[posDeleteIdx] == rowPos {
					posDeleteIdx++
					continue
				}

				deleted := false
				for _, g := range resolvedEqGroups {
					key := buildEqualityKey(rows[i], g.colIndices)
					if _, ok := g.keys[key]; ok {
						deleted = true
						break
					}
				}
				if deleted {
					continue
				}

				filteredRows = append(filteredRows, rows[i])
				filteredPositions = append(filteredPositions, rowPos)
			}
			absolutePos += int64(n)
			if len(filteredRows) > 0 {
				if err := onRows(filteredRows, filteredPositions); err != nil {
					return totalRows, err
				}
				totalRows += int64(len(filteredRows))
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				return totalRows, nil
			}
			return totalRows, fmt.Errorf("read rows from %s: %w", source, readErr)
		}
	}
}

func mergeParquetFilesSorted(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, dataPath string,
	entries []iceberg.ManifestEntry,
	positionDeletes map[string][]int64,
	eqDeleteGroups []equalityDeleteGroup,
	icebergSchema *iceberg.Schema,
	rewritePlan *compactionRewritePlan,
	lineage bool,
) ([]byte, int64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("no entries to merge")
	}
	if rewritePlan == nil || rewritePlan.strategy != "sort" {
		return nil, 0, fmt.Errorf("sorted merge requires sort rewrite plan")
	}

	firstData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, entries[0].DataFile().FilePath())
	if err != nil {
		return nil, 0, fmt.Errorf("read parquet file %s: %w", entries[0].DataFile().FilePath(), err)
	}
	firstReader := parquet.NewReader(bytes.NewReader(firstData))
	parquetSchema := firstReader.Schema()
	if parquetSchema == nil {
		firstReader.Close()
		return nil, 0, fmt.Errorf("no parquet schema found in %s", entries[0].DataFile().FilePath())
	}

	outputSchema := parquetSchema
	var layout *lineageLayout
	if lineage {
		if layout, err = newLineageLayout(parquetSchema); err != nil {
			firstReader.Close()
			return nil, 0, fmt.Errorf("schema of %s: %w", entries[0].DataFile().FilePath(), err)
		}
		outputSchema = layout.schema
	}

	sortingColumns, err := rewritePlan.parquetSortingColumns(outputSchema)
	if err != nil {
		firstReader.Close()
		return nil, 0, fmt.Errorf("resolve parquet sorting columns: %w", err)
	}

	resolvedEqGroups, err := resolveEqualityDeleteGroupsForSchema(parquetSchema, eqDeleteGroups, icebergSchema)
	if err != nil {
		firstReader.Close()
		return nil, 0, fmt.Errorf("resolve equality columns: %w", err)
	}

	// Sorted rewrites stream rows through parquet-go's sorting writer rather
	// than holding the whole bin: rows accumulate until sortBufferRows, then a
	// sorted run is encoded into a temporary row group, and Close merges the
	// runs into the output. The runs live in files from a buffer pool instead
	// of on the heap, so a bin larger than memory sorts rather than failing.
	spillDir := rewritePlan.spillDir
	if spillDir == "" {
		// NewFileBufferPool takes filepath.Abs of what it is given, and
		// filepath.Abs("") is the working directory, not the temp directory.
		spillDir = os.TempDir()
	}
	sortBufferRows := rewritePlan.bufferRows
	if sortBufferRows < minSortBufferRows {
		sortBufferRows = defaultSortBufferRows
	}

	var outputBuf bytes.Buffer
	writer := parquet.NewSortingWriter[any](&outputBuf, sortBufferRows, outputSchema,
		parquet.SortingWriterConfig(
			parquet.SortingColumns(sortingColumns...),
			parquet.SortingBuffers(parquet.NewFileBufferPool(spillDir, "seaweedfs-iceberg-sort-*")),
		),
	)
	// Closing returns the run files to the pool, which deletes them. A failure
	// before that has to release them too, or the job leaves temp files behind.
	closed := false
	defer func() {
		if !closed {
			writer.Reset(io.Discard)
		}
	}()

	writeRows := func(reader *parquet.Reader, entry iceberg.ManifestEntry) (int64, error) {
		return copyFileRows(ctx, reader, entry, layout, bucketName, dataPath, positionDeletes, resolvedEqGroups, eqDeleteGroups, icebergSchema, func(filtered []parquet.Row) error {
			// The writer's row buffer copies the values it is handed, so these
			// rows need no cloning out of the reader's own buffer first.
			_, writeErr := writer.WriteRows(filtered)
			return writeErr
		})
	}

	totalRows, err := writeRows(firstReader, entries[0])
	if err != nil {
		return nil, 0, err
	}

	for _, entry := range entries[1:] {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		data, err := loadFileByIcebergPath(ctx, filerClient, bucketName, dataPath, entry.DataFile().FilePath())
		if err != nil {
			return nil, 0, fmt.Errorf("read parquet file %s: %w", entry.DataFile().FilePath(), err)
		}

		reader := parquet.NewReader(bytes.NewReader(data))
		if layout == nil && !schemasEqual(parquetSchema, reader.Schema()) {
			reader.Close()
			return nil, 0, fmt.Errorf("schema mismatch in %s: cannot merge files with different schemas", entry.DataFile().FilePath())
		}

		rowsWritten, err := writeRows(reader, entry)
		if err != nil {
			return nil, 0, err
		}
		totalRows += rowsWritten
	}

	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close sorting writer: %w", err)
	}
	closed = true

	return outputBuf.Bytes(), totalRows, nil
}

// compactRandomSuffix returns a short random hex string for use in artifact
// filenames to prevent collisions between concurrent runs.
func compactRandomSuffix() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano()&0xFFFFFFFF)
	}
	return hex.EncodeToString(b)
}

// schemasEqual compares two parquet schemas structurally.
func schemasEqual(a, b *parquet.Schema) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return parquet.EqualNodes(a, b)
}

// ensureFilerDir ensures a directory exists in the filer.
func ensureFilerDir(ctx context.Context, client filer_pb.SeaweedFilerClient, dirPath string) error {
	parentDir := path.Dir(dirPath)
	dirName := path.Base(dirPath)

	_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parentDir,
		Name:      dirName,
	})
	if err == nil {
		return nil // already exists
	}
	if !errors.Is(err, filer_pb.ErrNotFound) && status.Code(err) != codes.NotFound {
		return fmt.Errorf("lookup dir %s: %w", dirPath, err)
	}

	resp, createErr := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: parentDir,
		Entry: &filer_pb.Entry{
			Name:        dirName,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0755),
			},
		},
	})
	if createErr != nil {
		return createErr
	}
	if resp.Error != "" && !strings.Contains(resp.Error, "exist") {
		return fmt.Errorf("create dir %s: %s", dirPath, resp.Error)
	}
	return nil
}
