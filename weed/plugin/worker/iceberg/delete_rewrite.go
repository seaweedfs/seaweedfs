package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

type deleteRewriteInput struct {
	Entry          iceberg.ManifestEntry
	ReferencedPath string
	Positions      []int64
}

type deleteRewriteGroup struct {
	SpecID         int32
	Partition      map[int]any
	PartitionKey   string
	ReferencedPath string
	Inputs         []deleteRewriteInput
	TotalSize      int64
}

type positionDeleteRow struct {
	FilePath string `parquet:"file_path"`
	Pos      int64  `parquet:"pos"`
}

func hasEligibleDeleteRewrite(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	manifests []iceberg.ManifestFile,
	config Config,
) (bool, error) {
	groups, _, err := collectDeleteRewriteGroups(ctx, filerClient, bucketName, tablePath, manifests)
	if err != nil {
		return false, err
	}
	for _, group := range groups {
		if groupEligibleForRewrite(group, config) {
			return true, nil
		}
	}
	return false, nil
}

func collectDeleteRewriteGroups(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	manifests []iceberg.ManifestFile,
) (map[string]*deleteRewriteGroup, []iceberg.ManifestEntry, error) {
	groups := make(map[string]*deleteRewriteGroup)
	var allPositionEntries []iceberg.ManifestEntry

	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}

		manifestData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
		if err != nil {
			return nil, nil, fmt.Errorf("read delete manifest %s: %w", mf.FilePath(), err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
		if err != nil {
			return nil, nil, fmt.Errorf("parse delete manifest %s: %w", mf.FilePath(), err)
		}

		for _, entry := range entries {
			if entry.DataFile().ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			allPositionEntries = append(allPositionEntries, entry)

			fileDeletes, err := readPositionDeleteFile(ctx, filerClient, bucketName, tablePath, entry.DataFile().FilePath())
			if err != nil {
				return nil, nil, fmt.Errorf("read position delete file %s: %w", entry.DataFile().FilePath(), err)
			}
			if len(fileDeletes) != 1 {
				// Phase 1 only rewrites files that target a single data file.
				continue
			}

			var referencedPath string
			var positions []int64
			for fp, pos := range fileDeletes {
				referencedPath = normalizeIcebergPath(fp, bucketName, tablePath)
				positions = append(positions, pos...)
			}
			sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })

			partKey := partitionKey(entry.DataFile().Partition())
			groupKey := fmt.Sprintf("spec%d\x00%s\x00%s", entry.DataFile().SpecID(), partKey, referencedPath)
			group, ok := groups[groupKey]
			if !ok {
				group = &deleteRewriteGroup{
					SpecID:         entry.DataFile().SpecID(),
					Partition:      entry.DataFile().Partition(),
					PartitionKey:   partKey,
					ReferencedPath: referencedPath,
				}
				groups[groupKey] = group
			}
			group.Inputs = append(group.Inputs, deleteRewriteInput{
				Entry:          entry,
				ReferencedPath: referencedPath,
				Positions:      positions,
			})
			group.TotalSize += entry.DataFile().FileSizeBytes()
		}
	}

	return groups, allPositionEntries, nil
}

func groupEligibleForRewrite(group *deleteRewriteGroup, config Config) bool {
	if group == nil {
		return false
	}
	if len(group.Inputs) < 2 {
		return false
	}
	if group.TotalSize > config.DeleteMaxFileGroupSizeBytes {
		return false
	}
	target := config.DeleteTargetFileSizeBytes
	if target <= 0 {
		target = defaultDeleteTargetFileSizeMB * 1024 * 1024
	}
	outputFiles := int64(estimatedDeleteOutputFiles(group.TotalSize, target))
	if config.DeleteMaxOutputFiles > 0 && outputFiles > config.DeleteMaxOutputFiles {
		return false
	}
	return int64(len(group.Inputs)) >= config.DeleteMinInputFiles
}

func estimatedDeleteOutputFiles(totalSize, targetSize int64) int {
	if totalSize <= 0 || targetSize <= 0 {
		return 1
	}
	count := int(math.Ceil(float64(totalSize) / float64(targetSize)))
	if count < 1 {
		return 1
	}
	return count
}

func manifestEntrySeqNum(entry iceberg.ManifestEntry) *int64 {
	seqNum := entry.SequenceNum()
	if seqNum < 0 {
		return nil
	}
	return &seqNum
}

func manifestEntryFileSeqNum(entry iceberg.ManifestEntry) *int64 {
	if fileSeqNum := entry.FileSequenceNum(); fileSeqNum != nil {
		value := *fileSeqNum
		return &value
	}
	return manifestEntrySeqNum(entry)
}

func writeManifestWithContent(
	filename string,
	version int,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	snapshotID int64,
	entries []iceberg.ManifestEntry,
	content iceberg.ManifestContent,
) (iceberg.ManifestFile, []byte, error) {
	var manifestBuf bytes.Buffer
	mf, err := iceberg.WriteManifest(filename, &manifestBuf, version, spec, schema, snapshotID, entries)
	if err != nil {
		return nil, nil, err
	}

	manifestBytes := manifestBuf.Bytes()
	if content == iceberg.ManifestContentDeletes {
		manifestBytes, err = patchManifestContentBytesToDeletes(manifestBytes)
		if err != nil {
			return nil, nil, err
		}
	}

	rebuilt := iceberg.NewManifestFile(version, filename, int64(len(manifestBytes)), int32(spec.ID()), snapshotID).
		Content(content).
		AddedFiles(mf.AddedDataFiles()).
		ExistingFiles(mf.ExistingDataFiles()).
		DeletedFiles(mf.DeletedDataFiles()).
		AddedRows(mf.AddedRows()).
		ExistingRows(mf.ExistingRows()).
		DeletedRows(mf.DeletedRows()).
		Partitions(mf.Partitions()).
		Build()
	return rebuilt, manifestBytes, nil
}

func patchManifestContentBytesToDeletes(manifestBytes []byte) ([]byte, error) {
	old := append([]byte{0x0e}, []byte("content")...)
	old = append(old, 0x08)
	old = append(old, []byte("data")...)

	new := append([]byte{0x0e}, []byte("content")...)
	new = append(new, 0x0e)
	new = append(new, []byte("deletes")...)

	result := bytes.Replace(manifestBytes, old, new, 1)
	if bytes.Equal(result, manifestBytes) {
		return nil, fmt.Errorf("delete manifest content patch failed")
	}
	return result, nil
}

func writePositionDeleteFile(rows []positionDeleteRow) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, parquet.SchemaOf(new(positionDeleteRow)))
	for _, row := range rows {
		if err := writer.Write(&row); err != nil {
			return nil, fmt.Errorf("write position delete row: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close position delete file: %w", err)
	}
	return buf.Bytes(), nil
}

func (h *Handler) rewritePositionDeleteFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	config Config,
) (string, map[string]int64, error) {
	start := time.Now()
	meta, metadataFileName, err := loadCurrentMetadata(ctx, filerClient, bucketName, tablePath)
	if err != nil {
		return "", nil, fmt.Errorf("load metadata: %w", err)
	}

	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return "no current snapshot", nil, nil
	}

	manifestListData, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, currentSnap.ManifestList)
	if err != nil {
		return "", nil, fmt.Errorf("read manifest list: %w", err)
	}
	manifests, err := iceberg.ReadManifestList(bytes.NewReader(manifestListData))
	if err != nil {
		return "", nil, fmt.Errorf("parse manifest list: %w", err)
	}

	var dataManifests []iceberg.ManifestFile
	var allEqualityEntries []iceberg.ManifestEntry
	for _, mf := range manifests {
		switch mf.ManifestContent() {
		case iceberg.ManifestContentData:
			dataManifests = append(dataManifests, mf)
		case iceberg.ManifestContentDeletes:
			manifestData, readErr := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, mf.FilePath())
			if readErr != nil {
				return "", nil, fmt.Errorf("read delete manifest %s: %w", mf.FilePath(), readErr)
			}
			entries, parseErr := iceberg.ReadManifest(mf, bytes.NewReader(manifestData), true)
			if parseErr != nil {
				return "", nil, fmt.Errorf("parse delete manifest %s: %w", mf.FilePath(), parseErr)
			}
			for _, entry := range entries {
				if entry.DataFile().ContentType() == iceberg.EntryContentEqDeletes {
					allEqualityEntries = append(allEqualityEntries, entry)
				}
			}
		}
	}

	groupMap, allPositionEntries, err := collectDeleteRewriteGroups(ctx, filerClient, bucketName, tablePath, manifests)
	if err != nil {
		return "", nil, err
	}
	if len(groupMap) == 0 {
		return "no position delete files eligible for rewrite", nil, nil
	}

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
				glog.Warningf("iceberg delete rewrite: failed to clean up artifact %s/%s: %v", a.dir, a.fileName, err)
			}
		}
	}()

	specByID := make(map[int]iceberg.PartitionSpec)
	for _, ps := range meta.PartitionSpecs() {
		specByID[ps.ID()] = ps
	}

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

	newSnapID := time.Now().UnixMilli()
	version := meta.Version()
	snapshotID := currentSnap.SnapshotID
	seqNum := currentSnap.SequenceNumber + 1
	metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	dataDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "data")
	artifactSuffix := compactRandomSuffix()

	replacedPaths := make(map[string]struct{})
	var rewrittenGroups int64
	var skippedGroups int64
	var deleteFilesRewritten int64
	var deleteFilesWritten int64
	var deleteBytesRewritten int64

	sortedKeys := make([]string, 0, len(groupMap))
	for key := range groupMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		group := groupMap[key]
		if !groupEligibleForRewrite(group, config) {
			skippedGroups++
			continue
		}
		rows := make([]positionDeleteRow, 0)
		for _, input := range group.Inputs {
			for _, pos := range input.Positions {
				rows = append(rows, positionDeleteRow{FilePath: input.ReferencedPath, Pos: pos})
			}
			replacedPaths[input.Entry.DataFile().FilePath()] = struct{}{}
			deleteFilesRewritten++
			deleteBytesRewritten += input.Entry.DataFile().FileSizeBytes()
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].FilePath != rows[j].FilePath {
				return rows[i].FilePath < rows[j].FilePath
			}
			return rows[i].Pos < rows[j].Pos
		})

		outputFiles := estimatedDeleteOutputFiles(group.TotalSize, config.DeleteTargetFileSizeBytes)
		rowsPerFile := (len(rows) + outputFiles - 1) / outputFiles
		if rowsPerFile < 1 {
			rowsPerFile = len(rows)
		}

		for startIdx, fileIdx := 0, 0; startIdx < len(rows); startIdx, fileIdx = startIdx+rowsPerFile, fileIdx+1 {
			endIdx := startIdx + rowsPerFile
			if endIdx > len(rows) {
				endIdx = len(rows)
			}
			outputRows := rows[startIdx:endIdx]
			deleteBytes, err := writePositionDeleteFile(outputRows)
			if err != nil {
				return "", nil, err
			}
			fileName := fmt.Sprintf("rewrite-delete-%d-%s-%d.parquet", newSnapID, artifactSuffix, deleteFilesWritten)
			if err := ensureFilerDir(ctx, filerClient, dataDir); err != nil {
				return "", nil, fmt.Errorf("ensure data dir: %w", err)
			}
			if err := saveFilerFile(ctx, filerClient, dataDir, fileName, deleteBytes); err != nil {
				return "", nil, fmt.Errorf("save rewritten delete file: %w", err)
			}
			writtenArtifacts = append(writtenArtifacts, artifact{dir: dataDir, fileName: fileName})

			spec, ok := specByID[int(group.SpecID)]
			if !ok {
				return "", nil, fmt.Errorf("partition spec %d not found", group.SpecID)
			}
			dfBuilder, err := iceberg.NewDataFileBuilder(
				spec,
				iceberg.EntryContentPosDeletes,
				path.Join("data", fileName),
				iceberg.ParquetFile,
				group.Partition,
				nil, nil,
				int64(len(outputRows)),
				int64(len(deleteBytes)),
			)
			if err != nil {
				return "", nil, fmt.Errorf("build rewritten delete file: %w", err)
			}
			entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &newSnapID, nil, nil, dfBuilder.Build())
			addToSpec(group.SpecID, entry)
			deleteFilesWritten++
		}

		for _, input := range group.Inputs {
			delEntry := iceberg.NewManifestEntry(
				iceberg.EntryStatusDELETED,
				&newSnapID,
				manifestEntrySeqNum(input.Entry),
				manifestEntryFileSeqNum(input.Entry),
				input.Entry.DataFile(),
			)
			addToSpec(group.SpecID, delEntry)
		}
		rewrittenGroups++
	}

	if rewrittenGroups == 0 {
		return "no position delete files eligible for rewrite", nil, nil
	}

	for _, entry := range allEqualityEntries {
		existingEntry := iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING,
			func() *int64 { id := entry.SnapshotID(); return &id }(),
			manifestEntrySeqNum(entry),
			manifestEntryFileSeqNum(entry),
			entry.DataFile(),
		)
		addToSpec(entry.DataFile().SpecID(), existingEntry)
	}

	for _, entry := range allPositionEntries {
		if _, replaced := replacedPaths[entry.DataFile().FilePath()]; replaced {
			continue
		}
		existingEntry := iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING,
			func() *int64 { id := entry.SnapshotID(); return &id }(),
			manifestEntrySeqNum(entry),
			manifestEntryFileSeqNum(entry),
			entry.DataFile(),
		)
		addToSpec(entry.DataFile().SpecID(), existingEntry)
	}

	sortedSpecIDs := make([]int32, 0, len(specEntriesMap))
	for specID := range specEntriesMap {
		sortedSpecIDs = append(sortedSpecIDs, specID)
	}
	sort.Slice(sortedSpecIDs, func(i, j int) bool { return sortedSpecIDs[i] < sortedSpecIDs[j] })

	allManifests := make([]iceberg.ManifestFile, 0, len(dataManifests)+len(sortedSpecIDs))
	allManifests = append(allManifests, dataManifests...)

	for _, specID := range sortedSpecIDs {
		spec, ok := specByID[int(specID)]
		if !ok {
			return "", nil, fmt.Errorf("partition spec %d not found", specID)
		}
		manifestName := fmt.Sprintf("rewrite-delete-%d-%s-spec%d.avro", newSnapID, artifactSuffix, specID)
		manifestPath := path.Join("metadata", manifestName)
		mf, manifestBytes, err := writeManifestWithContent(
			manifestPath,
			version,
			spec,
			meta.CurrentSchema(),
			newSnapID,
			specEntriesMap[specID].entries,
			iceberg.ManifestContentDeletes,
		)
		if err != nil {
			return "", nil, fmt.Errorf("write delete manifest for spec %d: %w", specID, err)
		}
		if err := saveFilerFile(ctx, filerClient, metaDir, manifestName, manifestBytes); err != nil {
			return "", nil, fmt.Errorf("save delete manifest for spec %d: %w", specID, err)
		}
		writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestName})
		allManifests = append(allManifests, mf)
	}

	var manifestListBuf bytes.Buffer
	if err := iceberg.WriteManifestList(version, &manifestListBuf, newSnapID, &snapshotID, &seqNum, 0, allManifests); err != nil {
		return "", nil, fmt.Errorf("write delete manifest list: %w", err)
	}
	manifestListName := fmt.Sprintf("snap-%d-%s.avro", newSnapID, artifactSuffix)
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestListName, manifestListBuf.Bytes()); err != nil {
		return "", nil, fmt.Errorf("save delete manifest list: %w", err)
	}
	writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestListName})

	manifestListLocation := path.Join("metadata", manifestListName)
	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		cs := currentMeta.CurrentSnapshot()
		if cs == nil || cs.SnapshotID != snapshotID {
			return errStalePlan
		}
		newSnapshot := &table.Snapshot{
			SnapshotID:       newSnapID,
			ParentSnapshotID: &snapshotID,
			SequenceNumber:   seqNum,
			TimestampMs:      newSnapID,
			ManifestList:     manifestListLocation,
			Summary: &table.Summary{
				Operation: table.OpReplace,
				Properties: map[string]string{
					"maintenance":            "rewrite_position_delete_files",
					"delete-files-rewritten": fmt.Sprintf("%d", deleteFilesRewritten),
					"delete-files-written":   fmt.Sprintf("%d", deleteFilesWritten),
					"delete-groups":          fmt.Sprintf("%d", rewrittenGroups),
				},
			},
			SchemaID: func() *int {
				id := meta.CurrentSchema().ID
				return &id
			}(),
		}
		if err := builder.AddSnapshot(newSnapshot); err != nil {
			return err
		}
		return builder.SetSnapshotRef(table.MainBranch, newSnapID, table.BranchRef)
	})
	if err != nil {
		return "", nil, fmt.Errorf("commit delete rewrite: %w", err)
	}

	committed = true
	metrics := map[string]int64{
		MetricDeleteFilesRewritten: deleteFilesRewritten,
		MetricDeleteFilesWritten:   deleteFilesWritten,
		MetricDeleteBytesRewritten: deleteBytesRewritten,
		MetricDeleteGroupsPlanned:  rewrittenGroups,
		MetricDeleteGroupsSkipped:  skippedGroups,
		MetricDurationMs:           time.Since(start).Milliseconds(),
	}
	return fmt.Sprintf(
		"rewrote %d position delete files into %d across %d group(s)",
		deleteFilesRewritten,
		deleteFilesWritten,
		rewrittenGroups,
	), metrics, nil
}
