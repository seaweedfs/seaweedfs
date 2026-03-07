package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// compactionBin groups small data files from the same partition for merging.
type compactionBin struct {
	PartitionKey string
	Partition    map[int]any
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
) (string, error) {
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

	// Abort if delete manifests exist — the compactor does not apply deletes,
	// so carrying them through could produce incorrect results.
	// Also detect multiple partition specs — the compactor writes a single
	// manifest under the current spec which is invalid for spec-evolved tables.
	specIDs := make(map[int32]struct{})
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			return "compaction skipped: delete manifests present (not yet supported)", nil
		}
		specIDs[mf.PartitionSpecID()] = struct{}{}
	}
	if len(specIDs) > 1 {
		return "compaction skipped: multiple partition specs present (not yet supported)", nil
	}

	// Collect data file entries from data manifests
	var allEntries []iceberg.ManifestEntry
	for _, mf := range manifests {
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

	// Build compaction bins: group small files by partition
	// MinInputFiles is clamped by ParseConfig to [2, ...] so int conversion is safe.
	bins := buildCompactionBins(allEntries, config.TargetFileSizeBytes, int(config.MinInputFiles))
	if len(bins) == 0 {
		return "no files eligible for compaction", nil
	}

	spec := meta.PartitionSpec()
	schema := meta.CurrentSchema()
	version := meta.Version()
	snapshotID := currentSnap.SnapshotID

	// Compute the snapshot ID for the commit up front so all manifest entries
	// reference the same snapshot that will actually be committed.
	newSnapID := time.Now().UnixMilli()

	// Process each bin: read source Parquet files, merge, write output
	var newManifestEntries []iceberg.ManifestEntry
	var deletedManifestEntries []iceberg.ManifestEntry
	totalMerged := 0

	metaDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "metadata")
	dataDir := path.Join(s3tables.TablesPath, bucketName, tablePath, "data")

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
			return "", ctx.Err()
		default:
		}

		mergedFileName := fmt.Sprintf("compact-%d-%d-%d.parquet", snapshotID, newSnapID, binIdx)
		mergedFilePath := path.Join("data", mergedFileName)

		mergedData, recordCount, err := mergeParquetFiles(ctx, filerClient, bucketName, tablePath, bin.Entries)
		if err != nil {
			glog.Warningf("iceberg compact: failed to merge bin %d (%d files): %v", binIdx, len(bin.Entries), err)
			continue
		}

		// Write merged file to filer
		if err := ensureFilerDir(ctx, filerClient, dataDir); err != nil {
			return "", fmt.Errorf("ensure data dir: %w", err)
		}
		if err := saveFilerFile(ctx, filerClient, dataDir, mergedFileName, mergedData); err != nil {
			return "", fmt.Errorf("save merged file: %w", err)
		}

		// Create new DataFile entry for the merged file
		dfBuilder, err := iceberg.NewDataFileBuilder(
			spec,
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
			// Clean up the written file
			_ = deleteFilerFile(ctx, filerClient, dataDir, mergedFileName)
			continue
		}
		writtenArtifacts = append(writtenArtifacts, artifact{dir: dataDir, fileName: mergedFileName})

		newEntry := iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED,
			&newSnapID,
			nil, nil,
			dfBuilder.Build(),
		)
		newManifestEntries = append(newManifestEntries, newEntry)

		// Mark original entries as deleted
		for _, entry := range bin.Entries {
			delEntry := iceberg.NewManifestEntry(
				iceberg.EntryStatusDELETED,
				&newSnapID,
				nil, nil,
				entry.DataFile(),
			)
			deletedManifestEntries = append(deletedManifestEntries, delEntry)
		}

		totalMerged += len(bin.Entries)
	}

	if len(newManifestEntries) == 0 {
		return "no bins successfully compacted", nil
	}

	// Build entries for the new manifest:
	// - ADDED entries for merged files
	// - DELETED entries for original files
	// - EXISTING entries for files that weren't compacted
	compactedPaths := make(map[string]struct{})
	for _, entry := range deletedManifestEntries {
		compactedPaths[entry.DataFile().FilePath()] = struct{}{}
	}

	var manifestEntries []iceberg.ManifestEntry
	manifestEntries = append(manifestEntries, newManifestEntries...)
	manifestEntries = append(manifestEntries, deletedManifestEntries...)

	// Keep existing entries that weren't compacted
	for _, entry := range allEntries {
		if _, compacted := compactedPaths[entry.DataFile().FilePath()]; !compacted {
			existingEntry := iceberg.NewManifestEntry(
				iceberg.EntryStatusEXISTING,
				func() *int64 { id := entry.SnapshotID(); return &id }(),
				nil, nil,
				entry.DataFile(),
			)
			manifestEntries = append(manifestEntries, existingEntry)
		}
	}

	// Write new manifest
	var manifestBuf bytes.Buffer
	manifestFileName := fmt.Sprintf("compact-%d.avro", newSnapID)
	newManifest, err := iceberg.WriteManifest(
		path.Join("metadata", manifestFileName),
		&manifestBuf,
		version,
		spec,
		schema,
		newSnapID,
		manifestEntries,
	)
	if err != nil {
		return "", fmt.Errorf("write compact manifest: %w", err)
	}

	if err := saveFilerFile(ctx, filerClient, metaDir, manifestFileName, manifestBuf.Bytes()); err != nil {
		return "", fmt.Errorf("save compact manifest: %w", err)
	}
	writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestFileName})

	// Build manifest list with only the new manifest (the early abort at the
	// top of this function guarantees no delete manifests are present).
	allManifests := []iceberg.ManifestFile{newManifest}

	// Write new manifest list
	var manifestListBuf bytes.Buffer
	seqNum := currentSnap.SequenceNumber + 1
	err = iceberg.WriteManifestList(version, &manifestListBuf, newSnapID, &snapshotID, &seqNum, 0, allManifests)
	if err != nil {
		return "", fmt.Errorf("write compact manifest list: %w", err)
	}

	manifestListFileName := fmt.Sprintf("snap-%d.avro", newSnapID)
	if err := saveFilerFile(ctx, filerClient, metaDir, manifestListFileName, manifestListBuf.Bytes()); err != nil {
		return "", fmt.Errorf("save compact manifest list: %w", err)
	}
	writtenArtifacts = append(writtenArtifacts, artifact{dir: metaDir, fileName: manifestListFileName})

	// Commit: add new snapshot and update main branch ref
	manifestListLocation := path.Join("metadata", manifestListFileName)
	err = h.commitWithRetry(ctx, filerClient, bucketName, tablePath, metadataFileName, config, func(currentMeta table.Metadata, builder *table.MetadataBuilder) error {
		// Guard: verify table head hasn't advanced since we planned.
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
					"maintenance":     "compact_data_files",
					"merged-files":    fmt.Sprintf("%d", totalMerged),
					"new-files":       fmt.Sprintf("%d", len(newManifestEntries)),
					"compaction-bins": fmt.Sprintf("%d", len(bins)),
				},
			},
			SchemaID: func() *int {
				id := schema.ID
				return &id
			}(),
		}
		if err := builder.AddSnapshot(newSnapshot); err != nil {
			return err
		}
		return builder.SetSnapshotRef(table.MainBranch, newSnapID, table.BranchRef)
	})
	if err != nil {
		return "", fmt.Errorf("commit compaction: %w", err)
	}

	committed = true
	return fmt.Sprintf("compacted %d files into %d (across %d bins)", totalMerged, len(newManifestEntries), len(bins)), nil
}

// buildCompactionBins groups small data files by partition for bin-packing.
// A file is "small" if it's below targetSize. A bin must have at least
// minFiles entries to be worth compacting.
func buildCompactionBins(entries []iceberg.ManifestEntry, targetSize int64, minFiles int) []compactionBin {
	if minFiles < 2 {
		minFiles = 2
	}

	// Group entries by partition key
	groups := make(map[string]*compactionBin)
	for _, entry := range entries {
		df := entry.DataFile()
		if df.FileFormat() != iceberg.ParquetFile {
			continue
		}
		if df.FileSizeBytes() >= targetSize {
			continue
		}

		partKey := partitionKey(df.Partition())
		bin, ok := groups[partKey]
		if !ok {
			bin = &compactionBin{
				PartitionKey: partKey,
				Partition:    df.Partition(),
			}
			groups[partKey] = bin
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

	// Sort by partition key for deterministic order
	sort.Slice(result, func(i, j int) bool {
		return result[i].PartitionKey < result[j].PartitionKey
	})

	return result
}

// splitOversizedBin splits a bin whose total size exceeds targetSize into
// sub-bins that each stay under targetSize while meeting minFiles.
func splitOversizedBin(bin compactionBin, targetSize int64, minFiles int) []compactionBin {
	var bins []compactionBin
	current := compactionBin{
		PartitionKey: bin.PartitionKey,
		Partition:    bin.Partition,
	}
	for _, entry := range bin.Entries {
		if current.TotalSize > 0 && current.TotalSize+entry.DataFile().FileSizeBytes() > targetSize && len(current.Entries) >= minFiles {
			bins = append(bins, current)
			current = compactionBin{
				PartitionKey: bin.PartitionKey,
				Partition:    bin.Partition,
			}
		}
		current.Entries = append(current.Entries, entry)
		current.TotalSize += entry.DataFile().FileSizeBytes()
	}
	if len(current.Entries) >= minFiles {
		bins = append(bins, current)
	}
	return bins
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

// mergeParquetFiles reads multiple small Parquet files and merges them into
// a single Parquet file. It reads rows from each source and writes them to
// the output using the schema from the first file.
//
// Files are loaded into memory (appropriate for compacting small files).
func mergeParquetFiles(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	entries []iceberg.ManifestEntry,
) ([]byte, int64, error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("no entries to merge")
	}

	// Read all source files and create parquet readers
	type sourceFile struct {
		reader *parquet.Reader
		data   []byte
	}
	var sources []sourceFile
	defer func() {
		for _, src := range sources {
			if src.reader != nil {
				src.reader.Close()
			}
		}
	}()

	var parquetSchema *parquet.Schema
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		data, err := loadFileByIcebergPath(ctx, filerClient, bucketName, tablePath, entry.DataFile().FilePath())
		if err != nil {
			return nil, 0, fmt.Errorf("read parquet file %s: %w", entry.DataFile().FilePath(), err)
		}

		reader := parquet.NewReader(bytes.NewReader(data))
		readerSchema := reader.Schema()
		if parquetSchema == nil {
			parquetSchema = readerSchema
		} else if !schemasEqual(parquetSchema, readerSchema) {
			return nil, 0, fmt.Errorf("schema mismatch in %s: cannot merge files with different schemas", entry.DataFile().FilePath())
		}
		sources = append(sources, sourceFile{reader: reader, data: data})
	}

	if parquetSchema == nil {
		return nil, 0, fmt.Errorf("no parquet schema found")
	}

	// Write merged output
	var outputBuf bytes.Buffer
	writer := parquet.NewWriter(&outputBuf, parquetSchema)

	var totalRows int64
	rows := make([]parquet.Row, 256)

	for _, src := range sources {
		for {
			n, err := src.reader.ReadRows(rows)
			if n > 0 {
				if _, writeErr := writer.WriteRows(rows[:n]); writeErr != nil {
					writer.Close()
					return nil, 0, fmt.Errorf("write rows: %w", writeErr)
				}
				totalRows += int64(n)
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				writer.Close()
				return nil, 0, fmt.Errorf("read rows: %w", err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("close writer: %w", err)
	}

	return outputBuf.Bytes(), totalRows, nil
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
