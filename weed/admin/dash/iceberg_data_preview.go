package dash

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/iceberg-go"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const (
	icebergPreviewDefaultRows  = 50
	icebergPreviewMaxRows      = 200
	icebergPreviewMaxFiles     = 5
	icebergPreviewMaxManifests = 100
	icebergPreviewMaxListed    = 500
	icebergPreviewMaxCellChars = 200
	icebergPreviewMaxMetaBytes = 64 << 20
)

type IcebergDataFileInfo struct {
	Path        string `json:"path"`
	Format      string `json:"format"`
	RecordCount int64  `json:"record_count"`
	SizeBytes   int64  `json:"size_bytes"`
}

// IcebergDataPreviewData backs the table data-preview page. PreviewError and
// PreviewNotes report per-snapshot problems without failing the whole page.
type IcebergDataPreviewData struct {
	Username          string                `json:"username"`
	CatalogName       string                `json:"catalog_name"`
	NamespaceName     string                `json:"namespace_name"`
	TableName         string                `json:"table_name"`
	BucketARN         string                `json:"bucket_arn"`
	SnapshotID        int64                 `json:"snapshot_id"`
	SnapshotTime      time.Time             `json:"snapshot_time"`
	CurrentSnapshotID int64                 `json:"current_snapshot_id"`
	Snapshots         []IcebergSnapshotInfo `json:"snapshots"`
	Columns           []string              `json:"columns"`
	Rows              [][]string            `json:"rows"`
	RowLimit          int                   `json:"row_limit"`
	ScannedFiles      int                   `json:"scanned_files"`
	SelectedFile      string                `json:"selected_file"`
	DataFiles         []IcebergDataFileInfo `json:"data_files"`
	TotalDataFiles    int                   `json:"total_data_files"`
	TotalRecords      int64                 `json:"total_records"`
	HasDeletes        bool                  `json:"has_deletes"`
	PreviewNotes      []string              `json:"preview_notes,omitempty"`
	PreviewError      string                `json:"preview_error,omitempty"`
	LastUpdated       time.Time             `json:"last_updated"`
}

// GetIcebergTableDataPreview walks the selected snapshot's manifests and reads
// sample rows from its Parquet data files. snapshotID 0 means the current
// snapshot; selectedFile restricts the preview to one manifest-listed file.
func (s *AdminServer) GetIcebergTableDataPreview(ctx context.Context, catalogName, bucketArn, namespace, tableName string, snapshotID int64, selectedFile string, rowLimit int) (IcebergDataPreviewData, error) {
	if rowLimit < 1 {
		rowLimit = icebergPreviewDefaultRows
	}
	if rowLimit > icebergPreviewMaxRows {
		rowLimit = icebergPreviewMaxRows
	}

	data := IcebergDataPreviewData{
		CatalogName:   catalogName,
		NamespaceName: namespace,
		TableName:     tableName,
		BucketARN:     bucketArn,
		RowLimit:      rowLimit,
		SelectedFile:  selectedFile,
		LastUpdated:   time.Now(),
	}

	namespaceParts, err := parseNamespaceInput(namespace)
	if err != nil {
		return data, err
	}
	var resp s3tables.GetTableResponse
	req := &s3tables.GetTableRequest{TableBucketARN: bucketArn, Namespace: namespaceParts, Name: tableName}
	if err := s.executeS3TablesOperation(ctx, "GetTable", req, &resp); err != nil {
		return data, err
	}
	if resp.Metadata == nil || len(resp.Metadata.FullMetadata) == 0 {
		data.PreviewError = "Table has no Iceberg metadata."
		return data, nil
	}
	var full icebergFullMetadata
	if err := json.Unmarshal(resp.Metadata.FullMetadata, &full); err != nil {
		data.PreviewError = fmt.Sprintf("Failed to parse Iceberg metadata: %v", err)
		return data, nil
	}
	data.Snapshots = snapshotsFromFullMetadata(full.Snapshots)
	data.CurrentSnapshotID = full.CurrentSnapshotID

	snap := selectPreviewSnapshot(full, snapshotID)
	if snap == nil {
		if snapshotID != 0 {
			data.PreviewError = fmt.Sprintf("Snapshot %d not found.", snapshotID)
		} else {
			data.PreviewError = "Table has no snapshots yet."
		}
		return data, nil
	}
	data.SnapshotID = snap.SnapshotID
	if snap.TimestampMs > 0 {
		data.SnapshotTime = time.Unix(0, snap.TimestampMs*int64(time.Millisecond))
	}
	if snap.ManifestList == "" {
		data.PreviewError = "Snapshot has no manifest list."
		return data, nil
	}

	dataFiles, hasDeletes, notes, err := s.listIcebergDataFiles(ctx, catalogName, namespace, tableName, snap.ManifestList)
	if err != nil {
		data.PreviewError = fmt.Sprintf("Failed to read snapshot manifests: %v", err)
		return data, nil
	}
	data.HasDeletes = hasDeletes
	data.PreviewNotes = notes
	data.TotalDataFiles = len(dataFiles)
	for _, f := range dataFiles {
		data.TotalRecords += f.RecordCount
	}
	data.DataFiles = dataFiles
	if len(dataFiles) > icebergPreviewMaxListed {
		data.DataFiles = dataFiles[:icebergPreviewMaxListed]
		data.PreviewNotes = append(data.PreviewNotes, fmt.Sprintf("Listing first %d of %d data files.", icebergPreviewMaxListed, len(dataFiles)))
	}

	toScan := dataFiles
	if selectedFile != "" {
		toScan = nil
		for _, f := range dataFiles {
			if f.Path == selectedFile {
				toScan = []IcebergDataFileInfo{f}
				break
			}
		}
		if toScan == nil {
			data.PreviewError = "Requested data file is not part of this snapshot."
			return data, nil
		}
	}
	if len(toScan) == 0 {
		data.PreviewNotes = append(data.PreviewNotes, "Snapshot contains no data files.")
		return data, nil
	}

	for _, f := range toScan {
		if len(data.Rows) >= rowLimit || data.ScannedFiles >= icebergPreviewMaxFiles {
			break
		}
		if !strings.EqualFold(f.Format, string(iceberg.ParquetFile)) {
			data.PreviewNotes = append(data.PreviewNotes, fmt.Sprintf("Skipped %s: %s preview is not supported.", path.Base(f.Path), f.Format))
			continue
		}
		filerPath, err := icebergLocationToFilerPath(f.Path, catalogName, namespace, tableName)
		if err != nil {
			data.PreviewNotes = append(data.PreviewNotes, fmt.Sprintf("Skipped %s: %v", path.Base(f.Path), err))
			continue
		}
		cols, rows, err := s.readParquetRows(ctx, filerPath, rowLimit-len(data.Rows))
		if err != nil {
			data.PreviewNotes = append(data.PreviewNotes, fmt.Sprintf("Failed to read %s: %v", path.Base(f.Path), err))
			data.ScannedFiles++
			continue
		}
		if data.Columns == nil {
			data.Columns = cols
		} else if !slices.Equal(data.Columns, cols) {
			data.PreviewNotes = append(data.PreviewNotes, fmt.Sprintf("Skipped %s: column layout differs from the first file.", path.Base(f.Path)))
			continue
		}
		data.Rows = append(data.Rows, rows...)
		data.ScannedFiles++
	}
	if hasDeletes {
		data.PreviewNotes = append(data.PreviewNotes, "Snapshot has row-level delete files; the preview shows raw data-file rows without applying deletes.")
	}
	return data, nil
}

func selectPreviewSnapshot(full icebergFullMetadata, snapshotID int64) *icebergSnapshot {
	if snapshotID != 0 {
		for i := range full.Snapshots {
			if full.Snapshots[i].SnapshotID == snapshotID {
				return &full.Snapshots[i]
			}
		}
		return nil
	}
	return selectSnapshotForMetrics(full)
}

func (s *AdminServer) listIcebergDataFiles(ctx context.Context, bucketName, namespaceDir, tableName, manifestListLocation string) (files []IcebergDataFileInfo, hasDeletes bool, notes []string, err error) {
	listPath, err := icebergLocationToFilerPath(manifestListLocation, bucketName, namespaceDir, tableName)
	if err != nil {
		return nil, false, nil, err
	}
	listBytes, err := s.readFilerFileContent(ctx, listPath)
	if err != nil {
		return nil, false, nil, fmt.Errorf("read manifest list %s: %w", manifestListLocation, err)
	}
	manifests, err := iceberg.ReadManifestList(bytes.NewReader(listBytes))
	if err != nil {
		return nil, false, nil, fmt.Errorf("parse manifest list: %w", err)
	}

	scanned := 0
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			hasDeletes = true
			continue
		}
		if scanned >= icebergPreviewMaxManifests {
			notes = append(notes, fmt.Sprintf("Scanned first %d of %d manifests.", icebergPreviewMaxManifests, len(manifests)))
			break
		}
		scanned++
		mfPath, err := icebergLocationToFilerPath(mf.FilePath(), bucketName, namespaceDir, tableName)
		if err != nil {
			notes = append(notes, fmt.Sprintf("Skipped manifest %s: %v", path.Base(mf.FilePath()), err))
			continue
		}
		mfBytes, err := s.readFilerFileContent(ctx, mfPath)
		if err != nil {
			notes = append(notes, fmt.Sprintf("Failed to read manifest %s: %v", path.Base(mf.FilePath()), err))
			continue
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(mfBytes), true)
		if err != nil {
			notes = append(notes, fmt.Sprintf("Failed to parse manifest %s: %v", path.Base(mf.FilePath()), err))
			continue
		}
		for _, entry := range entries {
			df := entry.DataFile()
			files = append(files, IcebergDataFileInfo{
				Path:        df.FilePath(),
				Format:      string(df.FileFormat()),
				RecordCount: df.Count(),
				SizeBytes:   df.FileSizeBytes(),
			})
		}
	}
	return files, hasDeletes, notes, nil
}

// icebergLocationToFilerPath maps an Iceberg file location (s3://bucket/...,
// an absolute filer path, or a table-relative path like data/x.parquet) to a
// filer path, which must stay under the table-buckets root.
func icebergLocationToFilerPath(location, bucketName, namespaceDir, tableName string) (string, error) {
	p := location
	if idx := strings.Index(p, "://"); idx >= 0 {
		p = path.Join(s3tables.TablesPath, p[idx+3:])
	} else if strings.HasPrefix(p, "/") {
		p = path.Clean(p)
	} else {
		tableDir := path.Join(s3tables.TablesPath, bucketName, namespaceDir, tableName)
		p = path.Join(tableDir, p)
		if !strings.HasPrefix(p, tableDir+"/") {
			return "", fmt.Errorf("location %q resolves outside the table directory", location)
		}
	}
	p = path.Clean(p)
	if !strings.HasPrefix(p, s3tables.TablesPath+"/") {
		return "", fmt.Errorf("location %q resolves outside %s", location, s3tables.TablesPath)
	}
	return p, nil
}

func (s *AdminServer) lookupFilerEntry(ctx context.Context, fullPath string) (*filer_pb.Entry, error) {
	dir, name := path.Dir(fullPath), path.Base(fullPath)
	var entry *filer_pb.Entry
	err := s.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		entry = resp.Entry
		return nil
	})
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("not found: %s", fullPath)
	}
	return entry, nil
}

func (s *AdminServer) readFilerFileContent(ctx context.Context, fullPath string) ([]byte, error) {
	entry, err := s.lookupFilerEntry(ctx, fullPath)
	if err != nil {
		return nil, err
	}
	if len(entry.Content) > 0 || len(entry.GetChunks()) == 0 {
		return entry.Content, nil
	}
	size := int64(filer.FileSize(entry))
	if size > icebergPreviewMaxMetaBytes {
		return nil, fmt.Errorf("file too large to load (%d bytes)", size)
	}
	streamFn, err := filer.PrepareStreamContentWithThrottler(ctx, s.GetMasterClient(), VolumeServerReadJwt, entry.GetChunks(), 0, size, 0)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *AdminServer) readParquetRows(ctx context.Context, fullPath string, want int) ([]string, [][]string, error) {
	entry, err := s.lookupFilerEntry(ctx, fullPath)
	if err != nil {
		return nil, nil, err
	}
	var readerAt io.ReaderAt
	var size int64
	if len(entry.Content) > 0 || len(entry.GetChunks()) == 0 {
		readerAt = bytes.NewReader(entry.Content)
		size = int64(len(entry.Content))
	} else {
		size = int64(filer.FileSize(entry))
		readerAt = &filerChunkReaderAt{ctx: ctx, server: s, chunks: entry.GetChunks(), size: size}
	}

	pf, err := parquet.OpenFile(readerAt, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, nil, fmt.Errorf("open parquet: %w", err)
	}
	leafColumns := pf.Schema().Columns()
	cols := make([]string, len(leafColumns))
	for i, c := range leafColumns {
		cols[i] = strings.Join(c, ".")
	}

	reader := parquet.NewReader(pf)
	defer reader.Close()
	rowBuf := make([]parquet.Row, 32)
	var out [][]string
	for len(out) < want {
		n, readErr := reader.ReadRows(rowBuf)
		for i := 0; i < n && len(out) < want; i++ {
			out = append(out, formatParquetRow(rowBuf[i], len(cols)))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, nil, readErr
		}
		if n == 0 {
			break
		}
	}
	return cols, out, nil
}

func formatParquetRow(row parquet.Row, columnCount int) []string {
	cells := make([]string, columnCount)
	seen := make([]bool, columnCount)
	for _, v := range row {
		ci := v.Column()
		if ci < 0 || ci >= columnCount {
			continue
		}
		cell := formatParquetValue(v)
		if seen[ci] {
			cells[ci] += ", " + cell
		} else {
			cells[ci] = cell
			seen[ci] = true
		}
	}
	return cells
}

func formatParquetValue(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if utf8.Valid(b) {
			return truncateCell(string(b))
		}
		return truncateCell(fmt.Sprintf("0x%x", b))
	default:
		return truncateCell(v.String())
	}
}

func truncateCell(s string) string {
	if len(s) <= icebergPreviewMaxCellChars {
		return s
	}
	cut := icebergPreviewMaxCellChars
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// filerChunkReaderAt serves random-access reads over a chunked filer entry by
// issuing ranged volume-server reads, carrying the read JWT when configured.
type filerChunkReaderAt struct {
	ctx    context.Context
	server *AdminServer
	chunks []*filer_pb.FileChunk
	size   int64
}

func (r *filerChunkReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset: %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	want := int64(len(p))
	if off+want > r.size {
		want = r.size - off
	}
	streamFn, err := filer.PrepareStreamContentWithThrottler(r.ctx, r.server.GetMasterClient(), VolumeServerReadJwt, r.chunks, off, want, 0)
	if err != nil {
		return 0, err
	}
	// Write straight into p; a bytes.Buffer wrapping p would silently
	// allocate a fresh backing array if it ever grew, dropping bytes the
	// caller expects in p.
	w := &sliceWriter{dst: p}
	if err := streamFn(w); err != nil {
		return w.n, err
	}
	if w.n < len(p) {
		return w.n, io.EOF
	}
	return w.n, nil
}

// sliceWriter writes into a fixed destination slice, discarding any overflow.
type sliceWriter struct {
	dst []byte
	n   int
}

func (w *sliceWriter) Write(p []byte) (int, error) {
	c := copy(w.dst[w.n:], p)
	w.n += c
	return c, nil
}
