package iceberg

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// Fixtures for v3 tables with a geometry column: Parquet files with field
// ids, ZSTD and a native GEOMETRY column, and manifests that carry the v3
// geometry bounds of each file, as DuckDB writes them.

const (
	geoTestIDField   = 1
	geoTestGeomField = 2
)

func wkbPoint(x, y float64) []byte {
	b := make([]byte, 21)
	b[0] = 1 // little endian
	binary.LittleEndian.PutUint32(b[1:], 1)
	binary.LittleEndian.PutUint64(b[5:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[13:], math.Float64bits(y))
	return b
}

// wkbBox is the polygon of one closed ring around the box.
func wkbBox(xmin, ymin, xmax, ymax float64) []byte {
	ring := [][2]float64{{xmin, ymin}, {xmax, ymin}, {xmax, ymax}, {xmin, ymax}, {xmin, ymin}}
	b := make([]byte, 0, 13+16*len(ring))
	b = append(b, 1)
	b = binary.LittleEndian.AppendUint32(b, 3)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(ring)))
	for _, p := range ring {
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(p[0]))
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(p[1]))
	}
	return b
}

func geoLE64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func xyBound(x, y float64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[8:], math.Float64bits(y))
	return b
}

func decodeXYBound(t *testing.T, b []byte) (float64, float64) {
	t.Helper()
	if len(b) < 16 {
		t.Fatalf("geometry bound of %d bytes, want 16", len(b))
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), math.Float64frombits(binary.LittleEndian.Uint64(b[8:]))
}

type geoRow struct {
	ID   int64
	Geom []byte // nil: a null geometry
}

type geoFile struct {
	Name          string
	Rows          []geoRow
	ParquetCRSSet bool // declare the default CRS in the Parquet logical type
}

type geoTableOptions struct {
	icebergType iceberg.Type     // the geometry column's Iceberg type (default: geometry with the default CRS)
	parquetCRS  string           // the CRS of the Parquet GEOMETRY type
	sortOrder   *table.SortOrder // nil: unsorted
	properties  iceberg.Properties
	// fixup, when set, rewrites each input file after it is written
	fixup func(t *testing.T, data []byte) []byte
}

func geoTestSchema(geomType iceberg.Type) *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: geoTestIDField, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: geoTestGeomField, Type: geomType, Name: "geom", Required: false},
	)
}

func writeGeoParquet(t *testing.T, rows []geoRow, crs string) []byte {
	t.Helper()
	schema := parquet.NewSchema("table", parquet.Group{
		"id":   parquet.FieldID(parquet.Compressed(parquet.Int(64), &zstd.Codec{}), geoTestIDField),
		"geom": parquet.FieldID(parquet.Compressed(parquet.Optional(parquet.Geometry(crs)), &zstd.Codec{}), geoTestGeomField),
	})
	geomCol, idCol := -1, -1
	for i, p := range schema.Columns() {
		switch strings.Join(p, ".") {
		case "geom":
			geomCol = i
		case "id":
			idCol = i
		}
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[any](&buf, schema)
	for _, r := range rows {
		row := make(parquet.Row, 2)
		if r.Geom == nil {
			row[geomCol] = parquet.NullValue().Level(0, 0, geomCol)
		} else {
			row[geomCol] = parquet.ByteArrayValue(r.Geom).Level(0, 1, geomCol)
		}
		row[idCol] = parquet.Int64Value(r.ID).Level(0, 0, idCol)
		if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return buf.Bytes()
}

// footerGeoBox is the union of the file's row-group geometry boxes.
func footerGeoBox(t *testing.T, data []byte) (xmin, ymin, xmax, ymax float64, ok bool) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	xmin, ymin, xmax, ymax = math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	for _, rg := range f.Metadata().RowGroups {
		for _, c := range rg.Columns {
			if strings.Join(c.MetaData.PathInSchema, ".") != "geom" || len(c.MetaData.GeospatialStatistics.GeoSpatialTypes) == 0 {
				continue
			}
			bb := c.MetaData.GeospatialStatistics.BBox
			xmin, ymin = math.Min(xmin, bb.XMin), math.Min(ymin, bb.YMin)
			xmax, ymax = math.Max(xmax, bb.XMax), math.Max(ymax, bb.YMax)
			ok = true
		}
	}
	return
}

// footerGeometryCRS is the CRS the file's GEOMETRY logical type declares, and
// whether the column is GEOMETRY at all.
func footerGeometryCRS(t *testing.T, data []byte, column string) (string, bool) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, el := range f.Metadata().Schema {
		if el.Name != column {
			continue
		}
		if g, ok := el.LogicalType.Value.(*format.GeometryType); ok {
			return g.CRS, true
		}
		return "", false
	}
	t.Fatalf("no column %s in the footer", column)
	return "", false
}

// populateGeoTable writes a v3 geometry table with one append snapshot and
// one manifest per file.
func populateGeoTable(t *testing.T, fs *fakeFilerServer, setup tableSetup, files []geoFile, opt geoTableOptions) table.Metadata {
	t.Helper()
	geomType := opt.icebergType
	if geomType == nil {
		geomType = iceberg.GeometryType{}
	}
	schema := geoTestSchema(geomType)
	spec := *iceberg.UnpartitionedSpec
	location := "s3://" + setup.BucketName + "/" + setup.dataPath()
	props := iceberg.Properties{"format-version": "3"}
	for k, v := range opt.properties {
		props[k] = v
	}
	sortOrder := table.UnsortedSortOrder
	if opt.sortOrder != nil {
		sortOrder = *opt.sortOrder
	}
	meta, err := table.NewMetadata(schema, &spec, sortOrder, location, props)
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}
	bucketPath := path.Join(s3tables.TablesPath, setup.BucketName)
	tableFilerPath := path.Join(bucketPath, setup.dataPath())
	metaDir := path.Join(tableFilerPath, "metadata")
	dataDir := path.Join(tableFilerPath, "data")
	putFile := func(dir, name string, data []byte) {
		fs.putEntry(dir, name, &filer_pb.Entry{Name: name, Content: data,
			Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix(), FileSize: uint64(len(data))}})
	}
	snapID := int64(1)
	var manifests []iceberg.ManifestFile
	for i, f := range files {
		crs := opt.parquetCRS
		if f.ParquetCRSSet && crs == "" {
			crs = iceberg.DefaultGeoCRS
		}
		data := writeGeoParquet(t, f.Rows, crs)
		if opt.fixup != nil && !f.ParquetCRSSet {
			data = opt.fixup(t, data)
		}
		putFile(dataDir, f.Name, data)
		dfb, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, setup.fileRef("data", f.Name), iceberg.ParquetFile, map[int]any{}, nil, nil, int64(len(f.Rows)), int64(len(data)))
		if err != nil {
			t.Fatalf("data file %s: %v", f.Name, err)
		}
		lo, hi := int64(math.MaxInt64), int64(math.MinInt64)
		for _, r := range f.Rows {
			lo, hi = min(lo, r.ID), max(hi, r.ID)
		}
		lower := map[int][]byte{geoTestIDField: geoLE64(lo)}
		upper := map[int][]byte{geoTestIDField: geoLE64(hi)}
		if xmin, ymin, xmax, ymax, ok := footerGeoBox(t, data); ok {
			lower[geoTestGeomField], upper[geoTestGeomField] = xyBound(xmin, ymin), xyBound(xmax, ymax)
		}
		dfb.LowerBoundValues(lower)
		dfb.UpperBoundValues(upper)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, dfb.Build())
		name := fmt.Sprintf("data-manifest-%d.avro", i)
		var buf bytes.Buffer
		mf, err := iceberg.WriteManifest(setup.fileRef("metadata", name), &buf, 3, spec, schema, snapID, []iceberg.ManifestEntry{entry})
		if err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		putFile(metaDir, name, buf.Bytes())
		manifests = append(manifests, mf)
	}
	var mlBuf bytes.Buffer
	mlw, err := iceberg.NewManifestListWriterV3(&mlBuf, snapID, 1, 0, nil)
	if err != nil {
		t.Fatalf("manifest list writer: %v", err)
	}
	if err := mlw.AddManifests(manifests); err != nil {
		t.Fatalf("add manifests: %v", err)
	}
	if err := mlw.Close(); err != nil {
		t.Fatalf("close manifest list: %v", err)
	}
	putFile(metaDir, "snap-1.avro", mlBuf.Bytes())
	firstRowID := int64(0)
	addedRows := *mlw.NextRowID()
	snap := table.Snapshot{SnapshotID: snapID, SequenceNumber: 1, TimestampMs: time.Now().UnixMilli(),
		ManifestList: setup.fileRef("metadata", "snap-1.avro"),
		Summary:      &table.Summary{Operation: table.OpAppend, Properties: map[string]string{}},
		FirstRowID:   &firstRowID, AddedRows: &addedRows}
	builder, err := table.MetadataBuilderFromBase(meta, location)
	if err != nil {
		t.Fatalf("metadata builder: %v", err)
	}
	if err := builder.AddSnapshot(&snap); err != nil {
		t.Fatalf("add snapshot: %v", err)
	}
	if err := builder.SetSnapshotRef(table.MainBranch, snapID, table.BranchRef); err != nil {
		t.Fatalf("set ref: %v", err)
	}
	if meta, err = builder.Build(); err != nil {
		t.Fatalf("build metadata: %v", err)
	}
	full, _ := json.Marshal(meta)
	xattr, _ := json.Marshal(map[string]interface{}{
		"metadataVersion":  1,
		"metadataLocation": setup.fileRef("metadata", "v1.metadata.json"),
		"metadata":         map[string]interface{}{"fullMetadata": json.RawMessage(full)},
	})
	fs.putEntry(s3tables.TablesPath, setup.BucketName, &filer_pb.Entry{
		Name: setup.BucketName, IsDirectory: true, Extended: map[string][]byte{s3tables.ExtendedKeyTableBucket: []byte("true")},
	})
	fs.putEntry(bucketPath, setup.Namespace, &filer_pb.Entry{Name: setup.Namespace, IsDirectory: true})
	fs.putEntry(path.Join(bucketPath, setup.Namespace), setup.TableName, &filer_pb.Entry{
		Name: setup.TableName, IsDirectory: true, Extended: map[string][]byte{s3tables.ExtendedKeyMetadata: xattr},
	})
	return meta
}

func compactGeo(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup, config Config) string {
	t.Helper()
	if config.TargetFileSizeBytes == 0 {
		config.TargetFileSizeBytes = 256 * 1024 * 1024
	}
	if config.MinInputFiles == 0 {
		config.MinInputFiles = 2
	}
	if config.MaxCommitRetries == 0 {
		config.MaxCommitRetries = 3
	}
	config.ApplyDeletes = true
	result, _, err := NewHandler(nil).compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	return result
}

// compactedFiles is every compact-*.parquet in the table's data directory.
func compactedFiles(t *testing.T, fs *fakeFilerServer, setup tableSetup) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	for _, e := range fs.listDir(path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")) {
		if strings.HasPrefix(e.Name, "compact-") {
			out[e.Name] = e.Content
		}
	}
	if len(out) == 0 {
		t.Fatal("no compacted file")
	}
	return out
}

// currentDataEntries is every entry of the current snapshot's data manifests.
func currentDataEntries(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup) []iceberg.ManifestEntry {
	t.Helper()
	ctx := context.Background()
	state, err := loadCurrentMetadata(ctx, client, setup.BucketName, setup.tablePath())
	if err != nil {
		t.Fatalf("loadCurrentMetadata: %v", err)
	}
	manifests, err := loadCurrentManifests(ctx, client, setup.BucketName, state.DataPath, state.Metadata)
	if err != nil {
		t.Fatalf("loadCurrentManifests: %v", err)
	}
	var out []iceberg.ManifestEntry
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		data, err := loadFileByIcebergPath(ctx, client, setup.BucketName, state.DataPath, mf.FilePath())
		if err != nil {
			t.Fatalf("load manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(data), true)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		out = append(out, entries...)
	}
	return out
}
