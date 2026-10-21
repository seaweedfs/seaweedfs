package iceberg

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
)

// unsetDefaultCRS makes an input file declare the default CRS as no CRS at
// all, as DuckDB and other writers do; parquet.Geometry("") spells it out.
func unsetDefaultCRS(t *testing.T, data []byte) []byte {
	t.Helper()
	out, changed, err := withDefaultGeometryCRSUnset(data, geoTestSchema(iceberg.GeometryType{}))
	if err != nil || !changed {
		t.Fatalf("unset the input's default CRS: changed %v, %v", changed, err)
	}
	return out
}

func fiveGeoFiles() []geoFile {
	var files []geoFile
	for f := 0; f < 5; f++ {
		var rows []geoRow
		for i := 0; i < 40; i++ {
			id := int64(f*40 + i)
			rows = append(rows, geoRow{ID: id, Geom: wkbPoint(float64(id%20)-10, float64(id/20)-5)})
		}
		files = append(files, geoFile{Name: fmt.Sprintf("g%d.parquet", f), Rows: rows})
	}
	return files
}

// The Parquet CRS of a default-CRS geometry column survives compaction as the
// inputs declared it: none. The control shows what parquet-go does with the
// same schema on its own: it spells the default out.
func TestCompactWritesDefaultCRSGeometryWithoutCRS(t *testing.T) {
	control := unsetDefaultCRS(t, writeGeoParquet(t, []geoRow{{1, wkbPoint(1, 2)}}, ""))
	if crs, ok := footerGeometryCRS(t, control, "geom"); !ok || crs != "" {
		t.Fatalf("setup: an input file declares CRS %q (geometry %v), want none", crs, ok)
	}
	f, err := parquet.OpenFile(bytes.NewReader(control), int64(len(control)))
	if err != nil {
		t.Fatal(err)
	}
	var plain bytes.Buffer
	w := parquet.NewWriter(&plain, f.Schema())
	if _, err := w.WriteRows([]parquet.Row{{parquet.ByteArrayValue(wkbPoint(1, 2)).Level(0, 1, 0), parquet.Int64Value(1).Level(0, 0, 1)}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if crs, _ := footerGeometryCRS(t, plain.Bytes(), "geom"); crs != iceberg.DefaultGeoCRS {
		t.Fatalf("control: parquet-go copied the schema with CRS %q; the fix is for %q", crs, iceberg.DefaultGeoCRS)
	}

	for _, strategy := range []string{"binpack", "sort"} {
		t.Run(strategy, func(t *testing.T) {
			fs, client := startFakeFiler(t)
			setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
			opt := geoTableOptions{fixup: unsetDefaultCRS}
			config := Config{}
			if strategy == "sort" {
				so, err := table.NewSortOrder(1, []table.SortField{{SourceIDs: []int{geoTestIDField}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst}})
				if err != nil {
					t.Fatal(err)
				}
				opt.sortOrder = &so
				config.RewriteStrategy, config.SortBufferRows = "sort", minSortBufferRows
			}
			files := fiveGeoFiles()
			files[2].ParquetCRSSet = true // an input that spells the default out
			populateGeoTable(t, fs, setup, files, opt)
			if result := compactGeo(t, client, setup, config); !strings.Contains(result, "compacted 5 files") {
				t.Fatalf("result %q", result)
			}
			for name, data := range compactedFiles(t, fs, setup) {
				if crs, ok := footerGeometryCRS(t, data, "geom"); !ok || crs != "" {
					t.Errorf("%s: geometry column declares CRS %q (geometry %v), want none", name, crs, ok)
				}
				got, err := parquet.Read[struct {
					ID int64 `parquet:"id"`
				}](bytes.NewReader(data), int64(len(data)))
				if err != nil || len(got) != 200 {
					t.Errorf("%s: the rewritten file reads %d rows, %v", name, len(got), err)
				}
			}
		})
	}
}

// An explicit CRS is not the default: it is written as the input declared it.
func TestCompactKeepsExplicitGeometryCRS(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
	g, err := iceberg.GeometryTypeOf("EPSG:2913")
	if err != nil {
		t.Fatal(err)
	}
	populateGeoTable(t, fs, setup, fiveGeoFiles(), geoTableOptions{icebergType: g, parquetCRS: "EPSG:2913"})
	compactGeo(t, client, setup, Config{})
	for name, data := range compactedFiles(t, fs, setup) {
		if crs, ok := footerGeometryCRS(t, data, "geom"); !ok || crs != "EPSG:2913" {
			t.Errorf("%s: geometry CRS %q (geometry %v), want EPSG:2913", name, crs, ok)
		}
	}
}

// A default-CRS geometry nested in a struct is written without a CRS too.
func TestDefaultCRSUnsetForNestedGeometry(t *testing.T) {
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.FieldID(parquet.Compressed(parquet.Int(64), &zstd.Codec{}), 1),
		"loc": parquet.FieldID(parquet.Group{"geom": parquet.FieldID(parquet.Optional(parquet.Geometry("")), 3)}, 2),
	})
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[any](&buf, schema)
	cols := map[string]int{}
	for i, p := range schema.Columns() {
		cols[strings.Join(p, ".")] = i
	}
	row := make(parquet.Row, 2)
	row[cols["id"]] = parquet.Int64Value(1).Level(0, 0, cols["id"])
	row[cols["loc.geom"]] = parquet.ByteArrayValue(wkbPoint(1, 2)).Level(0, 1, cols["loc.geom"])
	if _, err := w.WriteRows([]parquet.Row{row}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if crs, ok := footerGeometryCRS(t, buf.Bytes(), "geom"); !ok || crs != iceberg.DefaultGeoCRS {
		t.Fatalf("setup: the nested column declares %q (geometry %v); the control is the spelled-out default", crs, ok)
	}
	ice := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Name: "loc", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "geom", Type: iceberg.GeometryType{}}}}})
	out, changed, err := withDefaultGeometryCRSUnset(buf.Bytes(), ice)
	if err != nil || !changed {
		t.Fatalf("changed %v, %v", changed, err)
	}
	if crs, ok := footerGeometryCRS(t, out, "geom"); !ok || crs != "" {
		t.Errorf("the nested geometry declares CRS %q (geometry %v), want none", crs, ok)
	}
	got, err := parquet.Read[struct {
		ID int64 `parquet:"id"`
	}](bytes.NewReader(out), int64(len(out)))
	if err != nil || len(got) != 1 || got[0].ID != 1 {
		t.Errorf("the rewritten file reads %v, %v", got, err)
	}
}

// footerGeometryCRSs is the CRS of every GEOMETRY element in the footer, by
// schema element index and name.
func footerGeometryCRSs(t *testing.T, data []byte) map[string]string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	out := map[string]string{}
	for i, el := range f.Metadata().Schema {
		if g, ok := el.LogicalType.Value.(*format.GeometryType); ok {
			out[fmt.Sprintf("%d:%s", i, el.Name)] = g.CRS
		}
	}
	return out
}

// A file without field ids is matched to the table by name: through the
// LIST and MAP wrapper groups too, which Iceberg's field names leave out.
func TestDefaultCRSUnsetWithoutFieldIDs(t *testing.T) {
	geom := func() parquet.Node { return parquet.Optional(parquet.Geometry("")) }
	element := func(id int, typ iceberg.Type) *iceberg.ListType {
		return &iceberg.ListType{ElementID: id, Element: typ, ElementRequired: false}
	}
	cases := []struct {
		name    string
		parquet parquet.Group
		iceberg []iceberg.NestedField
	}{
		{"top level", parquet.Group{"id": parquet.Int(64), "geom": geom()},
			[]iceberg.NestedField{{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}, {ID: 2, Name: "geom", Type: iceberg.GeometryType{}}}},
		{"list", parquet.Group{"shapes": parquet.Optional(parquet.List(geom()))},
			[]iceberg.NestedField{{ID: 1, Name: "shapes", Type: element(2, iceberg.GeometryType{})}}},
		{"list of structs", parquet.Group{"items": parquet.Optional(parquet.List(parquet.Group{"geom": geom()}))},
			[]iceberg.NestedField{{ID: 1, Name: "items", Type: element(2, &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "geom", Type: iceberg.GeometryType{}}}})}}},
		{"map", parquet.Group{"attrs": parquet.Optional(parquet.Map(parquet.String(), geom()))},
			[]iceberg.NestedField{{ID: 1, Name: "attrs", Type: &iceberg.MapType{KeyID: 2, KeyType: iceberg.PrimitiveTypes.String, ValueID: 3, ValueType: iceberg.GeometryType{}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[any](&buf, parquet.NewSchema("table", tc.parquet))
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			before := footerGeometryCRSs(t, buf.Bytes())
			if len(before) != 1 {
				t.Fatalf("setup: geometry columns %v, want one", before)
			}
			for path, crs := range before {
				if crs != iceberg.DefaultGeoCRS {
					t.Fatalf("setup: %s declares %q; the control is the spelled-out default", path, crs)
				}
			}
			out, changed, err := withDefaultGeometryCRSUnset(buf.Bytes(), iceberg.NewSchema(0, tc.iceberg...))
			if err != nil || !changed {
				t.Fatalf("changed %v, %v", changed, err)
			}
			for path, crs := range footerGeometryCRSs(t, out) {
				if crs != "" {
					t.Errorf("%s declares CRS %q, want none", path, crs)
				}
			}
		})
	}
}

// The footer is replaced inside the file's own buffer: a compacted file can be
// as large as the target file size, and a copy of it would double the memory
// the rewrite needs.
func TestDefaultCRSUnsetReusesBuffer(t *testing.T) {
	data := writeGeoParquet(t, []geoRow{{1, wkbPoint(1, 2)}, {2, wkbPoint(3, 4)}}, "")
	if crs, ok := footerGeometryCRS(t, data, "geom"); !ok || crs != iceberg.DefaultGeoCRS {
		t.Fatalf("setup: CRS %q (geometry %v); the control is the spelled-out default", crs, ok)
	}
	first := &data[0]
	out, changed, err := withDefaultGeometryCRSUnset(data, geoTestSchema(iceberg.GeometryType{}))
	if err != nil || !changed {
		t.Fatalf("changed %v, %v", changed, err)
	}
	if &out[0] != first {
		t.Errorf("the rewrite copied the file into a new buffer")
	}
	if crs, ok := footerGeometryCRS(t, out, "geom"); !ok || crs != "" {
		t.Errorf("geometry declares CRS %q (geometry %v), want none", crs, ok)
	}
	got, err := parquet.Read[struct {
		ID   int64  `parquet:"id"`
		Geom []byte `parquet:"geom,optional"`
	}](bytes.NewReader(out), int64(len(out)))
	if err != nil || len(got) != 2 || got[1].ID != 2 || !bytes.Equal(got[1].Geom, wkbPoint(3, 4)) {
		t.Errorf("the rewritten file reads %v, %v", got, err)
	}
}
