package iceberg

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go/format"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type geoBox struct{ xmin, ymin, xmax, ymax float64 }

func (b geoBox) String() string {
	return fmt.Sprintf("(%g %g, %g %g)", b.xmin, b.ymin, b.xmax, b.ymax)
}

func (b geoBox) covers(o geoBox) bool {
	return b.xmin <= o.xmin && b.ymin <= o.ymin && b.xmax >= o.xmax && b.ymax >= o.ymax
}

// compactedGeoBounds is the geometry bound of each compacted file in the
// current manifests (nil: none), and whether its id column has a bound, the
// control that statistics were collected at all.
func compactedGeoBounds(t *testing.T, client filer_pb.SeaweedFilerClient, setup tableSetup) (map[string]*geoBox, map[string]bool) {
	t.Helper()
	geo, idBounded := map[string]*geoBox{}, map[string]bool{}
	for _, e := range currentDataEntries(t, client, setup) {
		df := e.DataFile()
		if !strings.Contains(df.FilePath(), "/compact-") {
			continue
		}
		idBounded[df.FilePath()] = df.LowerBoundValues()[geoTestIDField] != nil
		lo, hi := df.LowerBoundValues()[geoTestGeomField], df.UpperBoundValues()[geoTestGeomField]
		if lo == nil && hi == nil {
			geo[df.FilePath()] = nil
			continue
		}
		x0, y0 := decodeXYBound(t, lo)
		x1, y1 := decodeXYBound(t, hi)
		geo[df.FilePath()] = &geoBox{x0, y0, x1, y1}
	}
	if len(geo) == 0 {
		t.Fatal("no compacted file in the manifests")
	}
	return geo, idBounded
}

// mixedGeoFiles holds points and polygons far apart, so a bound that misses
// any of them shows.
func mixedGeoFiles() ([]geoFile, geoBox) {
	files := []geoFile{
		{Name: "pts.parquet", Rows: []geoRow{{1, wkbPoint(-3.70, 40.41)}, {2, wkbPoint(2.17, 41.38)}, {3, wkbPoint(-5.98, 37.38)}}},
		{Name: "boxes.parquet", Rows: []geoRow{{4, wkbBox(-9.3, 42.1, -8.5, 43.4)}, {5, wkbBox(1.0, 36.0, 4.3, 39.9)}}},
		{Name: "nulls.parquet", Rows: []geoRow{{6, nil}, {7, nil}}},
		{Name: "mixed.parquet", Rows: []geoRow{{8, nil}, {9, wkbPoint(-1.10, 40.34)}, {10, wkbBox(-0.5, 38.9, -0.3, 39.5)}}},
	}
	return files, geoBox{-9.3, 36.0, 4.3, 43.4}
}

func TestCompactRecordsGeometryBounds(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
	files, want := mixedGeoFiles()
	populateGeoTable(t, fs, setup, files, geoTableOptions{})
	if result := compactGeo(t, client, setup, Config{}); !strings.Contains(result, "compacted 4 files") {
		t.Fatalf("result %q", result)
	}
	geo, idBounded := compactedGeoBounds(t, client, setup)
	for p, got := range geo {
		if !idBounded[p] {
			t.Errorf("%s: no id bound: statistics were not collected", p)
		}
		if got == nil {
			t.Fatalf("%s: no geometry bound", p)
		}
		if *got != want {
			t.Errorf("%s: geometry bound %s, want the union of every geometry %s", p, got, want)
		}
	}
}

// A sorted rewrite may write its output without row-group geospatial
// statistics; the file then gets no geometry bound, never one that misses
// a row.
func TestCompactSortedGeometryBoundCoversEveryRow(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
	so, err := table.NewSortOrder(1, []table.SortField{{SourceIDs: []int{geoTestIDField}, Transform: iceberg.IdentityTransform{}, Direction: table.SortDESC, NullOrder: table.NullsFirst}})
	if err != nil {
		t.Fatal(err)
	}
	files, want := mixedGeoFiles()
	populateGeoTable(t, fs, setup, files, geoTableOptions{sortOrder: &so})
	compactGeo(t, client, setup, Config{RewriteStrategy: "sort", SortBufferRows: minSortBufferRows})
	geo, _ := compactedGeoBounds(t, client, setup)
	for p, got := range geo {
		if got != nil && !got.covers(want) {
			t.Errorf("%s: geometry bound %s does not cover every geometry %s", p, got, want)
		}
	}
}

func TestCompactAllNullGeometryGetsNoBound(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
	populateGeoTable(t, fs, setup, []geoFile{
		{Name: "a.parquet", Rows: []geoRow{{1, nil}, {2, nil}}},
		{Name: "b.parquet", Rows: []geoRow{{3, nil}}},
	}, geoTableOptions{})
	compactGeo(t, client, setup, Config{})
	geo, idBounded := compactedGeoBounds(t, client, setup)
	for p, got := range geo {
		if got != nil {
			t.Errorf("%s: geometry bound %s on a column of nulls", p, got)
		}
		if !idBounded[p] {
			t.Errorf("%s: no id bound: statistics were not collected", p)
		}
	}
}

// A value that does not parse as WKB leaves its row group without
// geospatial statistics; the file then gets no geometry bound rather than
// one from the other row groups that does not cover that row group.
func TestCompactUnparseableGeometryGivesNoBound(t *testing.T) {
	fs, client := startFakeFiler(t)
	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "geo"}
	populateGeoTable(t, fs, setup, []geoFile{
		{Name: "ok.parquet", Rows: []geoRow{{1, wkbPoint(10, 10)}, {2, wkbPoint(11, 11)}}},
		{Name: "bad.parquet", Rows: []geoRow{{3, []byte("not wkb at all")}, {4, wkbPoint(-50, -50)}}},
	}, geoTableOptions{})
	// two rows a row group: the valid file's rows, then the other file's
	compactGeo(t, client, setup, Config{RowGroupRowLimit: 2})
	geo, idBounded := compactedGeoBounds(t, client, setup)
	for p, got := range geo {
		if got != nil {
			t.Errorf("%s: geometry bound %s although a value did not parse", p, got)
		}
		if !idBounded[p] {
			t.Errorf("%s: no id bound: statistics were not collected", p)
		}
	}
}

func TestCollectColumnStatsGivesGeographyNoBound(t *testing.T) {
	data := writeGeoParquet(t, []geoRow{{1, wkbPoint(170, 10)}, {2, wkbPoint(-170, 12)}}, "")
	geog, err := iceberg.GeographyTypeOf(iceberg.DefaultGeoCRS, "spherical")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		typ       iceberg.Type
		wantBound bool
	}{{iceberg.GeometryType{}, true}, {geog, false}} {
		stats, err := collectColumnStats(data, geoTestSchema(tc.typ))
		if err != nil {
			t.Fatal(err)
		}
		if _, has := stats.lower[geoTestGeomField]; has != tc.wantBound {
			t.Errorf("%s: lower bound present %v, want %v", tc.typ, has, tc.wantBound)
		}
		if stats.values[geoTestGeomField] != 2 {
			t.Errorf("%s: value count %d, want 2", tc.typ, stats.values[geoTestGeomField])
		}
	}
}

func TestGeometryBoundsAggregation(t *testing.T) {
	stats := func(xmin, ymin, xmax, ymax float64) format.GeospatialStatistics {
		return format.GeospatialStatistics{BBox: format.BoundingBox{XMin: xmin, XMax: xmax, YMin: ymin, YMax: ymax}, GeoSpatialTypes: []int32{1}}
	}
	type chunk struct {
		gs            format.GeospatialStatistics
		values, nulls int64
	}
	for _, tc := range []struct {
		name   string
		chunks []chunk
		want   *geoBox
	}{
		{"one", []chunk{{stats(1, 2, 3, 4), 5, 0}}, &geoBox{1, 2, 3, 4}},
		{"union", []chunk{{stats(1, 2, 3, 4), 5, 0}, {stats(-1, 3, 2, 9), 5, 1}}, &geoBox{-1, 2, 3, 9}},
		{"nulls only constrain nothing", []chunk{{stats(1, 2, 3, 4), 5, 0}, {format.GeospatialStatistics{}, 4, 4}}, &geoBox{1, 2, 3, 4}},
		{"values without statistics", []chunk{{stats(1, 2, 3, 4), 5, 0}, {format.GeospatialStatistics{}, 4, 1}}, nil},
		{"NaN", []chunk{{stats(1, math.NaN(), 3, 4), 5, 0}}, nil},
		{"infinity", []chunk{{stats(1, 2, math.Inf(1), 4), 5, 0}}, nil},
		{"inverted", []chunk{{stats(3, 2, 1, 4), 5, 0}}, nil},
		{"nothing", nil, nil},
	} {
		var g geometryBounds
		for _, c := range tc.chunks {
			g.add(c.gs, c.values, c.nulls)
		}
		lo, hi, ok := g.encoded()
		if tc.want == nil {
			if ok {
				t.Errorf("%s: bound given, want none", tc.name)
			}
			continue
		}
		if !ok {
			t.Errorf("%s: no bound, want %s", tc.name, tc.want)
			continue
		}
		x0, y0 := decodeXYBound(t, lo)
		x1, y1 := decodeXYBound(t, hi)
		if got := (geoBox{x0, y0, x1, y1}); got != *tc.want {
			t.Errorf("%s: bound %s, want %s", tc.name, got, tc.want)
		}
	}
}
