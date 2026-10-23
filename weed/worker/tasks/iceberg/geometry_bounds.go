package iceberg

import (
	"encoding/binary"
	"math"

	"github.com/parquet-go/parquet-go/format"
)

// geometryBounds aggregates the bound of a geometry column over the row groups
// of a written file. The spec serializes a geometry bound as a point: the
// lower bound is (xmin, ymin), the upper bound (xmax, ymax), each coordinate
// an 8-byte little-endian double; z and m are optional and not written here.
//
// Readers skip a file whose bound does not intersect a spatial filter, so a
// box smaller than the file's geometries returns wrong results. Any doubt
// gives no bound:
//   - the box is the union of the row groups' geospatial statistics, which
//     the Parquet writer computes from every non-null value;
//   - a row group with non-null values but no statistics (the writer omits
//     them when a value does not parse as WKB) gives the column no bound, as
//     does a box that is not finite or is inverted;
//   - a row group of nulls only constrains nothing.
type geometryBounds struct {
	xmin, ymin, xmax, ymax float64
	seen                   bool
	invalid                bool
}

func (g *geometryBounds) add(gs format.GeospatialStatistics, numValues, nullCount int64) {
	if g.invalid || numValues <= 0 || nullCount >= numValues {
		return
	}
	if len(gs.GeoSpatialTypes) == 0 {
		g.invalid = true
		return
	}
	bb := gs.BBox
	for _, v := range []float64{bb.XMin, bb.XMax, bb.YMin, bb.YMax} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			g.invalid = true
			return
		}
	}
	if bb.XMin > bb.XMax || bb.YMin > bb.YMax {
		g.invalid = true
		return
	}
	if !g.seen {
		g.xmin, g.ymin, g.xmax, g.ymax, g.seen = bb.XMin, bb.YMin, bb.XMax, bb.YMax, true
		return
	}
	g.xmin, g.ymin = math.Min(g.xmin, bb.XMin), math.Min(g.ymin, bb.YMin)
	g.xmax, g.ymax = math.Max(g.xmax, bb.XMax), math.Max(g.ymax, bb.YMax)
}

// encoded returns the lower and upper bound, or false when the column gets
// none.
func (g *geometryBounds) encoded() (lower, upper []byte, ok bool) {
	if g == nil || g.invalid || !g.seen {
		return nil, nil, false
	}
	return encodeGeometryPoint(g.xmin, g.ymin), encodeGeometryPoint(g.xmax, g.ymax), true
}

func encodeGeometryPoint(x, y float64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[8:], math.Float64bits(y))
	return b
}
