package iceberg

import (
	"bytes"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// boundTruncateLength is the default metrics mode of the Iceberg reference
// implementation, truncate(16), applied to string and binary bounds.
const boundTruncateLength = 16

// columnStats is what a data file's manifest entry records about its columns,
// keyed by Iceberg field id. Readers plan scans from it.
type columnStats struct {
	sizes, values, nulls map[int]int64
	lower, upper         map[int][]byte
	splitOffsets         []int64
}

func (s *columnStats) applyTo(b *iceberg.DataFileBuilder) {
	b.ColumnSizes(s.sizes).ValueCounts(s.values).NullValueCounts(s.nulls).
		LowerBoundValues(s.lower).UpperBoundValues(s.upper).SplitOffsets(s.splitOffsets)
}

// collectColumnStats reads back the footer of a Parquet file the compactor
// wrote. Counts and sizes are summed over the row groups and the bounds are
// the minimum and maximum of the row group statistics, encoded with the
// spec's single-value serialization. A column whose bounds cannot be read
// for every row group, or converted exactly, gets no bounds.
func collectColumnStats(data []byte, schema *iceberg.Schema) (*columnStats, error) {
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)),
		parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, err
	}
	var leaves []*parquet.Column
	var walk func(*parquet.Column)
	walk = func(c *parquet.Column) {
		if c.Leaf() {
			leaves = append(leaves, c)
			return
		}
		for _, child := range c.Columns() {
			walk(child)
		}
	}
	walk(file.Root())

	type bounds struct {
		field    iceberg.NestedField
		typ      parquet.Type
		min, max parquet.Value
		seen     bool
		invalid  bool
	}
	stats := &columnStats{sizes: map[int]int64{}, values: map[int]int64{}, nulls: map[int]int64{},
		lower: map[int][]byte{}, upper: map[int][]byte{}}
	agg := map[int]*bounds{}
	geoAgg := map[int]*geometryBounds{}
	meta := file.Metadata()
	for rgIdx, rg := range file.RowGroups() {
		stats.splitOffsets = append(stats.splitOffsets, rowGroupOffset(&meta.RowGroups[rgIdx]))
		for col, cc := range rg.ColumnChunks() {
			field, ok := icebergFieldOf(leaves[col], schema)
			if !ok {
				continue
			}
			stats.sizes[field.ID] += meta.RowGroups[rgIdx].Columns[col].MetaData.TotalCompressedSize
			stats.values[field.ID] += cc.NumValues()
			b := agg[field.ID]
			if b == nil {
				b = &bounds{field: field, typ: leaves[col].Type()}
				agg[field.ID] = b
			}
			chunk, ok := cc.(*parquet.FileColumnChunk)
			if !ok {
				b.invalid = true
				geoAgg[field.ID] = &geometryBounds{invalid: true}
				continue
			}
			stats.nulls[field.ID] += chunk.NullCount()
			switch field.Type.(type) {
			case iceberg.GeometryType:
				// the bound comes from the row group's geospatial statistics
				g := geoAgg[field.ID]
				if g == nil {
					g = &geometryBounds{}
					geoAgg[field.ID] = g
				}
				g.add(meta.RowGroups[rgIdx].Columns[col].MetaData.GeospatialStatistics, chunk.NumValues(), chunk.NullCount())
				continue
			case iceberg.GeographyType:
				// no bound: edges are geodesic and a box may cross the antimeridian
				continue
			}
			if chunk.NullCount() == chunk.NumValues() {
				continue // an all-null chunk constrains nothing
			}
			lo, hi, ok := chunk.Bounds()
			switch {
			case !ok:
				b.invalid = true
			case !b.seen:
				b.min, b.max, b.seen = lo.Clone(), hi.Clone(), true
			default:
				if b.typ.Compare(lo, b.min) < 0 {
					b.min = lo.Clone()
				}
				if b.typ.Compare(hi, b.max) > 0 {
					b.max = hi.Clone()
				}
			}
		}
	}

	for id, b := range agg {
		if b.invalid || !b.seen {
			continue
		}
		lo, okLo := boundLiteral(b.min, b.typ, b.field.Type, false)
		hi, okHi := boundLiteral(b.max, b.typ, b.field.Type, true)
		if !okLo || !okHi {
			continue
		}
		lo, hi, okHi = truncateBounds(lo, hi)
		if raw, err := lo.MarshalBinary(); err == nil {
			stats.lower[id] = raw
		}
		if raw, err := hi.MarshalBinary(); err == nil && okHi {
			stats.upper[id] = raw
		}
	}
	for id, g := range geoAgg {
		if lower, upper, ok := g.encoded(); ok {
			stats.lower[id], stats.upper[id] = lower, upper
		}
	}
	sort.Slice(stats.splitOffsets, func(i, j int) bool { return stats.splitOffsets[i] < stats.splitOffsets[j] })
	return stats, nil
}

// icebergFieldOf finds the Iceberg field of a Parquet leaf column by the field
// id the writer stored, or by its dotted path in a file written without ids.
// Only primitive fields outside lists and maps carry statistics.
func icebergFieldOf(leaf *parquet.Column, schema *iceberg.Schema) (iceberg.NestedField, bool) {
	var field iceberg.NestedField
	var ok bool
	if leaf.MaxRepetitionLevel() > 0 {
		return field, false
	}
	if id := leaf.ID(); id > 0 {
		field, ok = schema.FindFieldByID(id)
	} else {
		field, ok = schema.FindFieldByName(strings.Join(leaf.Path(), "."))
	}
	if !ok {
		return field, false
	}
	_, primitive := field.Type.(iceberg.PrimitiveType)
	return field, primitive
}

// rowGroupOffset is where a row group's bytes start: the split offset readers
// plan one task per row group from.
func rowGroupOffset(rg *format.RowGroup) int64 {
	if rg.FileOffset > 0 || len(rg.Columns) == 0 {
		return rg.FileOffset
	}
	first := rg.Columns[0].MetaData
	if first.DictionaryPageOffset > 0 && first.DictionaryPageOffset < first.DataPageOffset {
		return first.DictionaryPageOffset
	}
	return first.DataPageOffset
}

// boundLiteral converts a Parquet statistics value into a literal of the
// column's Iceberg type, when the conversion is exact or rounds towards the
// safe side of the bound: roundUp is true for an upper bound, so a time value
// rescaled to a coarser unit stays a bound. Values a reader could not order the
// same way as the writer (NaN, invalid UTF-8, a decimal at another scale or
// stored as bytes, an unknown time unit) get no bound.
func boundLiteral(v parquet.Value, pt parquet.Type, t iceberg.Type, roundUp bool) (iceberg.Literal, bool) {
	if v.IsNull() {
		return nil, false
	}
	switch t := t.(type) {
	case iceberg.BooleanType:
		return iceberg.BoolLiteral(v.Boolean()), v.Kind() == parquet.Boolean
	case iceberg.Int32Type:
		return iceberg.Int32Literal(v.Int32()), v.Kind() == parquet.Int32
	case iceberg.Int64Type:
		switch v.Kind() {
		case parquet.Int64:
			return iceberg.Int64Literal(v.Int64()), true
		case parquet.Int32:
			return iceberg.Int64Literal(int64(v.Int32())), true
		}
	case iceberg.Float32Type:
		f := v.Float()
		return iceberg.Float32Literal(f), v.Kind() == parquet.Float && !math.IsNaN(float64(f))
	case iceberg.Float64Type:
		switch v.Kind() {
		case parquet.Double:
			f := v.Double()
			return iceberg.Float64Literal(f), !math.IsNaN(f)
		case parquet.Float:
			f := v.Float()
			return iceberg.Float64Literal(float64(f)), !math.IsNaN(float64(f))
		}
	case iceberg.DateType:
		return iceberg.DateLiteral(iceberg.Date(v.Int32())), v.Kind() == parquet.Int32
	case iceberg.TimeType:
		var i64 int64
		switch v.Kind() {
		case parquet.Int64:
			i64 = v.Int64()
		case parquet.Int32: // TIME(MILLIS) is stored as INT32
			i64 = int64(v.Int32())
		default:
			return nil, false
		}
		if us, ok := convertTimeUnit(i64, pt, time.Microsecond, roundUp); ok {
			return iceberg.TimeLiteral(iceberg.Time(us)), true
		}
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if v.Kind() == parquet.Int64 {
			if us, ok := convertTimeUnit(v.Int64(), pt, time.Microsecond, roundUp); ok {
				return iceberg.TimestampLiteral(iceberg.Timestamp(us)), true
			}
		}
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		if v.Kind() == parquet.Int64 {
			if ns, ok := convertTimeUnit(v.Int64(), pt, time.Nanosecond, roundUp); ok {
				return iceberg.TimestampNsLiteral(iceberg.TimestampNano(ns)), true
			}
		}
	case iceberg.StringType:
		b := v.ByteArray()
		return iceberg.StringLiteral(string(b)), v.Kind() == parquet.ByteArray && utf8.Valid(b)
	case iceberg.BinaryType:
		return iceberg.BinaryLiteral(bytes.Clone(v.ByteArray())), v.Kind() == parquet.ByteArray
	case iceberg.FixedType:
		if v.Kind() == parquet.FixedLenByteArray && len(v.ByteArray()) == t.Len() {
			return iceberg.FixedLiteral(bytes.Clone(v.ByteArray())), true
		}
	case iceberg.UUIDType:
		if v.Kind() == parquet.FixedLenByteArray && len(v.ByteArray()) == 16 {
			var u uuid.UUID
			copy(u[:], v.ByteArray())
			return iceberg.UUIDLiteral(u), true
		}
	case iceberg.DecimalType:
		// Only the integer encodings (precision up to 18, what DuckDB writes for
		// DECIMAL(18,s)): the statistics of a byte-array decimal are compared
		// as bytes, which does not order negative two's-complement values.
		if scale, ok := parquetDecimalScale(pt); !ok || scale != t.Scale() {
			return nil, false
		}
		switch v.Kind() {
		case parquet.Int32:
			return iceberg.DecimalLiteral(iceberg.Decimal{Val: decimal128.FromI64(int64(v.Int32())), Scale: t.Scale()}), true
		case parquet.Int64:
			return iceberg.DecimalLiteral(iceberg.Decimal{Val: decimal128.FromI64(v.Int64()), Scale: t.Scale()}), true
		}
	}
	return nil, false
}

// convertTimeUnit rescales a time value from the Parquet column's unit to the
// unit the Iceberg type stores, rounding towards the safe side of a bound.
func convertTimeUnit(v int64, pt parquet.Type, target time.Duration, roundUp bool) (int64, bool) {
	unit := timeUnit(pt)
	if unit == 0 {
		return 0, false
	}
	if unit == target {
		return v, true
	}
	if unit > target {
		factor := int64(unit / target)
		if v > math.MaxInt64/factor || v < math.MinInt64/factor {
			return 0, false
		}
		return v * factor, true
	}
	factor := int64(target / unit)
	q := v / factor
	if roundUp && v%factor != 0 && v > 0 {
		q++
	}
	if !roundUp && v%factor != 0 && v < 0 {
		q--
	}
	return q, true
}

func timeUnit(pt parquet.Type) time.Duration {
	if pt == nil || pt.LogicalType() == nil {
		return 0
	}
	switch lt := pt.LogicalType().Value.(type) {
	case *format.TimestampType:
		if lt.Unit.Value != nil {
			return lt.Unit.Value.Duration()
		}
	case *format.TimeType:
		if lt.Unit.Value != nil {
			return lt.Unit.Value.Duration()
		}
	}
	return 0
}

func parquetDecimalScale(pt parquet.Type) (int, bool) {
	if pt == nil || pt.LogicalType() == nil {
		return 0, false
	}
	if d, ok := pt.LogicalType().Value.(*format.DecimalType); ok {
		return int(d.Scale), true
	}
	return 0, false
}

// truncateBounds applies truncate(16) to string and binary bounds. A prefix is
// still a lower bound; a shortened upper bound is not, so a value longer than
// the limit keeps no upper bound (the reference implementation increments the
// last character instead).
func truncateBounds(lo, hi iceberg.Literal) (iceberg.Literal, iceberg.Literal, bool) {
	switch l := lo.(type) {
	case iceberg.StringLiteral:
		if r := []rune(string(l)); len(r) > boundTruncateLength {
			lo = iceberg.StringLiteral(string(r[:boundTruncateLength]))
		}
		return lo, hi, utf8.RuneCountInString(string(hi.(iceberg.StringLiteral))) <= boundTruncateLength
	case iceberg.BinaryLiteral:
		if len(l) > boundTruncateLength {
			lo = l[:boundTruncateLength]
		}
		return lo, hi, len(hi.(iceberg.BinaryLiteral)) <= boundTruncateLength
	}
	return lo, hi, true
}
