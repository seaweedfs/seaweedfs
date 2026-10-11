package iceberg

import (
	"bytes"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/iceberg-go"
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
				continue
			}
			stats.nulls[field.ID] += chunk.NullCount()
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
		lo, okLo := boundLiteral(b.min, b.typ, b.field.Type)
		hi, okHi := boundLiteral(b.max, b.typ, b.field.Type)
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
// column's Iceberg type, when the conversion is exact. Other types (decimal,
// time, uuid, fixed, timestamps not in the type's unit) get no bound.
func boundLiteral(v parquet.Value, pt parquet.Type, t iceberg.Type) (iceberg.Literal, bool) {
	if v.IsNull() {
		return nil, false
	}
	switch t.(type) {
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
		f := v.Double()
		return iceberg.Float64Literal(f), v.Kind() == parquet.Double && !math.IsNaN(f)
	case iceberg.DateType:
		return iceberg.DateLiteral(iceberg.Date(v.Int32())), v.Kind() == parquet.Int32
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return iceberg.TimestampLiteral(iceberg.Timestamp(v.Int64())), v.Kind() == parquet.Int64 && timeUnit(pt) == time.Microsecond
	case iceberg.TimestampNsType:
		return iceberg.TimestampNsLiteral(iceberg.TimestampNano(v.Int64())), v.Kind() == parquet.Int64 && timeUnit(pt) == time.Nanosecond
	case iceberg.StringType:
		b := v.ByteArray()
		return iceberg.StringLiteral(string(b)), v.Kind() == parquet.ByteArray && utf8.Valid(b)
	case iceberg.BinaryType:
		return iceberg.BinaryLiteral(bytes.Clone(v.ByteArray())), v.Kind() == parquet.ByteArray
	}
	return nil, false
}

func timeUnit(pt parquet.Type) time.Duration {
	if pt == nil || pt.LogicalType() == nil {
		return 0
	}
	if ts, ok := pt.LogicalType().Value.(*format.TimestampType); ok && ts.Unit.Value != nil {
		return ts.Unit.Value.Duration()
	}
	return 0
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
