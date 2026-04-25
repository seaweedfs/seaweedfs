// Package footer holds the parsed-footer cache and the parser that
// fills it. Phase 1 of the dev plan: parsed-footer cache + file/range-
// level pushdown. Page-level (ColumnIndex/OffsetIndex) consumption
// lands in M6; for M1 we capture the column-chunk byte ranges that
// file/range-level pushdown needs.
package footer

import (
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// ParsedFooter is the daemon's compact, cache-friendly view of a
// Parquet file's footer. It carries only the fields M1 actually
// consumes; later milestones extend it (ColumnIndex / OffsetIndex,
// statistics, bloom-filter offsets).
type ParsedFooter struct {
	NumRows   int64
	NumCols   int
	NumGroups int

	// ColumnPaths is the dotted path-in-schema for each leaf column,
	// in the canonical order parquet-go assigns to the file.
	ColumnPaths []string

	// RowGroups[i] holds the byte ranges and row count for row group i.
	RowGroups []RowGroupRanges
}

// RowGroupRanges enumerates the column chunks of one row group, in
// the canonical column order.
type RowGroupRanges struct {
	NumRows      int64
	ColumnChunks []ColumnChunkRange
}

// ColumnChunkRange is the byte range to read for one column chunk in
// one row group, including the dictionary page region when present.
//
// Iceberg-style page-level pruning narrows reads further; M1 returns
// the full column-chunk byte range. Including the dictionary-page
// prefix when DictionaryPageOffset > 0 follows the rule from the
// design's Page-Level Index section: the dictionary page is not in
// OffsetIndex but must be fetched whenever any data page is read.
type ColumnChunkRange struct {
	Path          []string
	Offset        int64
	Length        int64
	NumValues     int64
	TotalCompSize int64
}

// ParseFromReader reads only the footer of a Parquet file and
// returns the projection used by file/range-level pushdown.
//
// SkipPageIndex is set: M1 does not need ColumnIndex/OffsetIndex,
// and skipping them avoids two extra reads per parse for files that
// have page indexes written. M6 will turn this back on for the page-
// level pruning milestone.
func ParseFromReader(r io.ReaderAt, size int64) (*ParsedFooter, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}
	if size <= 0 {
		return nil, fmt.Errorf("invalid size %d", size)
	}

	pf, err := parquet.OpenFile(r, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("open parquet footer: %w", err)
	}

	md := pf.Metadata()
	out := &ParsedFooter{
		NumRows:   md.NumRows,
		NumGroups: len(md.RowGroups),
		RowGroups: make([]RowGroupRanges, len(md.RowGroups)),
	}

	// All row groups share the same leaf-column layout; sample row
	// group 0 to get column paths and column count.
	if len(md.RowGroups) > 0 {
		out.NumCols = len(md.RowGroups[0].Columns)
		out.ColumnPaths = make([]string, out.NumCols)
		for j, c := range md.RowGroups[0].Columns {
			out.ColumnPaths[j] = joinPath(c.MetaData.PathInSchema)
		}
	}

	for i, rg := range md.RowGroups {
		out.RowGroups[i].NumRows = rg.NumRows
		out.RowGroups[i].ColumnChunks = make([]ColumnChunkRange, len(rg.Columns))
		for j, c := range rg.Columns {
			start, length := columnChunkBytes(&c)
			out.RowGroups[i].ColumnChunks[j] = ColumnChunkRange{
				Path:          c.MetaData.PathInSchema,
				Offset:        start,
				Length:        length,
				NumValues:     c.MetaData.NumValues,
				TotalCompSize: c.MetaData.TotalCompressedSize,
			}
		}
	}
	return out, nil
}

// columnChunkBytes returns the (offset, length) of the contiguous
// byte region that a reader needs to fetch to consume this column
// chunk: the dictionary page (when present) plus all data pages.
//
// parquet-go exposes DictionaryPageOffset as the start of the
// dictionary page when one exists, otherwise zero, and DataPageOffset
// as the start of the first data page. The end of the chunk is
// `start + TotalCompressedSize`.
func columnChunkBytes(c *format.ColumnChunk) (int64, int64) {
	start := c.MetaData.DataPageOffset
	if dict := c.MetaData.DictionaryPageOffset; dict > 0 && dict < start {
		start = dict
	}
	return start, c.MetaData.TotalCompressedSize
}

func joinPath(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "." + p
	}
	return out
}
