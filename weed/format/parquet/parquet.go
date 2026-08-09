// Package parquet adapts parquet files: extents are cut at row-group starts,
// with one trailing extent for the page indexes and footer. Readers that fetch
// row groups by offset then hit exactly the covering chunks. No view is
// needed; the whole benefit is delivered by alignment.
package parquet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"

	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/format"
)

const FormatName = "parquet"

var magic = []byte("PAR1")

func init() {
	format.Register(Adapter{})
}

type Adapter struct{}

func (Adapter) Name() string { return FormatName }

func (Adapter) Sniff(h format.Hint) bool {
	return bytes.HasPrefix(h.Head, magic) && bytes.HasSuffix(h.Tail, magic)
}

// Index reads the footer and cuts one extent per row group. The leading magic
// rides with the first row group; everything after the last row group (page
// indexes, footer) forms the final extent.
func (Adapter) Index(ctx context.Context, r io.ReaderAt, size int64) (*format.Layout, error) {
	file, err := parquetgo.OpenFile(r, size, parquetgo.SkipPageIndex(true), parquetgo.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	rowGroups := file.Metadata().RowGroups
	if len(rowGroups) == 0 {
		return nil, fmt.Errorf("parquet file has no row groups")
	}
	if len(rowGroups) >= format.MaxExtentCount {
		return nil, fmt.Errorf("parquet file has too many row groups: %d", len(rowGroups))
	}

	starts := make([]int64, 0, len(rowGroups))
	var lastEnd int64
	for i, rowGroup := range rowGroups {
		start, end := int64(math.MaxInt64), int64(0)
		for _, column := range rowGroup.Columns {
			columnStart := column.MetaData.DataPageOffset
			if dictionary := column.MetaData.DictionaryPageOffset; dictionary > 0 && dictionary < columnStart {
				columnStart = dictionary
			}
			if columnStart < start {
				start = columnStart
			}
			if columnEnd := columnStart + column.MetaData.TotalCompressedSize; columnEnd > end {
				end = columnEnd
			}
		}
		if len(rowGroup.Columns) == 0 || start >= end || start < int64(len(magic)) || end > size {
			return nil, fmt.Errorf("row group %d has an invalid byte range [%d, %d)", i, start, end)
		}
		if i > 0 && start < lastEnd {
			return nil, fmt.Errorf("row group %d overlaps its predecessor", i)
		}
		starts = append(starts, start)
		lastEnd = end
	}
	if lastEnd >= size {
		return nil, fmt.Errorf("row groups leave no room for the footer")
	}

	var sizes []int64
	var previous int64
	for _, start := range starts[1:] {
		sizes = append(sizes, start-previous)
		previous = start
	}
	sizes = append(sizes, lastEnd-previous)
	sizes = append(sizes, size-lastEnd)

	layout := &format.Layout{Format: FormatName, ExtentSizes: sizes, Align: 1}
	if err := layout.Validate(size); err != nil {
		return nil, err
	}
	return layout, nil
}
