package idx

import (
	"fmt"
	"io"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type indexEntry struct {
	index  int
	id     types.NeedleId
	offset int64
	size   types.Size
}

func (ie *indexEntry) Compare(other *indexEntry) int {
	if ie.offset < other.offset {
		return -1
	}
	if ie.offset > other.offset {
		return 1
	}
	if ie.size < other.size {
		return -1
	}
	if ie.size > other.size {
		return 1
	}
	return 0
}

// CheckIndexFile verifies the integrity of a IDX/ECX index file. Returns a count of processed file entries, and slice of found errors.
func CheckIndexFile(r io.ReaderAt, indexFileSize int64, version needle.Version) (int64, []error) {
	errs := []error{}

	entries := []*indexEntry{}
	var i int
	err := WalkIndexFile(r, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		entries = append(entries, &indexEntry{
			index:  i,
			id:     id,
			offset: offset.ToActualOffset(),
			size:   size,
		})
		i++
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Compare(entries[j]) < 0
	})

	for i, e := range entries {
		if i == 0 {
			// nothing to check for the first entry
			continue
		}

		start, end := e.offset, e.offset
		if size := needle.GetActualSize(e.size, version); size != 0 {
			end += size - 1
		}

		last := entries[i-1]
		lastStart, lastEnd := last.offset, last.offset
		if lastSize := needle.GetActualSize(last.size, version); lastSize != 0 {
			lastEnd += lastSize - 1
		}

		// check if needles overlap
		if start <= lastEnd {
			errs = append(errs, fmt.Errorf(
				"needle %d (#%d) at [%d-%d] overlaps needle %d at [%d-%d]",
				e.id, e.index+1,
				start, end,
				last.id,
				lastStart, lastEnd))
		}

		// The check below is intended to ensure all index entries are contiguous; unfortunately, Seaweed
		// can delete index entries for files while keeping their data, so volumes with deleted files
		// will fail this test :(
		// See https://github.com/seaweedfs/seaweedfs/issues/8204 for details.
		/*
			if e.offset != lastEnd + 1 {
				errs = append(errs, fmt.Errorf("offset %d for needle %d (#%d) doesn't match end of needle %d at %d", e.offset, e.id, e.index+1, last.id, lastEnd))
			}
		*/
	}

	count := int64(len(entries))
	if got, want := count*types.NeedleMapEntrySize, indexFileSize; got != want {
		errs = append(errs, fmt.Errorf("expected an index file of size %d, got %d", want, got))
	}

	return count, errs
}
