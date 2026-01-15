package idx

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// CheckIndexFile verifies the integrity of a given index (IDX/ECX) file. Returns a count of processed file entries, and slice of found errors.
func CheckIndexFile(r io.ReaderAt, indexFileSize int64, version needle.Version) (int64, []error) {
	errs := []error{}

	var count int64
	var lastActualOffset, nextExpectedOffset int64

	err := WalkIndexFile(r, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		actualOffset := offset.ToActualOffset()

		// skip first iteration
		if lastActualOffset != 0 {
			if actualOffset <= lastActualOffset {
				return fmt.Errorf("offset %d for needle %d (#%d) overlaps previous needle at %d", actualOffset, id, count, lastActualOffset)
			}
			if actualOffset != nextExpectedOffset {
				return fmt.Errorf("offset %d for needle %d (#%d) doesn't match expected %d", actualOffset, id, count, nextExpectedOffset)
			}
		}

		lastActualOffset = actualOffset
		nextExpectedOffset = actualOffset + needle.GetActualSize(size, version)
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	if got, want := (count * types.NeedleMapEntrySize), indexFileSize; got != want {
		errs = append(errs, fmt.Errorf("expected an index file of size %d, got %d", want, got))
	}

	return count, errs
}
