package page_writer

import (
	"io"
)

type SaveToStorageFunc func(reader io.Reader, offset int64, size int64, modifiedTsNs int64, cleanupFn func())

// FillChunkFunc reads existing file data into buf starting at the given
// absolute file offset.  Used to fill unwritten gaps in a partial chunk
// so that it can be uploaded as a single complete chunk instead of many
// small fragments.
type FillChunkFunc func(buf []byte, fileOffset int64)

type PageChunk interface {
	FreeResource()
	WriteDataAt(src []byte, offset int64, tsNs int64) (n int)
	ReadDataAt(p []byte, off int64, tsNs int64) (maxStop int64)
	IsComplete() bool
	ActivityScore() int64
	WrittenSize() int64
	LastWriteTsNs() int64
	SaveContent(saveFn SaveToStorageFunc)
	// FillGaps reads existing file data into unwritten regions of the
	// chunk buffer so that SaveContent can upload the whole logical chunk
	// as a single piece rather than one fragment per written interval.
	FillGaps(fill FillChunkFunc)
}
