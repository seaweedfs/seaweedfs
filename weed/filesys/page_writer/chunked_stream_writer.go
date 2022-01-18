package page_writer

import (
	"io"
)

type SaveToStorageFunc func(reader io.Reader, offset int64, size int64, cleanupFn func())

type MemChunk struct {
	buf   []byte
	usage *ChunkWrittenIntervalList
}
