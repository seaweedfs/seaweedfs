package page_writer

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestChunkedFileWriter_toActualOffset(t *testing.T) {
	cw := NewChunkedFileWriter("", 16)

	writeToFile(cw, 50, 60)
	writeToFile(cw, 60, 64)

	writeToFile(cw, 32, 40)
	writeToFile(cw, 42, 48)

	writeToFile(cw, 48, 50)

	assert.Equal(t, 1, cw.chunkUsages[0].size(), "fully covered")
	assert.Equal(t, 2, cw.chunkUsages[1].size(), "2 intervals")

}

func writeToFile(cw *ChunkedFileWriter, startOffset int64, stopOffset int64) {

	_, chunkUsage := cw.toActualWriteOffset(startOffset)

	// skip doing actual writing

	innerOffset := startOffset % cw.ChunkSize
	chunkUsage.MarkWritten(innerOffset, innerOffset+stopOffset-startOffset)

}

func TestWriteChunkedFile(t *testing.T) {
	x := NewChunkedFileWriter(os.TempDir(), 20)
	defer x.Reset()
	y := NewChunkedFileWriter(os.TempDir(), 12)
	defer y.Reset()

	batchSize := 4
	buf := make([]byte, batchSize)
	for i := 0; i < 256; i++ {
		for x := 0; x < batchSize; x++ {
			buf[x] = byte(i)
		}
		x.WriteAt(buf, int64(i*batchSize))
		y.WriteAt(buf, int64((255-i)*batchSize))
	}

	a := make([]byte, 1)
	b := make([]byte, 1)
	for i := 0; i < 256*batchSize; i++ {
		x.ReadDataAt(a, int64(i))
		y.ReadDataAt(b, int64(256*batchSize-1-i))
		assert.Equal(t, a[0], b[0], "same read")
	}

}
