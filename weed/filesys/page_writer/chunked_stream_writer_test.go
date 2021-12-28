package page_writer

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWriteChunkedStream(t *testing.T) {
	x := NewChunkedStreamWriter(20)
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
