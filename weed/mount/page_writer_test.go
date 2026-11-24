package mount

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
)

// stubDirtyPages reports dirty data only for the chunks listed in dirty,
// filling the whole requested range of those chunks.
type stubDirtyPages struct {
	page_writer.DirtyPages
	chunkSize int64
	dirty     map[int64]bool
}

func (s *stubDirtyPages) ReadDirtyDataAt(data []byte, startOffset int64, tsNs int64) (maxStop int64) {
	if !s.dirty[startOffset/s.chunkSize] {
		return 0
	}
	for i := range data {
		data[i] = 'x'
	}
	return startOffset + int64(len(data))
}

func TestPageWriterReadDirtyDataAtAcrossChunks(t *testing.T) {
	const chunkSize = 4

	tests := []struct {
		name   string
		dirty  map[int64]bool
		offset int64
		size   int
		want   int64
	}{
		{name: "dirty then clean", dirty: map[int64]bool{0: true}, offset: 2, size: 4, want: 4},
		{name: "clean then dirty", dirty: map[int64]bool{1: true}, offset: 2, size: 4, want: 6},
		{name: "dirty clean clean", dirty: map[int64]bool{0: true}, offset: 0, size: 12, want: 4},
		{name: "dirty clean dirty", dirty: map[int64]bool{0: true, 2: true}, offset: 0, size: 12, want: 12},
		{name: "all clean", dirty: map[int64]bool{}, offset: 0, size: 12, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pw := &PageWriter{
				fh:           &FileHandle{},
				chunkSize:    chunkSize,
				randomWriter: &stubDirtyPages{chunkSize: chunkSize, dirty: tt.dirty},
			}
			if got := pw.ReadDirtyDataAt(make([]byte, tt.size), tt.offset, 0); got != tt.want {
				t.Errorf("ReadDirtyDataAt(offset=%d, size=%d) = %d, want %d", tt.offset, tt.size, got, tt.want)
			}
		})
	}
}
