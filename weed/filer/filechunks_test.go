package filer

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestCompactFileChunks(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 10, Size: 100, FileId: "abc", ModifiedTsNs: 50},
		{Offset: 100, Size: 100, FileId: "def", ModifiedTsNs: 100},
		{Offset: 200, Size: 100, FileId: "ghi", ModifiedTsNs: 200},
		{Offset: 110, Size: 200, FileId: "jkl", ModifiedTsNs: 300},
	}

	compacted, garbage := CompactFileChunks(context.Background(), nil, chunks)

	if len(compacted) != 3 {
		t.Fatalf("unexpected compacted: %d", len(compacted))
	}
	if len(garbage) != 1 {
		t.Fatalf("unexpected garbage: %d", len(garbage))
	}

}

func TestCompactFileChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 50},
		{Offset: 100, Size: 100, FileId: "def", ModifiedTsNs: 100},
		{Offset: 200, Size: 100, FileId: "ghi", ModifiedTsNs: 200},
		{Offset: 0, Size: 100, FileId: "abcf", ModifiedTsNs: 300},
		{Offset: 50, Size: 100, FileId: "fhfh", ModifiedTsNs: 400},
		{Offset: 100, Size: 100, FileId: "yuyu", ModifiedTsNs: 500},
	}

	k := 3

	for n := 0; n < k; n++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 100), Size: 100, FileId: fmt.Sprintf("fileId%d", n), ModifiedTsNs: int64(n),
		})
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 50), Size: 100, FileId: fmt.Sprintf("fileId%d", n+k), ModifiedTsNs: int64(n + k),
		})
	}

	compacted, garbage := CompactFileChunks(context.Background(), nil, chunks)

	if len(compacted) != 4 {
		t.Fatalf("unexpected compacted: %d", len(compacted))
	}
	if len(garbage) != 8 {
		t.Fatalf("unexpected garbage: %d", len(garbage))
	}
}

func TestRandomFileChunksCompact(t *testing.T) {

	data := make([]byte, 1024)

	var chunks []*filer_pb.FileChunk
	for i := 0; i < 15; i++ {
		start, stop := rand.Intn(len(data)), rand.Intn(len(data))
		if start > stop {
			start, stop = stop, start
		}
		if start+16 < stop {
			stop = start + 16
		}
		chunk := &filer_pb.FileChunk{
			FileId:       strconv.Itoa(i),
			Offset:       int64(start),
			Size:         uint64(stop - start),
			ModifiedTsNs: int64(i),
			Fid:          &filer_pb.FileId{FileKey: uint64(i)},
		}
		chunks = append(chunks, chunk)
		for x := start; x < stop; x++ {
			data[x] = byte(i)
		}
	}

	visibles, _ := NonOverlappingVisibleIntervals(context.Background(), nil, chunks, 0, math.MaxInt64)

	for visible := visibles.Front(); visible != nil; visible = visible.Next {
		v := visible.Value
		for x := v.start; x < v.stop; x++ {
			assert.Equal(t, strconv.Itoa(int(data[x])), v.fileId)
		}
	}

}

func TestIntervalMerging(t *testing.T) {

	testcases := []struct {
		Chunks   []*filer_pb.FileChunk
		Expected []*VisibleInterval
	}{
		// case 0: normal
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 100, Size: 100, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 200, Size: 100, FileId: "fsad", ModifiedTsNs: 353},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 100, fileId: "abc"},
				{start: 100, stop: 200, fileId: "asdf"},
				{start: 200, stop: 300, fileId: "fsad"},
			},
		},
		// case 1: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
			},
		},
		// case 2: updates overwrite part of previous chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 123},
				{Offset: 0, Size: 70, FileId: "b", ModifiedTsNs: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 70, fileId: "b"},
				{start: 70, stop: 100, fileId: "a", offsetInChunk: 70},
			},
		},
		// case 3: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 50, Size: 250, FileId: "xxxx", ModifiedTsNs: 154},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 50, fileId: "asdf"},
				{start: 50, stop: 300, fileId: "xxxx"},
			},
		},
		// case 4: updates far away from prev chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 250, Size: 250, FileId: "xxxx", ModifiedTsNs: 154},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
				{start: 250, stop: 500, fileId: "xxxx"},
			},
		},
		// case 5: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "d", ModifiedTsNs: 184},
				{Offset: 70, Size: 150, FileId: "c", ModifiedTsNs: 143},
				{Offset: 80, Size: 100, FileId: "b", ModifiedTsNs: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "d"},
				{start: 200, stop: 220, fileId: "c", offsetInChunk: 130},
			},
		},
		// case 6: same updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Fid: &filer_pb.FileId{FileKey: 1}, ModifiedTsNs: 123},
				{Offset: 0, Size: 100, FileId: "axf", Fid: &filer_pb.FileId{FileKey: 2}, ModifiedTsNs: 124},
				{Offset: 0, Size: 100, FileId: "xyz", Fid: &filer_pb.FileId{FileKey: 3}, ModifiedTsNs: 125},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 100, fileId: "xyz"},
			},
		},
		// case 7: real updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 2097152, FileId: "7,0294cbb9892b", ModifiedTsNs: 123},
				{Offset: 0, Size: 3145728, FileId: "3,029565bf3092", ModifiedTsNs: 130},
				{Offset: 2097152, Size: 3145728, FileId: "6,029632f47ae2", ModifiedTsNs: 140},
				{Offset: 5242880, Size: 3145728, FileId: "2,029734c5aa10", ModifiedTsNs: 150},
				{Offset: 8388608, Size: 3145728, FileId: "5,02982f80de50", ModifiedTsNs: 160},
				{Offset: 11534336, Size: 2842193, FileId: "7,0299ad723803", ModifiedTsNs: 170},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 2097152, fileId: "3,029565bf3092"},
				{start: 2097152, stop: 5242880, fileId: "6,029632f47ae2"},
				{start: 5242880, stop: 8388608, fileId: "2,029734c5aa10"},
				{start: 8388608, stop: 11534336, fileId: "5,02982f80de50"},
				{start: 11534336, stop: 14376529, fileId: "7,0299ad723803"},
			},
		},
		// case 8: real bug
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 77824, FileId: "4,0b3df938e301", ModifiedTsNs: 123},
				{Offset: 471040, Size: 472225 - 471040, FileId: "6,0b3e0650019c", ModifiedTsNs: 130},
				{Offset: 77824, Size: 208896 - 77824, FileId: "4,0b3f0c7202f0", ModifiedTsNs: 140},
				{Offset: 208896, Size: 339968 - 208896, FileId: "2,0b4031a72689", ModifiedTsNs: 150},
				{Offset: 339968, Size: 471040 - 339968, FileId: "3,0b416a557362", ModifiedTsNs: 160},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 77824, fileId: "4,0b3df938e301"},
				{start: 77824, stop: 208896, fileId: "4,0b3f0c7202f0"},
				{start: 208896, stop: 339968, fileId: "2,0b4031a72689"},
				{start: 339968, stop: 471040, fileId: "3,0b416a557362"},
				{start: 471040, stop: 472225, fileId: "6,0b3e0650019c"},
			},
		},
	}

	for i, testcase := range testcases {
		log.Printf("++++++++++ merged test case %d ++++++++++++++++++++", i)
		intervals, _ := NonOverlappingVisibleIntervals(context.Background(), nil, testcase.Chunks, 0, math.MaxInt64)
		x := -1
		for visible := intervals.Front(); visible != nil; visible = visible.Next {
			x++
			interval := visible.Value
			log.Printf("test case %d, interval start=%d, stop=%d, fileId=%s",
				i, interval.start, interval.stop, interval.fileId)
		}
		x = -1
		for visible := intervals.Front(); visible != nil; visible = visible.Next {
			x++
			interval := visible.Value
			if interval.start != testcase.Expected[x].start {
				t.Fatalf("failed on test case %d, interval %d, start %d, expect %d",
					i, x, interval.start, testcase.Expected[x].start)
			}
			if interval.stop != testcase.Expected[x].stop {
				t.Fatalf("failed on test case %d, interval %d, stop %d, expect %d",
					i, x, interval.stop, testcase.Expected[x].stop)
			}
			if interval.fileId != testcase.Expected[x].fileId {
				t.Fatalf("failed on test case %d, interval %d, chunkId %s, expect %s",
					i, x, interval.fileId, testcase.Expected[x].fileId)
			}
			if interval.offsetInChunk != testcase.Expected[x].offsetInChunk {
				t.Fatalf("failed on test case %d, interval %d, offsetInChunk %d, expect %d",
					i, x, interval.offsetInChunk, testcase.Expected[x].offsetInChunk)
			}
		}
		if intervals.Len() != len(testcase.Expected) {
			t.Fatalf("failed to compact test case %d, len %d expected %d", i, intervals.Len(), len(testcase.Expected))
		}

	}

}

func TestChunksReading(t *testing.T) {

	testcases := []struct {
		Chunks   []*filer_pb.FileChunk
		Offset   int64
		Size     int64
		Expected []*ChunkView
	}{
		// case 0: normal
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 100, Size: 100, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 200, Size: 100, FileId: "fsad", ModifiedTsNs: 353},
			},
			Offset: 0,
			Size:   250,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 100, FileId: "abc", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 100, FileId: "asdf", ViewOffset: 100},
				{OffsetInChunk: 0, ViewSize: 50, FileId: "fsad", ViewOffset: 200},
			},
		},
		// case 1: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
			},
			Offset: 50,
			Size:   100,
			Expected: []*ChunkView{
				{OffsetInChunk: 50, ViewSize: 100, FileId: "asdf", ViewOffset: 50},
			},
		},
		// case 2: updates overwrite part of previous chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 3, Size: 100, FileId: "a", ModifiedTsNs: 123},
				{Offset: 10, Size: 50, FileId: "b", ModifiedTsNs: 134},
			},
			Offset: 30,
			Size:   40,
			Expected: []*ChunkView{
				{OffsetInChunk: 20, ViewSize: 30, FileId: "b", ViewOffset: 30},
				{OffsetInChunk: 57, ViewSize: 10, FileId: "a", ViewOffset: 60},
			},
		},
		// case 3: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 50, Size: 250, FileId: "xxxx", ModifiedTsNs: 154},
			},
			Offset: 0,
			Size:   200,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 50, FileId: "asdf", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 150, FileId: "xxxx", ViewOffset: 50},
			},
		},
		// case 4: updates far away from prev chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 250, Size: 250, FileId: "xxxx", ModifiedTsNs: 154},
			},
			Offset: 0,
			Size:   400,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 200, FileId: "asdf", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 150, FileId: "xxxx", ViewOffset: 250},
			},
		},
		// case 5: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 123},
				{Offset: 0, Size: 200, FileId: "c", ModifiedTsNs: 184},
				{Offset: 70, Size: 150, FileId: "b", ModifiedTsNs: 143},
				{Offset: 80, Size: 100, FileId: "xxxx", ModifiedTsNs: 134},
			},
			Offset: 0,
			Size:   220,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 200, FileId: "c", ViewOffset: 0},
				{OffsetInChunk: 130, ViewSize: 20, FileId: "b", ViewOffset: 200},
			},
		},
		// case 6: same updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Fid: &filer_pb.FileId{FileKey: 1}, ModifiedTsNs: 123},
				{Offset: 0, Size: 100, FileId: "def", Fid: &filer_pb.FileId{FileKey: 2}, ModifiedTsNs: 124},
				{Offset: 0, Size: 100, FileId: "xyz", Fid: &filer_pb.FileId{FileKey: 3}, ModifiedTsNs: 125},
			},
			Offset: 0,
			Size:   100,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 100, FileId: "xyz", ViewOffset: 0},
			},
		},
		// case 7: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 100, Size: 100, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 200, Size: 100, FileId: "fsad", ModifiedTsNs: 353},
			},
			Offset: 0,
			Size:   200,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 100, FileId: "abc", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 100, FileId: "asdf", ViewOffset: 100},
			},
		},
		// case 8: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 123},
				{Offset: 90, Size: 200, FileId: "asdf", ModifiedTsNs: 134},
				{Offset: 190, Size: 300, FileId: "fsad", ModifiedTsNs: 353},
			},
			Offset: 0,
			Size:   300,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 90, FileId: "abc", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 100, FileId: "asdf", ViewOffset: 90},
				{OffsetInChunk: 0, ViewSize: 110, FileId: "fsad", ViewOffset: 190},
			},
		},
		// case 9: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 43175947, FileId: "2,111fc2cbfac1", ModifiedTsNs: 1},
				{Offset: 43175936, Size: 52981771 - 43175936, FileId: "2,112a36ea7f85", ModifiedTsNs: 2},
				{Offset: 52981760, Size: 72564747 - 52981760, FileId: "4,112d5f31c5e7", ModifiedTsNs: 3},
				{Offset: 72564736, Size: 133255179 - 72564736, FileId: "1,113245f0cdb6", ModifiedTsNs: 4},
				{Offset: 133255168, Size: 137269259 - 133255168, FileId: "3,1141a70733b5", ModifiedTsNs: 5},
				{Offset: 137269248, Size: 153578836 - 137269248, FileId: "1,114201d5bbdb", ModifiedTsNs: 6},
			},
			Offset: 0,
			Size:   153578836,
			Expected: []*ChunkView{
				{OffsetInChunk: 0, ViewSize: 43175936, FileId: "2,111fc2cbfac1", ViewOffset: 0},
				{OffsetInChunk: 0, ViewSize: 52981760 - 43175936, FileId: "2,112a36ea7f85", ViewOffset: 43175936},
				{OffsetInChunk: 0, ViewSize: 72564736 - 52981760, FileId: "4,112d5f31c5e7", ViewOffset: 52981760},
				{OffsetInChunk: 0, ViewSize: 133255168 - 72564736, FileId: "1,113245f0cdb6", ViewOffset: 72564736},
				{OffsetInChunk: 0, ViewSize: 137269248 - 133255168, FileId: "3,1141a70733b5", ViewOffset: 133255168},
				{OffsetInChunk: 0, ViewSize: 153578836 - 137269248, FileId: "1,114201d5bbdb", ViewOffset: 137269248},
			},
		},
	}

	for i, testcase := range testcases {
		if i != 2 {
			// continue
		}
		log.Printf("++++++++++ read test case %d ++++++++++++++++++++", i)
		chunks := ViewFromChunks(context.Background(), nil, testcase.Chunks, testcase.Offset, testcase.Size)
		x := -1
		for c := chunks.Front(); c != nil; c = c.Next {
			x++
			chunk := c.Value
			log.Printf("read case %d, chunk %d, offset=%d, size=%d, fileId=%s",
				i, x, chunk.OffsetInChunk, chunk.ViewSize, chunk.FileId)
			if chunk.OffsetInChunk != testcase.Expected[x].OffsetInChunk {
				t.Fatalf("failed on read case %d, chunk %s, Offset %d, expect %d",
					i, chunk.FileId, chunk.OffsetInChunk, testcase.Expected[x].OffsetInChunk)
			}
			if chunk.ViewSize != testcase.Expected[x].ViewSize {
				t.Fatalf("failed on read case %d, chunk %s, ViewSize %d, expect %d",
					i, chunk.FileId, chunk.ViewSize, testcase.Expected[x].ViewSize)
			}
			if chunk.FileId != testcase.Expected[x].FileId {
				t.Fatalf("failed on read case %d, chunk %d, FileId %s, expect %s",
					i, x, chunk.FileId, testcase.Expected[x].FileId)
			}
			if chunk.ViewOffset != testcase.Expected[x].ViewOffset {
				t.Fatalf("failed on read case %d, chunk %d, ViewOffset %d, expect %d",
					i, x, chunk.ViewOffset, testcase.Expected[x].ViewOffset)
			}
		}
		if chunks.Len() != len(testcase.Expected) {
			t.Fatalf("failed to read test case %d, len %d expected %d", i, chunks.Len(), len(testcase.Expected))
		}
	}

}

func BenchmarkCompactFileChunks(b *testing.B) {

	var chunks []*filer_pb.FileChunk

	k := 1024

	for n := 0; n < k; n++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 100), Size: 100, FileId: fmt.Sprintf("fileId%d", n), ModifiedTsNs: int64(n),
		})
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 50), Size: 100, FileId: fmt.Sprintf("fileId%d", n+k), ModifiedTsNs: int64(n + k),
		})
	}

	for n := 0; n < b.N; n++ {
		CompactFileChunks(context.Background(), nil, chunks)
	}
}

func addVisibleInterval(visibles *IntervalList[*VisibleInterval], x *VisibleInterval) {
	visibles.AppendInterval(&Interval[*VisibleInterval]{
		StartOffset: x.start,
		StopOffset:  x.stop,
		TsNs:        x.modifiedTsNs,
		Value:       x,
	})
}

func TestViewFromVisibleIntervals(t *testing.T) {
	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:  0,
		stop:   25,
		fileId: "fid1",
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:  4096,
		stop:   8192,
		fileId: "fid2",
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:  16384,
		stop:   18551,
		fileId: "fid3",
	})

	views := ViewFromVisibleIntervals(visibles, 0, math.MaxInt32)

	if views.Len() != visibles.Len() {
		assert.Equal(t, visibles.Len(), views.Len(), "ViewFromVisibleIntervals error")
	}

}

func TestViewFromVisibleIntervals2(t *testing.T) {
	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:  344064,
		stop:   348160,
		fileId: "fid1",
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:  348160,
		stop:   356352,
		fileId: "fid2",
	})

	views := ViewFromVisibleIntervals(visibles, 0, math.MaxInt32)

	if views.Len() != visibles.Len() {
		assert.Equal(t, visibles.Len(), views.Len(), "ViewFromVisibleIntervals error")
	}

}

func TestViewFromVisibleIntervals3(t *testing.T) {
	visibles := NewIntervalList[*VisibleInterval]()
	addVisibleInterval(visibles, &VisibleInterval{
		start:  1000,
		stop:   2000,
		fileId: "fid1",
	})
	addVisibleInterval(visibles, &VisibleInterval{
		start:  3000,
		stop:   4000,
		fileId: "fid2",
	})

	views := ViewFromVisibleIntervals(visibles, 1700, 1500)

	if views.Len() != visibles.Len() {
		assert.Equal(t, visibles.Len(), views.Len(), "ViewFromVisibleIntervals error")
	}

}

func TestCompactFileChunks3(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "abc", ModifiedTsNs: 50},
		{Offset: 100, Size: 100, FileId: "ghi", ModifiedTsNs: 50},
		{Offset: 200, Size: 100, FileId: "jkl", ModifiedTsNs: 100},
		{Offset: 300, Size: 100, FileId: "def", ModifiedTsNs: 200},
	}

	compacted, _ := CompactFileChunks(context.Background(), nil, chunks)

	if len(compacted) != 4 {
		t.Fatalf("unexpected compacted: %d", len(compacted))
	}
}
