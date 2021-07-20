package filer

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func TestCompactFileChunks(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 10, Size: 100, FileId: "abc", Mtime: 50},
		{Offset: 100, Size: 100, FileId: "def", Mtime: 100},
		{Offset: 200, Size: 100, FileId: "ghi", Mtime: 200},
		{Offset: 110, Size: 200, FileId: "jkl", Mtime: 300},
	}

	compacted, garbage := CompactFileChunks(nil, chunks)

	if len(compacted) != 3 {
		t.Fatalf("unexpected compacted: %d", len(compacted))
	}
	if len(garbage) != 1 {
		t.Fatalf("unexpected garbage: %d", len(garbage))
	}

}

func TestCompactFileChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "abc", Mtime: 50},
		{Offset: 100, Size: 100, FileId: "def", Mtime: 100},
		{Offset: 200, Size: 100, FileId: "ghi", Mtime: 200},
		{Offset: 0, Size: 100, FileId: "abcf", Mtime: 300},
		{Offset: 50, Size: 100, FileId: "fhfh", Mtime: 400},
		{Offset: 100, Size: 100, FileId: "yuyu", Mtime: 500},
	}

	k := 3

	for n := 0; n < k; n++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 100), Size: 100, FileId: fmt.Sprintf("fileId%d", n), Mtime: int64(n),
		})
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 50), Size: 100, FileId: fmt.Sprintf("fileId%d", n+k), Mtime: int64(n + k),
		})
	}

	compacted, garbage := CompactFileChunks(nil, chunks)

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
			FileId: strconv.Itoa(i),
			Offset: int64(start),
			Size:   uint64(stop - start),
			Mtime:  int64(i),
			Fid:    &filer_pb.FileId{FileKey: uint64(i)},
		}
		chunks = append(chunks, chunk)
		for x := start; x < stop; x++ {
			data[x] = byte(i)
		}
	}

	visibles, _ := NonOverlappingVisibleIntervals(nil, chunks, 0, math.MaxInt64)

	for _, v := range visibles {
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
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 100, Size: 100, FileId: "asdf", Mtime: 134},
				{Offset: 200, Size: 100, FileId: "fsad", Mtime: 353},
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
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
			},
		},
		// case 2: updates overwrite part of previous chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", Mtime: 123},
				{Offset: 0, Size: 70, FileId: "b", Mtime: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 70, fileId: "b"},
				{start: 70, stop: 100, fileId: "a", chunkOffset: 70},
			},
		},
		// case 3: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 50, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 50, fileId: "asdf"},
				{start: 50, stop: 300, fileId: "xxxx"},
			},
		},
		// case 4: updates far away from prev chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 250, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "asdf"},
				{start: 250, stop: 500, fileId: "xxxx"},
			},
		},
		// case 5: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "d", Mtime: 184},
				{Offset: 70, Size: 150, FileId: "c", Mtime: 143},
				{Offset: 80, Size: 100, FileId: "b", Mtime: 134},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 200, fileId: "d"},
				{start: 200, stop: 220, fileId: "c", chunkOffset: 130},
			},
		},
		// case 6: same updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Fid: &filer_pb.FileId{FileKey: 1}, Mtime: 123},
				{Offset: 0, Size: 100, FileId: "axf", Fid: &filer_pb.FileId{FileKey: 2}, Mtime: 123},
				{Offset: 0, Size: 100, FileId: "xyz", Fid: &filer_pb.FileId{FileKey: 3}, Mtime: 123},
			},
			Expected: []*VisibleInterval{
				{start: 0, stop: 100, fileId: "xyz"},
			},
		},
		// case 7: real updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 2097152, FileId: "7,0294cbb9892b", Mtime: 123},
				{Offset: 0, Size: 3145728, FileId: "3,029565bf3092", Mtime: 130},
				{Offset: 2097152, Size: 3145728, FileId: "6,029632f47ae2", Mtime: 140},
				{Offset: 5242880, Size: 3145728, FileId: "2,029734c5aa10", Mtime: 150},
				{Offset: 8388608, Size: 3145728, FileId: "5,02982f80de50", Mtime: 160},
				{Offset: 11534336, Size: 2842193, FileId: "7,0299ad723803", Mtime: 170},
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
				{Offset: 0, Size: 77824, FileId: "4,0b3df938e301", Mtime: 123},
				{Offset: 471040, Size: 472225 - 471040, FileId: "6,0b3e0650019c", Mtime: 130},
				{Offset: 77824, Size: 208896 - 77824, FileId: "4,0b3f0c7202f0", Mtime: 140},
				{Offset: 208896, Size: 339968 - 208896, FileId: "2,0b4031a72689", Mtime: 150},
				{Offset: 339968, Size: 471040 - 339968, FileId: "3,0b416a557362", Mtime: 160},
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
		intervals, _ := NonOverlappingVisibleIntervals(nil, testcase.Chunks, 0, math.MaxInt64)
		for x, interval := range intervals {
			log.Printf("test case %d, interval %d, start=%d, stop=%d, fileId=%s",
				i, x, interval.start, interval.stop, interval.fileId)
		}
		for x, interval := range intervals {
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
			if interval.chunkOffset != testcase.Expected[x].chunkOffset {
				t.Fatalf("failed on test case %d, interval %d, chunkOffset %d, expect %d",
					i, x, interval.chunkOffset, testcase.Expected[x].chunkOffset)
			}
		}
		if len(intervals) != len(testcase.Expected) {
			t.Fatalf("failed to compact test case %d, len %d expected %d", i, len(intervals), len(testcase.Expected))
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
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 100, Size: 100, FileId: "asdf", Mtime: 134},
				{Offset: 200, Size: 100, FileId: "fsad", Mtime: 353},
			},
			Offset: 0,
			Size:   250,
			Expected: []*ChunkView{
				{Offset: 0, Size: 100, FileId: "abc", LogicOffset: 0},
				{Offset: 0, Size: 100, FileId: "asdf", LogicOffset: 100},
				{Offset: 0, Size: 50, FileId: "fsad", LogicOffset: 200},
			},
		},
		// case 1: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
			},
			Offset: 50,
			Size:   100,
			Expected: []*ChunkView{
				{Offset: 50, Size: 100, FileId: "asdf", LogicOffset: 50},
			},
		},
		// case 2: updates overwrite part of previous chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 3, Size: 100, FileId: "a", Mtime: 123},
				{Offset: 10, Size: 50, FileId: "b", Mtime: 134},
			},
			Offset: 30,
			Size:   40,
			Expected: []*ChunkView{
				{Offset: 20, Size: 30, FileId: "b", LogicOffset: 30},
				{Offset: 57, Size: 10, FileId: "a", LogicOffset: 60},
			},
		},
		// case 3: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 50, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Offset: 0,
			Size:   200,
			Expected: []*ChunkView{
				{Offset: 0, Size: 50, FileId: "asdf", LogicOffset: 0},
				{Offset: 0, Size: 150, FileId: "xxxx", LogicOffset: 50},
			},
		},
		// case 4: updates far away from prev chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 250, Size: 250, FileId: "xxxx", Mtime: 154},
			},
			Offset: 0,
			Size:   400,
			Expected: []*ChunkView{
				{Offset: 0, Size: 200, FileId: "asdf", LogicOffset: 0},
				{Offset: 0, Size: 150, FileId: "xxxx", LogicOffset: 250},
			},
		},
		// case 5: updates overwrite full chunks
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", Mtime: 123},
				{Offset: 0, Size: 200, FileId: "c", Mtime: 184},
				{Offset: 70, Size: 150, FileId: "b", Mtime: 143},
				{Offset: 80, Size: 100, FileId: "xxxx", Mtime: 134},
			},
			Offset: 0,
			Size:   220,
			Expected: []*ChunkView{
				{Offset: 0, Size: 200, FileId: "c", LogicOffset: 0},
				{Offset: 130, Size: 20, FileId: "b", LogicOffset: 200},
			},
		},
		// case 6: same updates
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Fid: &filer_pb.FileId{FileKey: 1}, Mtime: 123},
				{Offset: 0, Size: 100, FileId: "def", Fid: &filer_pb.FileId{FileKey: 2}, Mtime: 123},
				{Offset: 0, Size: 100, FileId: "xyz", Fid: &filer_pb.FileId{FileKey: 3}, Mtime: 123},
			},
			Offset: 0,
			Size:   100,
			Expected: []*ChunkView{
				{Offset: 0, Size: 100, FileId: "xyz", LogicOffset: 0},
			},
		},
		// case 7: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 100, Size: 100, FileId: "asdf", Mtime: 134},
				{Offset: 200, Size: 100, FileId: "fsad", Mtime: 353},
			},
			Offset: 0,
			Size:   200,
			Expected: []*ChunkView{
				{Offset: 0, Size: 100, FileId: "abc", LogicOffset: 0},
				{Offset: 0, Size: 100, FileId: "asdf", LogicOffset: 100},
			},
		},
		// case 8: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "abc", Mtime: 123},
				{Offset: 90, Size: 200, FileId: "asdf", Mtime: 134},
				{Offset: 190, Size: 300, FileId: "fsad", Mtime: 353},
			},
			Offset: 0,
			Size:   300,
			Expected: []*ChunkView{
				{Offset: 0, Size: 90, FileId: "abc", LogicOffset: 0},
				{Offset: 0, Size: 100, FileId: "asdf", LogicOffset: 90},
				{Offset: 0, Size: 110, FileId: "fsad", LogicOffset: 190},
			},
		},
		// case 9: edge cases
		{
			Chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 43175947, FileId: "2,111fc2cbfac1", Mtime: 1},
				{Offset: 43175936, Size: 52981771 - 43175936, FileId: "2,112a36ea7f85", Mtime: 2},
				{Offset: 52981760, Size: 72564747 - 52981760, FileId: "4,112d5f31c5e7", Mtime: 3},
				{Offset: 72564736, Size: 133255179 - 72564736, FileId: "1,113245f0cdb6", Mtime: 4},
				{Offset: 133255168, Size: 137269259 - 133255168, FileId: "3,1141a70733b5", Mtime: 5},
				{Offset: 137269248, Size: 153578836 - 137269248, FileId: "1,114201d5bbdb", Mtime: 6},
			},
			Offset: 0,
			Size:   153578836,
			Expected: []*ChunkView{
				{Offset: 0, Size: 43175936, FileId: "2,111fc2cbfac1", LogicOffset: 0},
				{Offset: 0, Size: 52981760 - 43175936, FileId: "2,112a36ea7f85", LogicOffset: 43175936},
				{Offset: 0, Size: 72564736 - 52981760, FileId: "4,112d5f31c5e7", LogicOffset: 52981760},
				{Offset: 0, Size: 133255168 - 72564736, FileId: "1,113245f0cdb6", LogicOffset: 72564736},
				{Offset: 0, Size: 137269248 - 133255168, FileId: "3,1141a70733b5", LogicOffset: 133255168},
				{Offset: 0, Size: 153578836 - 137269248, FileId: "1,114201d5bbdb", LogicOffset: 137269248},
			},
		},
	}

	for i, testcase := range testcases {
		if i != 2 {
			// continue
		}
		log.Printf("++++++++++ read test case %d ++++++++++++++++++++", i)
		chunks := ViewFromChunks(nil, testcase.Chunks, testcase.Offset, testcase.Size)
		for x, chunk := range chunks {
			log.Printf("read case %d, chunk %d, offset=%d, size=%d, fileId=%s",
				i, x, chunk.Offset, chunk.Size, chunk.FileId)
			if chunk.Offset != testcase.Expected[x].Offset {
				t.Fatalf("failed on read case %d, chunk %s, Offset %d, expect %d",
					i, chunk.FileId, chunk.Offset, testcase.Expected[x].Offset)
			}
			if chunk.Size != testcase.Expected[x].Size {
				t.Fatalf("failed on read case %d, chunk %s, Size %d, expect %d",
					i, chunk.FileId, chunk.Size, testcase.Expected[x].Size)
			}
			if chunk.FileId != testcase.Expected[x].FileId {
				t.Fatalf("failed on read case %d, chunk %d, FileId %s, expect %s",
					i, x, chunk.FileId, testcase.Expected[x].FileId)
			}
			if chunk.LogicOffset != testcase.Expected[x].LogicOffset {
				t.Fatalf("failed on read case %d, chunk %d, LogicOffset %d, expect %d",
					i, x, chunk.LogicOffset, testcase.Expected[x].LogicOffset)
			}
		}
		if len(chunks) != len(testcase.Expected) {
			t.Fatalf("failed to read test case %d, len %d expected %d", i, len(chunks), len(testcase.Expected))
		}
	}

}

func BenchmarkCompactFileChunks(b *testing.B) {

	var chunks []*filer_pb.FileChunk

	k := 1024

	for n := 0; n < k; n++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 100), Size: 100, FileId: fmt.Sprintf("fileId%d", n), Mtime: int64(n),
		})
		chunks = append(chunks, &filer_pb.FileChunk{
			Offset: int64(n * 50), Size: 100, FileId: fmt.Sprintf("fileId%d", n+k), Mtime: int64(n + k),
		})
	}

	for n := 0; n < b.N; n++ {
		CompactFileChunks(nil, chunks)
	}
}

func TestViewFromVisibleIntervals(t *testing.T) {
	visibles := []VisibleInterval{
		{
			start:  0,
			stop:   25,
			fileId: "fid1",
		},
		{
			start:  4096,
			stop:   8192,
			fileId: "fid2",
		},
		{
			start:  16384,
			stop:   18551,
			fileId: "fid3",
		},
	}

	views := ViewFromVisibleIntervals(visibles, 0, math.MaxInt32)

	if len(views) != len(visibles) {
		assert.Equal(t, len(visibles), len(views), "ViewFromVisibleIntervals error")
	}

}

func TestViewFromVisibleIntervals2(t *testing.T) {
	visibles := []VisibleInterval{
		{
			start:  344064,
			stop:   348160,
			fileId: "fid1",
		},
		{
			start:  348160,
			stop:   356352,
			fileId: "fid2",
		},
	}

	views := ViewFromVisibleIntervals(visibles, 0, math.MaxInt32)

	if len(views) != len(visibles) {
		assert.Equal(t, len(visibles), len(views), "ViewFromVisibleIntervals error")
	}

}

func TestViewFromVisibleIntervals3(t *testing.T) {
	visibles := []VisibleInterval{
		{
			start:  1000,
			stop:   2000,
			fileId: "fid1",
		},
		{
			start:  3000,
			stop:   4000,
			fileId: "fid2",
		},
	}

	views := ViewFromVisibleIntervals(visibles, 1700, 1500)

	if len(views) != len(visibles) {
		assert.Equal(t, len(visibles), len(views), "ViewFromVisibleIntervals error")
	}

}
