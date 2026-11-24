package filer

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "a",
			Offset:       0,
			Size:         100,
			ModifiedTsNs: 1,
		},
		{
			FileId:       "b",
			Offset:       50,
			Size:         100,
			ModifiedTsNs: 2,
		},
		{
			FileId:       "c",
			Offset:       200,
			Size:         50,
			ModifiedTsNs: 3,
		},
		{
			FileId:       "d",
			Offset:       250,
			Size:         50,
			ModifiedTsNs: 4,
		},
		{
			FileId:       "e",
			Offset:       175,
			Size:         100,
			ModifiedTsNs: 5,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("resolved to %d visible intervales\n", visibles.Len())
	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestReadResolvedChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "c",
			Offset:       200,
			Size:         50,
			ModifiedTsNs: 3,
		},
		{
			FileId:       "e",
			Offset:       200,
			Size:         25,
			ModifiedTsNs: 5,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("resolved to %d visible intervales\n", visibles.Len())
	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestRandomizedReadResolvedChunks(t *testing.T) {

	var limit int64 = 1024 * 1024
	array := make([]int64, limit)
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 1024; ts++ {
		x := rand.Int63n(limit)
		y := rand.Int63n(limit)
		size := x - y
		if size < 0 {
			size = -size
		}
		if size > 1024 {
			size = 1024
		}
		start := x
		if start > y {
			start = y
		}
		chunks = append(chunks, randomWrite(array, start, size, ts))
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		for i := visible.start; i < visible.stop; i++ {
			if array[i] != visible.modifiedTsNs {
				t.Errorf("position %d expected ts %d actual ts %d", i, array[i], visible.modifiedTsNs)
			}
		}
	}

	// fmt.Printf("visibles %d", len(visibles))

}

func randomWrite(array []int64, start int64, size int64, ts int64) *filer_pb.FileChunk {
	for i := start; i < start+size; i++ {
		array[i] = ts
	}
	// fmt.Printf("write [%d,%d) %d\n", start, start+size, ts)
	return &filer_pb.FileChunk{
		FileId:       "",
		Offset:       start,
		Size:         uint64(size),
		ModifiedTsNs: ts,
	}
}

func TestSequentialReadResolvedChunks(t *testing.T) {

	var chunkSize int64 = 1024 * 1024 * 2
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 13; ts++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId:       "",
			Offset:       chunkSize * ts,
			Size:         uint64(chunkSize),
			ModifiedTsNs: 1,
		})
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	fmt.Printf("visibles %d", visibles.Len())

}

func TestActualReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "5,e7b96fef48",
			Offset:       0,
			Size:         2097152,
			ModifiedTsNs: 1634447487595823000,
		},
		{
			FileId:       "5,e5562640b9",
			Offset:       2097152,
			Size:         2097152,
			ModifiedTsNs: 1634447487595826000,
		},
		{
			FileId:       "5,df033e0fe4",
			Offset:       4194304,
			Size:         2097152,
			ModifiedTsNs: 1634447487595827000,
		},
		{
			FileId:       "7,eb08148a9b",
			Offset:       6291456,
			Size:         2097152,
			ModifiedTsNs: 1634447487595827000,
		},
		{
			FileId:       "7,e0f92d1604",
			Offset:       8388608,
			Size:         2097152,
			ModifiedTsNs: 1634447487595828000,
		},
		{
			FileId:       "7,e33cb63262",
			Offset:       10485760,
			Size:         2097152,
			ModifiedTsNs: 1634447487595828000,
		},
		{
			FileId:       "5,ea98e40e93",
			Offset:       12582912,
			Size:         2097152,
			ModifiedTsNs: 1634447487595829000,
		},
		{
			FileId:       "5,e165661172",
			Offset:       14680064,
			Size:         2097152,
			ModifiedTsNs: 1634447487595829000,
		},
		{
			FileId:       "3,e692097486",
			Offset:       16777216,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "3,e28e2e3cbd",
			Offset:       18874368,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "3,e443974d4e",
			Offset:       20971520,
			Size:         2097152,
			ModifiedTsNs: 1634447487595830000,
		},
		{
			FileId:       "2,e815bed597",
			Offset:       23068672,
			Size:         2097152,
			ModifiedTsNs: 1634447487595831000,
		},
		{
			FileId:       "5,e94715199e",
			Offset:       25165824,
			Size:         1974736,
			ModifiedTsNs: 1634447487595832000,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestActualReadResolvedChunks2(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId:       "1,e7b96fef48",
			Offset:       0,
			Size:         184320,
			ModifiedTsNs: 1,
		},
		{
			FileId:       "2,22562640b9",
			Offset:       184320,
			Size:         4096,
			ModifiedTsNs: 2,
		},
		{
			FileId:       "2,33562640b9",
			Offset:       184320,
			Size:         4096,
			ModifiedTsNs: 4,
		},
		{
			FileId:       "4,df033e0fe4",
			Offset:       188416,
			Size:         2097152,
			ModifiedTsNs: 3,
		},
	}

	visibles := readResolvedChunks(chunks, 0, math.MaxInt64)

	for x := visibles.Front(); x != nil; x = x.Next {
		visible := x.Value
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTsNs)
	}

}

func TestReadResolvedChunksTiedTimestamps(t *testing.T) {
	a := &filer_pb.FileChunk{FileId: "1,aaa", Offset: 0, Size: 100, ModifiedTsNs: 7}
	b := &filer_pb.FileChunk{FileId: "2,bbb", Offset: 50, Size: 100, ModifiedTsNs: 7}

	visibles := readResolvedChunks([]*filer_pb.FileChunk{a, b}, 0, 300)

	type span struct {
		start, stop int64
		fileId      string
	}
	var got []span
	for x := visibles.Front(); x != nil; x = x.Next {
		got = append(got, span{x.Value.start, x.Value.stop, x.Value.fileId})
	}
	want := []span{{0, 50, "1,aaa"}, {50, 150, "2,bbb"}}
	if len(got) != len(want) {
		t.Fatalf("visibles = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("visibles = %+v, want %+v", got, want)
		}
	}
}

func TestRandomizedReadResolvedChunksTiedTimestamps(t *testing.T) {
	const limit = 4096
	for round := 0; round < 200; round++ {
		var chunks []*filer_pb.FileChunk
		incremental := NewIntervalList[*VisibleInterval]()
		for i := 0; i < 32; i++ {
			start := rand.Int63n(limit)
			size := 1 + rand.Int63n(256)
			if start+size > limit {
				size = limit - start
			}
			chunk := &filer_pb.FileChunk{
				FileId:       fmt.Sprintf("%d,%d", round, i),
				Offset:       start,
				Size:         uint64(size),
				ModifiedTsNs: rand.Int63n(4), // lots of ties
			}
			chunks = append(chunks, chunk)
			MergeIntoVisibles(incremental, start, start+size, chunk)
		}
		var wantFileId [limit]string
		for x := incremental.Front(); x != nil; x = x.Next {
			for i := x.Value.start; i < x.Value.stop; i++ {
				wantFileId[i] = x.Value.fileId
			}
		}

		// expected max ts at each position, -1 when nothing covers it
		var want [limit]int64
		for i := range want {
			want[i] = -1
		}
		for _, c := range chunks {
			for i := c.Offset; i < c.Offset+int64(c.Size); i++ {
				want[i] = max(want[i], c.ModifiedTsNs)
			}
		}

		var got [limit]int64
		for i := range got {
			got[i] = -1
		}
		visibles := readResolvedChunks(chunks, 0, math.MaxInt64)
		for x := visibles.Front(); x != nil; x = x.Next {
			v := x.Value
			for i := v.start; i < v.stop; i++ {
				if got[i] != -1 {
					t.Fatalf("round %d: position %d covered twice", round, i)
				}
				if v.fileId != wantFileId[i] {
					t.Fatalf("round %d: position %d reads %s, MergeIntoVisibles has %s", round, i, v.fileId, wantFileId[i])
				}
				got[i] = v.modifiedTsNs
			}
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("round %d: position %d expected ts %d actual ts %d", round, i, want[i], got[i])
			}
		}
	}
}
