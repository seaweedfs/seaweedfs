package filer

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"math/rand"
	"testing"
)

func TestReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId: "a",
			Offset: 0,
			Size:   100,
			Mtime:  1,
		},
		{
			FileId: "b",
			Offset: 50,
			Size:   100,
			Mtime:  2,
		},
		{
			FileId: "c",
			Offset: 200,
			Size:   50,
			Mtime:  3,
		},
		{
			FileId: "d",
			Offset: 250,
			Size:   50,
			Mtime:  4,
		},
		{
			FileId: "e",
			Offset: 175,
			Size:   100,
			Mtime:  5,
		},
	}

	visibles := readResolvedChunks(chunks)

	for _, visible := range visibles {
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTime)
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

	visibles := readResolvedChunks(chunks)

	for _, visible := range visibles {
		for i := visible.start; i < visible.stop; i++ {
			if array[i] != visible.modifiedTime {
				t.Errorf("position %d expected ts %d actual ts %d", i, array[i], visible.modifiedTime)
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
		FileId: "",
		Offset: start,
		Size:   uint64(size),
		Mtime:  ts,
	}
}

func TestSequentialReadResolvedChunks(t *testing.T) {

	var chunkSize int64 = 1024 * 1024 * 2
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 13; ts++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: "",
			Offset: chunkSize * ts,
			Size:   uint64(chunkSize),
			Mtime:  1,
		})
	}

	visibles := readResolvedChunks(chunks)

	fmt.Printf("visibles %d", len(visibles))

}

func TestActualReadResolvedChunks(t *testing.T) {

	chunks := []*filer_pb.FileChunk{
		{
			FileId: "5,e7b96fef48",
			Offset: 0,
			Size:   2097152,
			Mtime:  1634447487595823000,
		},
		{
			FileId: "5,e5562640b9",
			Offset: 2097152,
			Size:   2097152,
			Mtime:  1634447487595826000,
		},
		{
			FileId: "5,df033e0fe4",
			Offset: 4194304,
			Size:   2097152,
			Mtime:  1634447487595827000,
		},
		{
			FileId: "7,eb08148a9b",
			Offset: 6291456,
			Size:   2097152,
			Mtime:  1634447487595827000,
		},
		{
			FileId: "7,e0f92d1604",
			Offset: 8388608,
			Size:   2097152,
			Mtime:  1634447487595828000,
		},
		{
			FileId: "7,e33cb63262",
			Offset: 10485760,
			Size:   2097152,
			Mtime:  1634447487595828000,
		},
		{
			FileId: "5,ea98e40e93",
			Offset: 12582912,
			Size:   2097152,
			Mtime:  1634447487595829000,
		},
		{
			FileId: "5,e165661172",
			Offset: 14680064,
			Size:   2097152,
			Mtime:  1634447487595829000,
		},
		{
			FileId: "3,e692097486",
			Offset: 16777216,
			Size:   2097152,
			Mtime:  1634447487595830000,
		},
		{
			FileId: "3,e28e2e3cbd",
			Offset: 18874368,
			Size:   2097152,
			Mtime:  1634447487595830000,
		},
		{
			FileId: "3,e443974d4e",
			Offset: 20971520,
			Size:   2097152,
			Mtime:  1634447487595830000,
		},
		{
			FileId: "2,e815bed597",
			Offset: 23068672,
			Size:   2097152,
			Mtime:  1634447487595831000,
		},
		{
			FileId: "5,e94715199e",
			Offset: 25165824,
			Size:   1974736,
			Mtime:  1634447487595832000,
		},
	}

	visibles := readResolvedChunks(chunks)

	for _, visible := range visibles {
		fmt.Printf("[%d,%d) %s %d\n", visible.start, visible.stop, visible.fileId, visible.modifiedTime)
	}

}
