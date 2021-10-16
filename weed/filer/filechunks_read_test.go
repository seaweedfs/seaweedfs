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

	var limit int64 = 1024*1024
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
		for i := visible.start; i<visible.stop;i++{
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

	var chunkSize int64 = 1024*1024*2
	var chunks []*filer_pb.FileChunk
	for ts := int64(0); ts < 13; ts++ {
		chunks = append(chunks, &filer_pb.FileChunk{
			FileId: "",
			Offset: chunkSize*ts,
			Size:   uint64(chunkSize),
			Mtime:  1,
		})
	}

	visibles := readResolvedChunks(chunks)

	fmt.Printf("visibles %d", len(visibles))

}
