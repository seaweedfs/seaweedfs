package filer2

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func TotalSize(chunks []*filer_pb.FileChunk) (size uint64) {
	for _, c := range chunks {
		t := uint64(c.Offset + int64(c.Size))
		if size < t {
			size = t
		}
	}
	return
}

func ETag(chunks []*filer_pb.FileChunk) (etag string) {
	if len(chunks) == 1 {
		return chunks[0].ETag
	}

	h := fnv.New32a()
	for _, c := range chunks {
		h.Write([]byte(c.ETag))
	}
	return fmt.Sprintf("%x", h.Sum32())
}

func CompactFileChunks(chunks []*filer_pb.FileChunk) (compacted, garbage []*filer_pb.FileChunk) {

	visibles := NonOverlappingVisibleIntervals(chunks)

	fileIds := make(map[string]bool)
	for _, interval := range visibles {
		fileIds[interval.fileId] = true
	}
	for _, chunk := range chunks {
		if found := fileIds[chunk.FileId]; found {
			compacted = append(compacted, chunk)
		} else {
			garbage = append(garbage, chunk)
		}
	}

	return
}

func FindUnusedFileChunks(oldChunks, newChunks []*filer_pb.FileChunk) (unused []*filer_pb.FileChunk) {

	fileIds := make(map[string]bool)
	for _, interval := range newChunks {
		fileIds[interval.FileId] = true
	}
	for _, chunk := range oldChunks {
		if found := fileIds[chunk.FileId]; !found {
			unused = append(unused, chunk)
		}
	}

	return
}

type ChunkView struct {
	FileId      string
	Offset      int64
	Size        uint64
	LogicOffset int64
	IsFullChunk bool
}

func ViewFromChunks(chunks []*filer_pb.FileChunk, offset int64, size int) (views []*ChunkView) {

	visibles := NonOverlappingVisibleIntervals(chunks)

	return ViewFromVisibleIntervals(visibles, offset, size)

}

func ViewFromVisibleIntervals(visibles []VisibleInterval, offset int64, size int) (views []*ChunkView) {

	stop := offset + int64(size)

	for _, chunk := range visibles {
		if chunk.start <= offset && offset < chunk.stop && offset < stop {
			isFullChunk := chunk.isFullChunk && chunk.start == offset && chunk.stop <= stop
			views = append(views, &ChunkView{
				FileId:      chunk.fileId,
				Offset:      offset - chunk.start, // offset is the data starting location in this file id
				Size:        uint64(min(chunk.stop, stop) - offset),
				LogicOffset: offset,
				IsFullChunk: isFullChunk,
			})
			offset = min(chunk.stop, stop)
		}
	}

	return views

}

func logPrintf(name string, visibles []VisibleInterval) {
	/*
		log.Printf("%s len %d", name, len(visibles))
		for _, v := range visibles {
			log.Printf("%s:  => %+v", name, v)
		}
	*/
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(VisibleInterval)
	},
}

func MergeIntoVisibles(visibles, newVisibles []VisibleInterval, chunk *filer_pb.FileChunk) []VisibleInterval {

	newV := newVisibleInterval(
		chunk.Offset,
		chunk.Offset+int64(chunk.Size),
		chunk.FileId,
		chunk.Mtime,
		true,
	)

	length := len(visibles)
	if length == 0 {
		return append(visibles, newV)
	}
	last := visibles[length-1]
	if last.stop <= chunk.Offset {
		return append(visibles, newV)
	}

	logPrintf("  before", visibles)
	for _, v := range visibles {
		if v.start < chunk.Offset && chunk.Offset < v.stop {
			newVisibles = append(newVisibles, newVisibleInterval(
				v.start,
				chunk.Offset,
				v.fileId,
				v.modifiedTime,
				false,
			))
		}
		chunkStop := chunk.Offset + int64(chunk.Size)
		if v.start < chunkStop && chunkStop < v.stop {
			newVisibles = append(newVisibles, newVisibleInterval(
				chunkStop,
				v.stop,
				v.fileId,
				v.modifiedTime,
				false,
			))
		}
		if chunkStop <= v.start || v.stop <= chunk.Offset {
			newVisibles = append(newVisibles, v)
		}
	}
	newVisibles = append(newVisibles, newV)

	logPrintf("  append", newVisibles)

	for i := len(newVisibles) - 1; i >= 0; i-- {
		if i > 0 && newV.start < newVisibles[i-1].start {
			newVisibles[i] = newVisibles[i-1]
		} else {
			newVisibles[i] = newV
			break
		}
	}
	logPrintf("  sorted", newVisibles)

	return newVisibles
}

func NonOverlappingVisibleIntervals(chunks []*filer_pb.FileChunk) (visibles []VisibleInterval) {

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Mtime < chunks[j].Mtime
	})

	var newVisibles []VisibleInterval
	for _, chunk := range chunks {
		newVisibles = MergeIntoVisibles(visibles, newVisibles, chunk)
		t := visibles[:0]
		visibles = newVisibles
		newVisibles = t

		logPrintf("add", visibles)

	}

	return
}

// find non-overlapping visible intervals
// visible interval map to one file chunk

type VisibleInterval struct {
	start        int64
	stop         int64
	modifiedTime int64
	fileId       string
	isFullChunk  bool
}

func newVisibleInterval(start, stop int64, fileId string, modifiedTime int64, isFullChunk bool) VisibleInterval {
	return VisibleInterval{
		start:        start,
		stop:         stop,
		fileId:       fileId,
		modifiedTime: modifiedTime,
		isFullChunk:  isFullChunk,
	}
}

func min(x, y int64) int64 {
	if x <= y {
		return x
	}
	return y
}
