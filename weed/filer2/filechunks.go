package filer2

import (
	"math"
	"sort"

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

func CompactFileChunks(chunks []*filer_pb.FileChunk) (compacted, garbage []*filer_pb.FileChunk) {

	visibles := nonOverlappingVisibleIntervals(chunks)

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
}

func ViewFromChunks(chunks []*filer_pb.FileChunk, offset int64, size int) (views []*ChunkView) {

	visibles := nonOverlappingVisibleIntervals(chunks)

	stop := offset + int64(size)

	for _, chunk := range visibles {
		if chunk.start <= offset && offset < chunk.stop && offset < stop {
			views = append(views, &ChunkView{
				FileId:      chunk.fileId,
				Offset:      offset - chunk.start, // offset is the data starting location in this file id
				Size:        uint64(min(chunk.stop, stop) - offset),
				LogicOffset: offset,
			})
			offset = min(chunk.stop, stop)
		}
	}

	return views

}

func logPrintf(name string, visibles []*visibleInterval) {
	/*
		log.Printf("%s len %d", name, len(visibles))
		for _, v := range visibles {
			log.Printf("%s:  => %+v", name, v)
		}
	*/
}

func nonOverlappingVisibleIntervals(chunks []*filer_pb.FileChunk) (visibles []*visibleInterval) {

	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Offset < chunks[j].Offset {
			return true
		}
		if chunks[i].Offset == chunks[j].Offset {
			return chunks[i].Mtime < chunks[j].Mtime
		}
		return false
	})

	if len(chunks) == 0 {
		return
	}

	var parallelIntervals, intervals []*visibleInterval
	var minStopInterval, upToDateInterval *visibleInterval
	watermarkStart := chunks[0].Offset
	for _, chunk := range chunks {
		// log.Printf("checking chunk: [%d,%d)", chunk.Offset, chunk.Offset+int64(chunk.Size))
		logPrintf("parallelIntervals", parallelIntervals)
		for len(parallelIntervals) > 0 && watermarkStart < chunk.Offset {
			logPrintf("parallelIntervals loop 1", parallelIntervals)
			logPrintf("parallelIntervals loop 1 intervals", intervals)
			minStopInterval, upToDateInterval = findMinStopInterval(parallelIntervals)
			nextStop := min(minStopInterval.stop, chunk.Offset)
			intervals = append(intervals, newVisibleInterval(
				max(watermarkStart, minStopInterval.start),
				nextStop,
				upToDateInterval.fileId,
				upToDateInterval.modifiedTime,
			))
			watermarkStart = nextStop
			logPrintf("parallelIntervals loop intervals =>", intervals)

			// remove processed intervals, possibly multiple
			var remaining []*visibleInterval
			for _, interval := range parallelIntervals {
				if interval.stop != watermarkStart {
					remaining = append(remaining, interval)
				}
			}
			parallelIntervals = remaining
			logPrintf("parallelIntervals loop 2", parallelIntervals)
			logPrintf("parallelIntervals loop 2 intervals", intervals)
		}
		parallelIntervals = append(parallelIntervals, newVisibleInterval(
			chunk.Offset,
			chunk.Offset+int64(chunk.Size),
			chunk.FileId,
			chunk.Mtime,
		))
	}

	logPrintf("parallelIntervals loop 3", parallelIntervals)
	logPrintf("parallelIntervals loop 3 intervals", intervals)
	for len(parallelIntervals) > 0 {
		minStopInterval, upToDateInterval = findMinStopInterval(parallelIntervals)
		intervals = append(intervals, newVisibleInterval(
			max(watermarkStart, minStopInterval.start),
			minStopInterval.stop,
			upToDateInterval.fileId,
			upToDateInterval.modifiedTime,
		))
		watermarkStart = minStopInterval.stop

		// remove processed intervals, possibly multiple
		var remaining []*visibleInterval
		for _, interval := range parallelIntervals {
			if interval.stop != watermarkStart {
				remaining = append(remaining, interval)
			}
		}
		parallelIntervals = remaining
	}
	logPrintf("parallelIntervals loop 4", parallelIntervals)
	logPrintf("intervals", intervals)

	// merge connected intervals, now the intervals are non-intersecting
	var lastIntervalIndex int
	var prevIntervalIndex int
	for i, interval := range intervals {
		if i == 0 {
			prevIntervalIndex = i
			lastIntervalIndex = i
			continue
		}
		if intervals[i-1].fileId != interval.fileId ||
			intervals[i-1].stop < intervals[i].start {
			visibles = append(visibles, newVisibleInterval(
				intervals[prevIntervalIndex].start,
				intervals[i-1].stop,
				intervals[prevIntervalIndex].fileId,
				intervals[prevIntervalIndex].modifiedTime,
			))
			prevIntervalIndex = i
		}
		lastIntervalIndex = i
		logPrintf("intervals loop 1 visibles", visibles)
	}

	visibles = append(visibles, newVisibleInterval(
		intervals[prevIntervalIndex].start,
		intervals[lastIntervalIndex].stop,
		intervals[prevIntervalIndex].fileId,
		intervals[prevIntervalIndex].modifiedTime,
	))

	logPrintf("visibles", visibles)

	return
}

func findMinStopInterval(intervals []*visibleInterval) (minStopInterval, upToDateInterval *visibleInterval) {
	var latestMtime int64
	latestIntervalIndex := 0
	minStop := int64(math.MaxInt64)
	minIntervalIndex := 0
	for i, interval := range intervals {
		if minStop > interval.stop {
			minIntervalIndex = i
			minStop = interval.stop
		}
		if latestMtime < interval.modifiedTime {
			latestMtime = interval.modifiedTime
			latestIntervalIndex = i
		}
	}
	minStopInterval = intervals[minIntervalIndex]
	upToDateInterval = intervals[latestIntervalIndex]
	return
}

// find non-overlapping visible intervals
// visible interval map to one file chunk

type visibleInterval struct {
	start        int64
	stop         int64
	modifiedTime int64
	fileId       string
}

func newVisibleInterval(start, stop int64, fileId string, modifiedTime int64) *visibleInterval {
	return &visibleInterval{start: start, stop: stop, fileId: fileId, modifiedTime: modifiedTime}
}

func min(x, y int64) int64 {
	if x <= y {
		return x
	}
	return y
}

func max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
