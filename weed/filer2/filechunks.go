package filer2

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"sort"
	"log"
	"math"
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
	return
}

func mergeToVisibleIntervals(visibles []*visibleInterval, chunk *filer_pb.FileChunk) (merged []*visibleInterval) {
	if len(visibles) == 0 {
		return []*visibleInterval{newVisibleInterval(chunk.Offset, chunk.Offset+int64(chunk.Size), chunk.FileId, chunk.Mtime)}
	}

	log.Printf("merge chunk %+v => %d", chunk, len(visibles))
	for _, v := range visibles {
		log.Printf("=> %+v", v)
	}

	var nonOverlappingStop int

	// find merge candidates
	var mergeCandidates []int
	for t := len(visibles) - 1; t >= 0; t-- {
		if visibles[t].stop > chunk.Offset {
			mergeCandidates = append(mergeCandidates, t)
		} else {
			nonOverlappingStop = t
			break
		}
	}
	log.Printf("merged candidates: %+v, starting from %d", mergeCandidates, nonOverlappingStop)

	if len(mergeCandidates) == 0 {
		merged = append(visibles, newVisibleInterval(
			chunk.Offset,
			chunk.Offset+int64(chunk.Size),
			chunk.FileId,
			chunk.Mtime,
		))
		return
	}

	// reverse merge candidates
	i, j := 0, len(mergeCandidates)-1
	for i < j {
		mergeCandidates[i], mergeCandidates[j] = mergeCandidates[j], mergeCandidates[i]
		i++
		j--
	}
	log.Printf("reversed merged candidates: %+v", mergeCandidates)

	// add chunk into a possibly connected intervals
	var overlappingIntevals []*visibleInterval
	for i = 0; i < len(mergeCandidates); i++ {
		interval := visibles[mergeCandidates[i]]
		if interval.modifiedTime >= chunk.Mtime {
			log.Printf("overlappingIntevals add existing interval: [%d,%d)", interval.start, interval.stop)
			overlappingIntevals = append(overlappingIntevals, interval)
		} else {
			start := max(interval.start, chunk.Offset)
			stop := min(interval.stop, chunk.Offset+int64(chunk.Size))
			if interval.start <= chunk.Offset {
				if interval.start < start {
					log.Printf("overlappingIntevals add 1: [%d,%d)", interval.start, start)
					overlappingIntevals = append(overlappingIntevals, newVisibleInterval(
						interval.start,
						start,
						interval.fileId,
						interval.modifiedTime,
					))
				}
				log.Printf("overlappingIntevals add 2: [%d,%d)", start, stop)
				overlappingIntevals = append(overlappingIntevals, newVisibleInterval(
					start,
					stop,
					chunk.FileId,
					chunk.Mtime,
				))
				if interval.stop < stop {
					log.Printf("overlappingIntevals add 3: [%d,%d)", interval.stop, stop)
					overlappingIntevals = append(overlappingIntevals, newVisibleInterval(
						interval.stop,
						stop,
						interval.fileId,
						interval.modifiedTime,
					))
				}
			}
		}
	}
	logPrintf("overlappingIntevals", overlappingIntevals)

	// merge connected intervals
	merged = visibles[:nonOverlappingStop]
	var lastInterval *visibleInterval
	var prevIntervalIndex int
	for i, interval := range overlappingIntevals {
		if i == 0 {
			prevIntervalIndex = i
			continue
		}
		if overlappingIntevals[prevIntervalIndex].fileId != interval.fileId {
			merged = append(merged, newVisibleInterval(
				overlappingIntevals[prevIntervalIndex].start,
				interval.start,
				overlappingIntevals[prevIntervalIndex].fileId,
				overlappingIntevals[prevIntervalIndex].modifiedTime,
			))
			prevIntervalIndex = i
		}
	}

	if lastInterval != nil {
		merged = append(merged, newVisibleInterval(
			overlappingIntevals[prevIntervalIndex].start,
			lastInterval.start,
			overlappingIntevals[prevIntervalIndex].fileId,
			overlappingIntevals[prevIntervalIndex].modifiedTime,
		))
	}

	logPrintf("merged", merged)

	return
}

func logPrintf(name string, visibles []*visibleInterval) {
	log.Printf("%s len %d", name, len(visibles))
	for _, v := range visibles {
		log.Printf("%s:  => %+v", name, v)
	}
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
		log.Printf("checking chunk: [%d,%d)", chunk.Offset, chunk.Offset+int64(chunk.Size))
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
					remaining = append(remaining, newVisibleInterval(
						interval.start,
						interval.stop,
						interval.fileId,
						interval.modifiedTime,
					))
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
				remaining = append(remaining, newVisibleInterval(
					interval.start,
					interval.stop,
					interval.fileId,
					interval.modifiedTime,
				))
			}
		}
		parallelIntervals = remaining
	}
	logPrintf("parallelIntervals loop 4", parallelIntervals)
	logPrintf("intervals", intervals)

	// merge connected intervals, now the intervals are non-intersecting
	var lastInterval *visibleInterval
	var prevIntervalIndex int
	for i, interval := range intervals {
		if i == 0 {
			prevIntervalIndex = i
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
		lastInterval = intervals[i]
		logPrintf("intervals loop 1 visibles", visibles)
	}

	if lastInterval != nil {
		visibles = append(visibles, newVisibleInterval(
			intervals[prevIntervalIndex].start,
			lastInterval.stop,
			intervals[prevIntervalIndex].fileId,
			intervals[prevIntervalIndex].modifiedTime,
		))
	}

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

func nonOverlappingVisibleIntervals0(chunks []*filer_pb.FileChunk) (visibles []*visibleInterval) {

	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].Offset < chunks[j].Offset {
			return true
		}
		if chunks[i].Offset == chunks[j].Offset {
			return chunks[i].Mtime < chunks[j].Mtime
		}
		return false
	})

	for _, c := range chunks {
		visibles = mergeToVisibleIntervals(visibles, c)
	}

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

type stackOfChunkIds struct {
	ids []int
}

func (s *stackOfChunkIds) isEmpty() bool {
	return len(s.ids) == 0
}

func (s *stackOfChunkIds) pop() int {
	t := s.ids[len(s.ids)-1]
	s.ids = s.ids[:len(s.ids)-1]
	return t
}

func (s *stackOfChunkIds) push(x int) {
	s.ids = append(s.ids, x)
}

func (s *stackOfChunkIds) peek() int {
	return s.ids[len(s.ids)-1]
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
