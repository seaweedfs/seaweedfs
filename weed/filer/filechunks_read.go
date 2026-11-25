package filer

import (
	"container/list"
	"slices"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func readResolvedChunks(chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *IntervalList[*VisibleInterval]) {

	// Allocate all points at once. pointStore is never grown, so the pointers stay valid.
	pointStore := make([]Point, 0, 2*len(chunks))
	points := make([]*Point, 0, 2*len(chunks))
	for i, chunk := range chunks {
		if chunk.IsChunkManifest {
			println("This should not happen! A manifest chunk found:", chunk.GetFileIdString())
		}
		start, stop := max(chunk.Offset, startOffset), min(chunk.Offset+int64(chunk.Size), stopOffset)
		if start >= stop {
			continue
		}
		pointStore = append(pointStore, Point{
			x:       chunk.Offset,
			ts:      chunk.ModifiedTsNs,
			order:   i,
			chunk:   chunk,
			isStart: true,
		}, Point{
			x:       chunk.Offset + int64(chunk.Size),
			ts:      chunk.ModifiedTsNs,
			order:   i,
			chunk:   chunk,
			isStart: false,
		})
		points = append(points, &pointStore[len(pointStore)-2], &pointStore[len(pointStore)-1])
	}
	slices.SortFunc(points, func(a, b *Point) int {
		if a.x != b.x {
			return int(a.x - b.x)
		}
		if a.ts != b.ts {
			return int(a.ts - b.ts)
		}
		if a.order != b.order {
			return a.order - b.order
		}
		if a.isStart {
			return 1
		}
		if b.isStart {
			return -1
		}
		return 0
	})

	var prevX int64
	queue := list.New() // points with higher ts are at the tail
	visibles = NewIntervalList[*VisibleInterval]()
	var prevPoint *Point
	for _, point := range points {
		if queue.Len() > 0 {
			prevPoint = queue.Back().Value.(*Point)
		} else {
			prevPoint = nil
		}
		if point.isStart {
			if prevPoint != nil {
				if point.x != prevX && point.newerThan(prevPoint) {
					addToVisibles(visibles, prevX, prevPoint, point)
					prevX = point.x
				}
			}
			// insert into queue
			if prevPoint == nil || point.newerThan(prevPoint) {
				queue.PushBack(point)
				prevX = point.x
			} else {
				for e := queue.Front(); e != nil; e = e.Next() {
					if e.Value.(*Point).newerThan(point) {
						queue.InsertBefore(point, e)
						break
					}
				}
			}
		} else {
			isLast := true
			for e := queue.Back(); e != nil; e = e.Prev() {
				if e.Value.(*Point).order == point.order {
					queue.Remove(e)
					break
				}
				isLast = false
			}
			if isLast && prevPoint != nil {
				addToVisibles(visibles, prevX, prevPoint, point)
				prevX = point.x
			}
		}
	}

	return
}

func addToVisibles(visibles *IntervalList[*VisibleInterval], prevX int64, startPoint *Point, point *Point) {
	if prevX < point.x {
		chunk := startPoint.chunk
		visible := &VisibleInterval{
			start:         prevX,
			stop:          point.x,
			fileId:        chunk.GetFileIdString(),
			modifiedTsNs:  chunk.ModifiedTsNs,
			offsetInChunk: prevX - chunk.Offset,
			chunkSize:     chunk.Size,
			cipherKey:     chunk.CipherKey,
			isGzipped:     chunk.IsCompressed,
		}
		appendVisibleInterfal(visibles, visible)
	}
}

func appendVisibleInterfal(visibles *IntervalList[*VisibleInterval], visible *VisibleInterval) {
	visibles.AppendInterval(&Interval[*VisibleInterval]{
		StartOffset: visible.start,
		StopOffset:  visible.stop,
		TsNs:        visible.modifiedTsNs,
		Value:       visible,
	})
}

type Point struct {
	x       int64
	ts      int64
	order   int
	chunk   *filer_pb.FileChunk
	isStart bool
}

// newerThan breaks a ts tie by position in the chunk list, so the later chunk
// wins, as it does in MergeIntoVisibles.
func (p *Point) newerThan(o *Point) bool {
	if p.ts != o.ts {
		return p.ts > o.ts
	}
	return p.order > o.order
}
