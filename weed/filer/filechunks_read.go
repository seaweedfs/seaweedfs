package filer

import (
	"container/list"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"golang.org/x/exp/slices"
)

// readResolvedChunks returns a container.List of VisibleInterval
func readResolvedChunks(chunks []*filer_pb.FileChunk, startOffset int64, stopOffset int64) (visibles *list.List) {

	var points []*Point
	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			println("This should not happen! A manifest chunk found:", chunk.GetFileIdString())
		}
		start, stop := max(chunk.Offset, startOffset), min(chunk.Offset+int64(chunk.Size), stopOffset)
		if start >= stop {
			continue
		}
		points = append(points, &Point{
			x:       chunk.Offset,
			ts:      chunk.ModifiedTsNs,
			chunk:   chunk,
			isStart: true,
		})
		points = append(points, &Point{
			x:       chunk.Offset + int64(chunk.Size),
			ts:      chunk.ModifiedTsNs,
			chunk:   chunk,
			isStart: false,
		})
	}
	slices.SortFunc(points, func(a, b *Point) bool {
		if a.x != b.x {
			return a.x < b.x
		}
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		return !a.isStart
	})

	var prevX int64
	queue := list.New() // points with higher ts are at the tail
	visibles = list.New()
	var prevPoint *Point
	for _, point := range points {
		if queue.Len() > 0 {
			prevPoint = queue.Back().Value.(*Point)
		} else {
			prevPoint = nil
		}
		if point.isStart {
			if prevPoint != nil {
				if point.x != prevX && prevPoint.ts < point.ts {
					visibles = addToVisibles(visibles, prevX, prevPoint, point)
					prevX = point.x
				}
			}
			// insert into queue
			if prevPoint == nil || prevPoint.ts < point.ts {
				queue.PushBack(point)
				prevX = point.x
			} else {
				for e := queue.Front(); e != nil; e = e.Next() {
					if e.Value.(*Point).ts > point.ts {
						queue.InsertBefore(point, e)
						break
					}
				}
			}
		} else {
			isLast := true
			for e := queue.Back(); e != nil; e = e.Prev() {
				if e.Value.(*Point).ts == point.ts {
					queue.Remove(e)
					break
				}
				isLast = false
			}
			if isLast && prevPoint != nil {
				visibles = addToVisibles(visibles, prevX, prevPoint, point)
				prevX = point.x
			}
		}
	}

	return
}

func addToVisibles(visibles *list.List, prevX int64, startPoint *Point, point *Point) *list.List {
	if prevX < point.x {
		chunk := startPoint.chunk
		visible := VisibleInterval{
			start:        prevX,
			stop:         point.x,
			fileId:       chunk.GetFileIdString(),
			modifiedTsNs: chunk.ModifiedTsNs,
			chunkOffset:  prevX - chunk.Offset,
			chunkSize:    chunk.Size,
			cipherKey:    chunk.CipherKey,
			isGzipped:    chunk.IsCompressed,
		}
		visibles.PushBack(visible)
	}
	return visibles
}

type Point struct {
	x       int64
	ts      int64
	chunk   *filer_pb.FileChunk
	isStart bool
}
