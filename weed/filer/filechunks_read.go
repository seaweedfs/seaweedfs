package filer

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"golang.org/x/exp/slices"
)

func readResolvedChunks(chunks []*filer_pb.FileChunk) (visibles []VisibleInterval) {

	var points []*Point
	for _, chunk := range chunks {
		points = append(points, &Point{
			x:       chunk.Offset,
			ts:      chunk.Mtime,
			chunk:   chunk,
			isStart: true,
		})
		points = append(points, &Point{
			x:       chunk.Offset + int64(chunk.Size),
			ts:      chunk.Mtime,
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
	var queue []*Point
	for _, point := range points {
		if point.isStart {
			if len(queue) > 0 {
				lastIndex := len(queue) - 1
				lastPoint := queue[lastIndex]
				if point.x != prevX && lastPoint.ts < point.ts {
					visibles = addToVisibles(visibles, prevX, lastPoint, point)
					prevX = point.x
				}
			}
			// insert into queue
			for i := len(queue); i >= 0; i-- {
				if i == 0 || queue[i-1].ts <= point.ts {
					if i == len(queue) {
						prevX = point.x
					}
					queue = addToQueue(queue, i, point)
					break
				}
			}
		} else {
			lastIndex := len(queue) - 1
			index := lastIndex
			var startPoint *Point
			for ; index >= 0; index-- {
				startPoint = queue[index]
				if startPoint.ts == point.ts {
					queue = removeFromQueue(queue, index)
					break
				}
			}
			if index == lastIndex && startPoint != nil {
				visibles = addToVisibles(visibles, prevX, startPoint, point)
				prevX = point.x
			}
		}
	}

	return
}

func removeFromQueue(queue []*Point, index int) []*Point {
	for i := index; i < len(queue)-1; i++ {
		queue[i] = queue[i+1]
	}
	queue = queue[:len(queue)-1]
	return queue
}

func addToQueue(queue []*Point, index int, point *Point) []*Point {
	queue = append(queue, point)
	for i := len(queue) - 1; i > index; i-- {
		queue[i], queue[i-1] = queue[i-1], queue[i]
	}
	return queue
}

func addToVisibles(visibles []VisibleInterval, prevX int64, startPoint *Point, point *Point) []VisibleInterval {
	if prevX < point.x {
		chunk := startPoint.chunk
		visibles = append(visibles, VisibleInterval{
			start:        prevX,
			stop:         point.x,
			fileId:       chunk.GetFileIdString(),
			modifiedTime: chunk.Mtime,
			chunkOffset:  prevX - chunk.Offset,
			chunkSize:    chunk.Size,
			cipherKey:    chunk.CipherKey,
			isGzipped:    chunk.IsCompressed,
		})
	}
	return visibles
}

type Point struct {
	x       int64
	ts      int64
	chunk   *filer_pb.FileChunk
	isStart bool
}
