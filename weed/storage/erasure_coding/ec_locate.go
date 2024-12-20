package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type Interval struct {
	BlockIndex          int // the index of the block in either the large blocks or the small blocks
	InnerBlockOffset    int64
	Size                types.Size
	IsLargeBlock        bool // whether the block is a large block or a small block
	LargeBlockRowsCount int
}

func LocateData(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64, size types.Size) (intervals []Interval) {
	blockIndex, isLargeBlock, nLargeBlockRows, innerBlockOffset := locateOffset(largeBlockLength, smallBlockLength, shardDatSize, offset)

	for size > 0 {
		interval := Interval{
			BlockIndex:          blockIndex,
			InnerBlockOffset:    innerBlockOffset,
			IsLargeBlock:        isLargeBlock,
			LargeBlockRowsCount: int(nLargeBlockRows),
		}

		blockRemaining := largeBlockLength - innerBlockOffset
		if !isLargeBlock {
			blockRemaining = smallBlockLength - innerBlockOffset
		}

		if int64(size) <= blockRemaining {
			interval.Size = size
			intervals = append(intervals, interval)
			return
		}
		interval.Size = types.Size(blockRemaining)
		intervals = append(intervals, interval)

		size -= interval.Size
		blockIndex += 1
		if isLargeBlock && blockIndex == interval.LargeBlockRowsCount*DataShardsCount {
			isLargeBlock = false
			blockIndex = 0
		}
		innerBlockOffset = 0

	}
	return
}

func locateOffset(largeBlockLength, smallBlockLength int64, shardDatSize int64, offset int64) (blockIndex int, isLargeBlock bool, nLargeBlockRows int64, innerBlockOffset int64) {
	largeRowSize := largeBlockLength * DataShardsCount
	nLargeBlockRows = (shardDatSize - 1) / largeBlockLength

	// if offset is within the large block area
	if offset < nLargeBlockRows*largeRowSize {
		isLargeBlock = true
		blockIndex, innerBlockOffset = locateOffsetWithinBlocks(largeBlockLength, offset)
		return
	}

	isLargeBlock = false
	offset -= nLargeBlockRows * largeRowSize
	blockIndex, innerBlockOffset = locateOffsetWithinBlocks(smallBlockLength, offset)
	return
}

func locateOffsetWithinBlocks(blockLength int64, offset int64) (blockIndex int, innerBlockOffset int64) {
	blockIndex = int(offset / blockLength)
	innerBlockOffset = offset % blockLength
	return
}

func (interval Interval) ToShardIdAndOffset(largeBlockSize, smallBlockSize int64) (ShardId, int64) {
	ecFileOffset := interval.InnerBlockOffset
	rowIndex := interval.BlockIndex / DataShardsCount
	if interval.IsLargeBlock {
		ecFileOffset += int64(rowIndex) * largeBlockSize
	} else {
		ecFileOffset += int64(interval.LargeBlockRowsCount)*largeBlockSize + int64(rowIndex)*smallBlockSize
	}
	ecFileIndex := interval.BlockIndex % DataShardsCount
	return ShardId(ecFileIndex), ecFileOffset
}
