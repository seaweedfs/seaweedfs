package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

type Interval struct {
	BlockIndex          int
	InnerBlockOffset    int64
	Size                types.Size
	IsLargeBlock        bool
	LargeBlockRowsCount int
}

func LocateData(largeBlockLength, smallBlockLength int64, datSize int64, offset int64, size types.Size) (intervals []Interval) {
	blockIndex, isLargeBlock, innerBlockOffset := locateOffset(largeBlockLength, smallBlockLength, datSize, offset)

	// adding DataShardsCount*smallBlockLength to ensure we can derive the number of large block size from a shard size
	nLargeBlockRows := int((datSize + DataShardsCount*smallBlockLength) / (largeBlockLength * DataShardsCount))

	for size > 0 {
		interval := Interval{
			BlockIndex:          blockIndex,
			InnerBlockOffset:    innerBlockOffset,
			IsLargeBlock:        isLargeBlock,
			LargeBlockRowsCount: nLargeBlockRows,
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
		if isLargeBlock && blockIndex == nLargeBlockRows*DataShardsCount {
			isLargeBlock = false
			blockIndex = 0
		}
		innerBlockOffset = 0

	}
	return
}

func locateOffset(largeBlockLength, smallBlockLength int64, datSize int64, offset int64) (blockIndex int, isLargeBlock bool, innerBlockOffset int64) {
	largeRowSize := largeBlockLength * DataShardsCount
	nLargeBlockRows := datSize / (largeBlockLength * DataShardsCount)

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
