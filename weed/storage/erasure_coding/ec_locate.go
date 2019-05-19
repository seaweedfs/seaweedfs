package erasure_coding

type Interval struct {
	blockIndex       int
	innerBlockOffset int64
	size             uint32
	isLargeBlock     bool
}

func locateData(largeBlockLength, smallBlockLength int64, datSize int64, offset int64, size uint32) (intervals []Interval) {
	blockIndex, isLargeBlock, innerBlockOffset := locateOffset(largeBlockLength, smallBlockLength, datSize, offset)

	nLargeBlockRows := int(datSize / (largeBlockLength * DataShardsCount))

	for size > 0 {
		interval := Interval{
			blockIndex:       blockIndex,
			innerBlockOffset: innerBlockOffset,
			isLargeBlock:     isLargeBlock,
		}

		blockRemaining := largeBlockLength - innerBlockOffset
		if !isLargeBlock {
			blockRemaining = smallBlockLength - innerBlockOffset
		}

		if int64(size) <= blockRemaining {
			interval.size = size
			intervals = append(intervals, interval)
			return
		}
		interval.size = uint32(blockRemaining)
		intervals = append(intervals, interval)

		size -= interval.size
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
