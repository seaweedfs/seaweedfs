package idx

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// firstInvalidIndex find the first index that do not satisfy the validation function's requirement.
func FirstInvalidIndex(bytes []byte, validation func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error)) (int, error) {
	left, right := 0, len(bytes)/types.NeedleMapEntrySize-1
	index := right + 1
	for left <= right {
		mid := left + (right-left)>>1
		loc := mid * types.NeedleMapEntrySize
		key := types.BytesToNeedleId(bytes[loc : loc+types.NeedleIdSize])
		offset := types.BytesToOffset(bytes[loc+types.NeedleIdSize : loc+types.NeedleIdSize+types.OffsetSize])
		size := types.BytesToSize(bytes[loc+types.NeedleIdSize+types.OffsetSize : loc+types.NeedleIdSize+types.OffsetSize+types.SizeSize])
		res, err := validation(key, offset, size)
		if err != nil {
			return -1, err
		}
		if res {
			left = mid + 1
		} else {
			index = mid
			right = mid - 1
		}
	}
	return index, nil
}
