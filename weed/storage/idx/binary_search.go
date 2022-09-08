package idx

import (
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// LastValidIndex find the last index that satisfy the validation function's requirement.
func LastValidIndex(bytes []byte, indexLength int, validation func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error)) (int, error) {
	left, right := 0, indexLength/types.NeedleMapEntrySize-1
	index := -1
	for left <= right {
		mid := left + (right-left)>>1
		loc := mid * types.NeedleMapEntrySize
		key := types.BytesToNeedleId(bytes[loc:types.NeedleIdSize])
		offset := types.BytesToOffset(bytes[loc+types.NeedleIdSize : loc+types.NeedleIdSize+types.OffsetSize])
		size := types.BytesToSize(bytes[loc+types.NeedleIdSize+types.OffsetSize : loc+types.NeedleIdSize+types.OffsetSize+types.SizeSize])
		res, err := validation(key, offset, size)
		if err != nil {
			return -1, err
		}
		if res {
			index = mid
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	if index == -1 {
		return 0, errors.New("no valid record")
	}
	return index, nil
}
