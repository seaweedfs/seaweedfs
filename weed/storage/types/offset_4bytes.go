//go:build !5BytesOffset
// +build !5BytesOffset

package types

import (
	"fmt"
)

type OffsetHigher struct {
	// b4 byte
}

const (
	OffsetSize                   = 4
	MaxPossibleVolumeSize uint64 = 4 * 1024 * 1024 * 1024 * 8 // 32GB
)

func OffsetToBytes(bytes []byte, offset Offset) {
	bytes[3] = offset.b0
	bytes[2] = offset.b1
	bytes[1] = offset.b2
	bytes[0] = offset.b3
}

// only for testing, will be removed later.
func Uint32ToOffset(offset uint32) Offset {
	return Offset{
		OffsetLower: OffsetLower{
			b0: byte(offset),
			b1: byte(offset >> 8),
			b2: byte(offset >> 16),
			b3: byte(offset >> 24),
		},
	}
}

func BytesToOffset(bytes []byte) Offset {
	return Offset{
		OffsetLower: OffsetLower{
			b0: bytes[3],
			b1: bytes[2],
			b2: bytes[1],
			b3: bytes[0],
		},
	}
}

func (offset Offset) IsZero() bool {
	return offset.b0 == 0 && offset.b1 == 0 && offset.b2 == 0 && offset.b3 == 0
}

func ToOffset(offset int64) Offset {
	smaller := uint32(offset / int64(NeedlePaddingSize))
	return Uint32ToOffset(smaller)
}

func (offset Offset) ToActualOffset() (actualOffset int64) {
	return (int64(offset.b0) + int64(offset.b1)<<8 + int64(offset.b2)<<16 + int64(offset.b3)<<24) * int64(NeedlePaddingSize)
}

func (offset Offset) String() string {
	return fmt.Sprintf("%d", int64(offset.b0)+int64(offset.b1)<<8+int64(offset.b2)<<16+int64(offset.b3)<<24)
}
