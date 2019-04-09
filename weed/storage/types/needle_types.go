package types

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"math"
	"strconv"
)

type Offset struct {
	OffsetHigher
	OffsetLower
}

type OffsetHigher struct {
	// b4 byte
}

type OffsetLower struct {
	b3 byte
	b2 byte
	b1 byte
	b0 byte // the smaller byte
}

type Cookie uint32

const (
	OffsetSize            = 4 // + 1
	SizeSize              = 4 // uint32 size
	NeedleEntrySize       = CookieSize + NeedleIdSize + SizeSize
	TimestampSize         = 8 // int64 size
	NeedlePaddingSize     = 8
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	TombstoneFileSize     = math.MaxUint32
	CookieSize            = 4
)

func CookieToBytes(bytes []byte, cookie Cookie) {
	util.Uint32toBytes(bytes, uint32(cookie))
}
func Uint32ToCookie(cookie uint32) Cookie {
	return Cookie(cookie)
}

func BytesToCookie(bytes []byte) Cookie {
	return Cookie(util.BytesToUint32(bytes[0:4]))
}

func ParseCookie(cookieString string) (Cookie, error) {
	cookie, err := strconv.ParseUint(cookieString, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("needle cookie %s format error: %v", cookieString, err)
	}
	return Cookie(cookie), nil
}

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

func (offset Offset) ToAcutalOffset() (actualOffset int64) {
	return (int64(offset.b0) + int64(offset.b1)<<8 + int64(offset.b2)<<16 + int64(offset.b3)<<24) * int64(NeedlePaddingSize)
}

func (offset Offset) String() string {
	return fmt.Sprintf("%d", int64(offset.b0)+int64(offset.b1)<<8+int64(offset.b2)<<16+int64(offset.b3)<<24)
}
