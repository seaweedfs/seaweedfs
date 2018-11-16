package types

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"math"
	"strconv"
)

type Offset uint32
type Cookie uint32

const (
	OffsetSize            = 4
	SizeSize              = 4 // uint32 size
	TimestampSize         = 8 // int64 size
	NeedlePaddingSize     = 64
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * NeedlePaddingSize
	TombstoneFileSize     = math.MaxUint32
	CookieSize            = 4
	NeedleEntrySize       = CookieSize + NeedleIdSize + SizeSize
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
	util.Uint32toBytes(bytes, uint32(offset))
}

func Uint32ToOffset(offset uint32) Offset {
	return Offset(offset)
}

func BytesToOffset(bytes []byte) Offset {
	return Offset(util.BytesToUint32(bytes[0:4]))
}
