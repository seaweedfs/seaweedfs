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

type OffsetLower struct {
	b3 byte
	b2 byte
	b1 byte
	b0 byte // the smaller byte
}

type Cookie uint32

const (
	SizeSize           = 4 // uint32 size
	NeedleHeaderSize   = CookieSize + NeedleIdSize + SizeSize
	NeedleMapEntrySize = NeedleIdSize + OffsetSize + SizeSize
	TimestampSize      = 8 // int64 size
	NeedlePaddingSize  = 8
	TombstoneFileSize  = math.MaxUint32
	CookieSize         = 4
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
