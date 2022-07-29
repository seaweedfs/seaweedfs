package types

import (
	"fmt"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Offset struct {
	OffsetHigher
	OffsetLower
}

type Size int32

func (s Size) IsDeleted() bool {
	return s < 0 || s == TombstoneFileSize
}
func (s Size) IsValid() bool {
	return s > 0 && s != TombstoneFileSize
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
	DataSizeSize       = 4
	NeedleMapEntrySize = NeedleIdSize + OffsetSize + SizeSize
	TimestampSize      = 8 // int64 size
	NeedlePaddingSize  = 8
	TombstoneFileSize  = Size(-1)
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

func BytesToSize(bytes []byte) Size {
	return Size(util.BytesToUint32(bytes))
}

func SizeToBytes(bytes []byte, size Size) {
	util.Uint32toBytes(bytes, uint32(size))
}
