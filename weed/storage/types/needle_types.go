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

// IsDeleted checks if the needle entry has been marked as deleted (tombstoned).
// Use this when checking if an entry should exist in the needle map.
// Returns true for negative sizes or TombstoneFileSize.
// Note: size=0 is NOT considered deleted - it's an anomalous but active entry.
func (s Size) IsDeleted() bool {
	return s < 0 || s == TombstoneFileSize
}

// IsValid checks if the needle has actual readable data.
// Use this when checking if needle data can be read or when counting data bytes.
// Returns true only for positive sizes (size > 0).
// Note: size=0 returns false - such needles exist in the map but have no readable data.
func (s Size) IsValid() bool {
	return s > 0 && s != TombstoneFileSize
}

// Raw returns the raw storage size for a needle, regardless of its deleted status.
func (s Size) Raw() uint32 {
	if s == TombstoneFileSize {
		return 0
	}
	if s < 0 {
		return uint32((-1) * s)
	}
	return uint32(s)
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
