package types

import (
	"math"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strconv"
	"fmt"
)

type NeedleId uint64
type Offset uint32
type Cookie uint32

const (
	NeedleIdSize          = 8
	OffsetSize            = 4
	SizeSize              = 4 // uint32 size
	NeedleEntrySize       = NeedleIdSize + OffsetSize + SizeSize
	NeedlePaddingSize     = 8
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	TombstoneFileSize     = math.MaxUint32
	CookieSize            = 4
)

func NeedleIdToBytes(bytes []byte, needleId NeedleId) {
	util.Uint64toBytes(bytes, uint64(needleId))
}

// NeedleIdToUint64 used to send max needle id to master
func NeedleIdToUint64(needleId NeedleId) uint64 {
	return uint64(needleId)
}

func Uint64ToNeedleId(needleId uint64) (NeedleId) {
	return NeedleId(needleId)
}

func BytesToNeedleId(bytes []byte) (NeedleId) {
	return NeedleId(util.BytesToUint64(bytes))
}

func CookieToBytes(bytes []byte, cookie Cookie) {
	util.Uint32toBytes(bytes, uint32(cookie))
}
func Uint32ToCookie(cookie uint32) (Cookie) {
	return Cookie(cookie)
}

func BytesToCookie(bytes []byte) (Cookie) {
	return Cookie(util.BytesToUint32(bytes[0:4]))
}

func OffsetToBytes(bytes []byte, offset Offset) {
	util.Uint32toBytes(bytes, uint32(offset))
}

func Uint32ToOffset(offset uint32) (Offset) {
	return Offset(offset)
}

func BytesToOffset(bytes []byte) (Offset) {
	return Offset(util.BytesToUint32(bytes[0:4]))
}

func (k NeedleId) String() string {
	return strconv.FormatUint(uint64(k), 10)
}

func ParseNeedleId(idString string) (NeedleId, error) {
	key, err := strconv.ParseUint(idString, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("needle id %s format error: %v", idString, err)
	}
	return NeedleId(key), nil
}

func ParseCookie(cookieString string) (Cookie, error) {
	cookie, err := strconv.ParseUint(cookieString, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("needle cookie %s format error: %v", cookieString, err)
	}
	return Cookie(cookie), nil
}
