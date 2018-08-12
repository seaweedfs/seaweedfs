// +build 128BitNeedleId

package types

import (
	"encoding/hex"
)

const (
	NeedleIdSize  = 16
	NeedleIdEmpty = ""
)

// this is a 128 bit needle id implementation.
// Usually a FileId has 32bit volume id, 64bit needle id, 32 bit cookie.
// But when your system is using UUID, which is 128 bit, a custom 128-bit needle id can be easier to manage.
// Caveat: In this mode, the fildId from master /dir/assign can not be directly used.
// Only the volume id and cookie from the fileId are usuable.
type NeedleId string

func NeedleIdToBytes(bytes []byte, needleId NeedleId) {
	hex.Decode(bytes, []byte(needleId))
}

// NeedleIdToUint64 used to send max needle id to master
func NeedleIdToUint64(needleId NeedleId) uint64 {
	return 0
}

func Uint64ToNeedleId(needleId uint64) NeedleId {
	return NeedleId("")
}

func BytesToNeedleId(bytes []byte) (needleId NeedleId) {
	return NeedleId(hex.EncodeToString(bytes))
}

func (k NeedleId) String() string {
	return string(k)
}

func ParseNeedleId(idString string) (NeedleId, error) {
	return NeedleId(idString), nil
}
