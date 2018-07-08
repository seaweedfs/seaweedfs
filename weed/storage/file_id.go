package storage

import (
	"encoding/hex"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

type FileId struct {
	VolumeId VolumeId
	Key      NeedleId
	Cookie   Cookie
}

func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Cookie: n.Cookie}
}

func NewFileId(VolumeId VolumeId, key uint64, cookie uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Uint64ToNeedleId(key), Cookie: Uint32ToCookie(cookie)}
}

func (n *FileId) String() string {
	bytes := make([]byte, NeedleIdSize+CookieSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], n.Key)
	CookieToBytes(bytes[NeedleIdSize:NeedleIdSize+CookieSize], n.Cookie)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return n.VolumeId.String() + "," + hex.EncodeToString(bytes[nonzero_index:])
}
