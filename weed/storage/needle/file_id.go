package needle

import (
	"encoding/hex"
	"fmt"
	"strings"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
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

// Deserialize the file id
func ParseFileIdFromString(fid string) (*FileId, error) {
	vid, needleKeyCookie, err := splitVolumeId(fid)
	if err != nil {
		return nil, err
	}
	volumeId, err := NewVolumeId(vid)
	if err != nil {
		return nil, err
	}

	nid, cookie, err := ParseNeedleIdCookie(needleKeyCookie)
	if err != nil {
		return nil, err
	}
	fileId := &FileId{VolumeId: volumeId, Key: nid, Cookie: cookie}
	return fileId, nil
}

func (n *FileId) GetVolumeId() VolumeId {
	return n.VolumeId
}

func (n *FileId) GetNeedleId() NeedleId {
	return n.Key
}

func (n *FileId) GetCookie() Cookie {
	return n.Cookie
}

func (n *FileId) GetNeedleIdCookie() string {
	return formatNeedleIdCookie(n.Key, n.Cookie)
}

func (n *FileId) String() string {
	return n.VolumeId.String() + "," + formatNeedleIdCookie(n.Key, n.Cookie)
}

func formatNeedleIdCookie(key NeedleId, cookie Cookie) string {
	bytes := make([]byte, NeedleIdSize+CookieSize)
	NeedleIdToBytes(bytes[0:NeedleIdSize], key)
	CookieToBytes(bytes[NeedleIdSize:NeedleIdSize+CookieSize], cookie)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0 && nonzero_index < NeedleIdSize; nonzero_index++ {
	}
	return hex.EncodeToString(bytes[nonzero_index:])
}

// copied from operation/delete_content.go, to cut off cycle dependency
func splitVolumeId(fid string) (vid string, key_cookie string, err error) {
	commaIndex := strings.Index(fid, ",")
	if commaIndex <= 0 {
		return "", "", fmt.Errorf("wrong fid format")
	}
	return fid[:commaIndex], fid[commaIndex+1:], nil
}
