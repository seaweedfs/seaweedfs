package storage

import (
	"errors"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type FileId struct {
	VolumeId VolumeId
	Key      uint64
	Hashcode uint32
}

func NewFileIdFromNeedle(VolumeId VolumeId, n *Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Hashcode: n.Cookie}
}
func NewFileId(VolumeId VolumeId, Key uint64, Hashcode uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Key, Hashcode: Hashcode}
}
func ParseFileId(fid string) (*FileId, error) {
	var a []string
	if strings.Contains(fid, ",") {
		a = strings.Split(fid, ",")
	} else {
		a = strings.Split(fid, "/")
	}
	if len(a) != 2 {
		glog.V(1).Infoln("Invalid fid ", fid, ", split length ", len(a))
		return nil, errors.New("Invalid fid " + fid)
	}

	vid_string, key_hash_string := a[0], a[1]
	volumeId, _ := NewVolumeId(vid_string)
	key, hash, e := ParseIdCookie(key_hash_string)
	return &FileId{VolumeId: volumeId, Key: key, Hashcode: hash}, e
}

func (n *FileId) String() string {
	return n.VolumeId.String() + "," + n.Nid()
}

func (n *FileId) Nid() string {
	return ToNid(n.Key, n.Hashcode)
}
