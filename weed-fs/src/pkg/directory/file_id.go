package directory

import (
	"encoding/hex"
	"storage"
	"strconv"
	"strings"
)

type FileId struct {
	VolumeId uint32
	Key      uint64
	Hashcode uint32
}

func NewFileId(VolumeId uint32, Key uint64, Hashcode uint32) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Key, Hashcode: Hashcode}
}

func ParseFileId(path string) *FileId {
	a := strings.Split(path, ",")
	if len(a) != 2 {
		return nil
	}
	vid_string, key_hash_string := a[0], a[1]
	key_hash_bytes, khe := hex.DecodeString(key_hash_string)
	key_hash_len := len(key_hash_bytes)
	if khe != nil || key_hash_len <= 4 {
		return nil
	}
	vid, _ := strconv.Atoui64(vid_string)
	key := storage.BytesToUint64(key_hash_bytes[0 : key_hash_len-4])
	hash := storage.BytesToUint32(key_hash_bytes[key_hash_len-4 : key_hash_len])
	return &FileId{VolumeId: uint32(vid), Key: key, Hashcode: hash}
}
func (n *FileId) String() string {
	bytes := make([]byte, 12)
	storage.Uint64toBytes(bytes[0:8], n.Key)
	storage.Uint32toBytes(bytes[8:12], n.Hashcode)
	nonzero_index := 0
	for ; bytes[nonzero_index] == 0; nonzero_index++ {
	}
	return strconv.Uitoa64(uint64(n.VolumeId)) + "," + hex.EncodeToString(bytes[nonzero_index:])
}
