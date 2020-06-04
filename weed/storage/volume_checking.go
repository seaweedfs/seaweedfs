package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, e error) {
	var indexSize int64
	if indexSize, e = verifyIndexFileIntegrity(indexFile); e != nil {
		return 0, fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), e)
	}
	if indexSize == 0 {
		return 0, nil
	}
	var lastIdxEntry []byte
	if lastIdxEntry, e = readIndexEntryAtOffset(indexFile, indexSize-NeedleMapEntrySize); e != nil {
		return 0, fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), e)
	}
	key, offset, size := idx.IdxFileEntry(lastIdxEntry)
	if offset.IsZero() {
		return 0, nil
	}
	if size == TombstoneFileSize {
		size = 0
	}
	if lastAppendAtNs, e = verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToAcutalOffset(), key, size); e != nil {
		return lastAppendAtNs, fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
	}
	return
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%NeedleMapEntrySize != 0 {
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 {
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	bytes = make([]byte, NeedleMapEntrySize)
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

func verifyNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key NeedleId, size uint32) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+int64(size), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}
