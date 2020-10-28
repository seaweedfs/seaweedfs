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
	if size < 0 {
		// read the deletion entry
		if lastAppendAtNs, e = verifyDeletedNeedleIntegrity(v.DataBackend, v.Version(), key); e != nil {
			return lastAppendAtNs, fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
		}
	} else {
		if lastAppendAtNs, e = verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToAcutalOffset(), key, size); e != nil {
			return lastAppendAtNs, fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
		}
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

func verifyNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key NeedleId, size Size) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	// case: node total memory 8g, set volumeLimitSize=2048 , save 10 files, every file size 2.2g or more , when we restart the volume server , while see out of memory error
	// fix: When the size of the last file exceeds 10M, consider directly returning the last modify time
	if size > 10 * 1024 * 1024 {
		bytes , err := needle.ReadNeedleBlob(datFile, offset+int64(size), 0, v);
		if  err == nil {
			if v == needle.Version3 {
				tsOffset := NeedleHeaderSize + 0 + needle.NeedleChecksumSize
				n.AppendAtNs = util.BytesToUint64(bytes[tsOffset : tsOffset+TimestampSize])
			}
		}
		return n.AppendAtNs, err
	}
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+int64(size), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}

func verifyDeletedNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, key NeedleId) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	size := n.DiskSize(v)
	var fileSize int64
	fileSize, _, err = datFile.GetStat()
	if err != nil {
		return 0, fmt.Errorf("GetStat: %v", err)
	}
	if err = n.ReadData(datFile, fileSize-size, Size(0), v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", fileSize-size, size, err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}
