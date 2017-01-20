package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/util"
)

func getActualSize(size uint32) int64 {
	padding := NeedlePaddingSize - ((NeedleHeaderSize + size + NeedleChecksumSize) % NeedlePaddingSize)
	return NeedleHeaderSize + int64(size) + NeedleChecksumSize + int64(padding)
}

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (error) {
	var indexSize int64
	var e error
	if indexSize, e = verifyIndexFileIntegrity(indexFile); e != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), e)
	}
	if indexSize == 0 {
		return nil
	}
	var lastIdxEntry []byte
	if lastIdxEntry, e = readIndexEntryAtOffset(indexFile, indexSize-NeedleIndexSize); e != nil {
		return fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), e)
	}
	key, offset, size := idxFileEntry(lastIdxEntry)
	if offset == 0 || size == TombstoneFileSize {
		return nil
	}
	if e = verifyNeedleIntegrity(v.dataFile, v.Version(), int64(offset)*NeedlePaddingSize, key, size); e != nil {
		return fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
	}

	return nil
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%NeedleIndexSize != 0 {
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
	bytes = make([]byte, NeedleIndexSize)
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

func verifyNeedleIntegrity(datFile *os.File, v Version, offset int64, key uint64, size uint32) error {
	n := new(Needle)
	err := n.ReadData(datFile, offset, size, v)
	if err != nil {
		return err
	}
	if n.Id != key {
		return fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return nil
}
