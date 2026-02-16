package storage

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// openIndex returns a file descriptor for the volume's index, and the index size in bytes.
func (v *Volume) openIndex() (*os.File, int64, error) {
	idxFileName := v.FileName(".idx")
	idxFile, err := os.OpenFile(idxFileName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open IDX file %s for volume %v: %v", idxFileName, v.Id, err)
	}

	idxStat, err := idxFile.Stat()
	if err != nil {
		idxFile.Close()
		return nil, 0, fmt.Errorf("failed to stat IDX file %s for volume %v: %v", idxFileName, v.Id, err)
	}
	if idxStat.Size() == 0 {
		idxFile.Close()
		return nil, 0, fmt.Errorf("zero-size IDX file for volume %v at %s", v.Id, idxFileName)
	}

	return idxFile, idxStat.Size(), nil
}

// ScrubIndex checks the volume's index for issues.
func (v *Volume) ScrubIndex() (int64, []error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	idxFile, idxFileSize, err := v.openIndex()
	if err != nil {
		return 0, []error{err}
	}
	defer idxFile.Close()

	return idx.CheckIndexFile(idxFile, idxFileSize, v.Version())
}

// scrubVolumeData checks a volume content + index for issues.
func (v *Volume) scrubVolumeData(dataFile backend.BackendStorageFile, idxFile *os.File, idxFileSize int64) (int64, []error) {
	// full scrubbing means also scrubbing the index
	var count int64
	_, errs := idx.CheckIndexFile(idxFile, idxFileSize, v.Version())

	// read and check every indexed needle
	var totalRead int64
	version := v.Version()
	err := idx.WalkIndexFile(idxFile, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		// compute the actual size of the needle in disk, including needle header, body and alignment padding.
		actualSize := int64(needle.GetActualSize(size, version))

		// TODO: Needle.ReadData() is currently broken for deleted files, which have a types.Size < 0. Fix
		// so deleted needles get properly scrubbed as well.
		// TODO: idx.WalkIndexFile() returns a size -1 (and actual size of 32 bytes) for deleted needles. We
		// want to scrub deleted needles whenever possible.
		if size.IsDeleted() {
			totalRead += actualSize
			return nil
		}

		n := needle.Needle{}
		if err := n.ReadData(dataFile, offset.ToActualOffset(), size, version); err != nil {
			errs = append(errs, fmt.Errorf("needle %d on volume %d: %v", id, v.Id, err))
		}

		totalRead += actualSize
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	// check total volume file size
	wantSize := totalRead + super_block.SuperBlockSize
	dataSize, _, err := dataFile.GetStat()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to stat data file for volume %d: %v", v.Id, err))
	} else {
		if dataSize < wantSize {
			errs = append(errs, fmt.Errorf("data file for volume %d is smaller (%d) than the %d needles it contains (%d)", v.Id, dataSize, count, wantSize))
		} else if dataSize != wantSize {
			errs = append(errs, fmt.Errorf("data file size for volume %d (%d) doesn't match the size for %d needles read (%d)", v.Id, dataSize, count, wantSize))
		}
	}

	return count, errs
}

// Scrub checks the entire volume content for issues.
func (v *Volume) Scrub() (int64, []error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	idxFile, idxFileSize, err := v.openIndex()
	if err != nil {
		return 0, []error{err}
	}
	defer idxFile.Close()

	return v.scrubVolumeData(v.DataBackend, idxFile, idxFileSize)
}

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, err error) {
	var indexSize int64
	if indexSize, err = verifyIndexFileIntegrity(indexFile); err != nil {
		return 0, fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), err)
	}
	if indexSize == 0 {
		return 0, nil
	}
	healthyIndexSize := indexSize
	for i := 1; i <= 10 && indexSize >= int64(i)*types.NeedleMapEntrySize; i++ {
		// check and fix last 10 entries
		lastAppendAtNs, err = doCheckAndFixVolumeData(v, indexFile, indexSize-int64(i)*types.NeedleMapEntrySize)
		if err == io.EOF {
			healthyIndexSize = indexSize - int64(i)*types.NeedleMapEntrySize
			continue
		}
		if err != ErrorSizeMismatch {
			break
		}
	}
	if healthyIndexSize < indexSize {
		return 0, fmt.Errorf("CheckVolumeDataIntegrity %s failed: index size %d differs from healthy size %d", indexFile.Name(), indexSize, healthyIndexSize)
	}
	return
}

func doCheckAndFixVolumeData(v *Volume, indexFile *os.File, indexOffset int64) (lastAppendAtNs uint64, err error) {
	var lastIdxEntry []byte
	if lastIdxEntry, err = readIndexEntryAtOffset(indexFile, indexOffset); err != nil {
		return 0, fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), err)
	}
	key, offset, size := idx.IdxFileEntry(lastIdxEntry)
	if offset.IsZero() {
		return 0, nil
	}
	if size < 0 {
		// read the deletion entry
		if lastAppendAtNs, err = verifyDeletedNeedleIntegrity(v.DataBackend, v.Version(), key); err != nil {
			return lastAppendAtNs, fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), err)
		}
	} else {
		if lastAppendAtNs, err = verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset(), key, size); err != nil {
			if err == ErrorSizeMismatch {
				return verifyNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset()+int64(types.MaxPossibleVolumeSize), key, size)
			}
			return lastAppendAtNs, err
		}
	}
	return lastAppendAtNs, nil
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%types.NeedleMapEntrySize != 0 {
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
	bytes = make([]byte, types.NeedleMapEntrySize)
	var readCount int
	readCount, err = indexFile.ReadAt(bytes, offset)
	if err == io.EOF && readCount == types.NeedleMapEntrySize {
		err = nil
	}
	return
}

func verifyNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key types.NeedleId, size types.Size) (lastAppendAtNs uint64, err error) {
	n, _, _, err := needle.ReadNeedleHeader(datFile, v, offset)
	if err == io.EOF {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("read %s at %d", datFile.Name(), offset)
	}
	if n.Size != size {
		return 0, ErrorSizeMismatch
	}
	if v == needle.Version3 {
		bytes := make([]byte, types.TimestampSize)
		var readCount int
		readCount, err = datFile.ReadAt(bytes, offset+types.NeedleHeaderSize+int64(size)+needle.NeedleChecksumSize)
		if err == io.EOF && readCount == types.TimestampSize {
			err = nil
		}
		if err == io.EOF {
			return 0, err
		}
		if err != nil {
			return 0, fmt.Errorf("verifyNeedleIntegrity check %s entry offset %d size %d: %v", datFile.Name(), offset, size, err)
		}
		n.AppendAtNs = util.BytesToUint64(bytes)
		fileTailOffset := offset + needle.GetActualSize(size, v)
		fileSize, _, err := datFile.GetStat()
		if err != nil {
			return 0, fmt.Errorf("stat file %s: %v", datFile.Name(), err)
		}
		if fileSize == fileTailOffset {
			return n.AppendAtNs, nil
		}
		if fileSize > fileTailOffset {
			glog.Warningf("data file %s actual %d bytes expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
			return n.AppendAtNs, fmt.Errorf("data file %s actual %d bytes expected %d bytes", datFile.Name(), fileSize, fileTailOffset)
		}
		glog.Warningf("data file %s has %d bytes, less than expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
	}
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+int64(size), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %v does not match needle's Id %v", key, n.Id)
	}
	return n.AppendAtNs, err
}

func verifyDeletedNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, key types.NeedleId) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	size := n.DiskSize(v)
	var fileSize int64
	fileSize, _, err = datFile.GetStat()
	if err != nil {
		return 0, fmt.Errorf("GetStat: %w", err)
	}
	if err = n.ReadData(datFile, fileSize-size, types.Size(0), v); err != nil {
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", fileSize-size, size, err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %v does not match needle's Id %v", key, n.Id)
	}
	return n.AppendAtNs, err
}

func (v *Volume) checkIdxFile() error {
	datFileSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		return fmt.Errorf("get stat %s: %v", v.FileName(".dat"), err)
	}
	if datFileSize <= super_block.SuperBlockSize {
		return nil
	}
	indexFileName := v.FileName(".idx")
	if util.FileExists(indexFileName) {
		return nil
	}
	return fmt.Errorf("idx file %s does not exists", indexFileName)
}
