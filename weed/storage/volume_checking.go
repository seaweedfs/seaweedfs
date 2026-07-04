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
		if v.DataBackend == nil {
			idxFile.Close()
			return nil, 0, fmt.Errorf("volume %v has no data backend", v.Id)
		}
		volumeFileSize, _, err := v.DataBackend.GetStat()
		if err != nil {
			idxFile.Close()
			return nil, 0, fmt.Errorf("failed to stat storage for zero-size IDX volume %v: %w", v.Id, err)
		}

		// account for pre-allocated volumes (f.ex. after running "volume.grow") without data, as these
		// are allowed to have zero-size indices.
		if volumeFileSize > int64(super_block.SuperBlockSize) {
			idxFile.Close()
			return nil, 0, fmt.Errorf("zero-size IDX file for volume %v with store size %d", v.Id, volumeFileSize)
		}
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
func (v *Volume) scrubVolumeData(idxFile *os.File, idxFileSize int64) (int64, []error) {
	if v.DataBackend == nil {
		return 0, []error{fmt.Errorf("volume %d has no data backend", v.Id)}
	}

	// full scrubbing means also scrubbing the index
	var count int64
	_, errs := idx.CheckIndexFile(idxFile, idxFileSize, v.Version())

	// read and check every indexed needle
	var totalRead int64
	version := v.Version()
	err := idx.WalkIndexFile(idxFile, 0, func(id types.NeedleId, offset types.Offset, size types.Size) error {
		count++
		// A remote-tier delete records an offset-0 tombstone with no physical .dat
		// bytes, so it must not contribute to totalRead.
		if offset.IsZero() && size.IsDeleted() {
			return nil
		}
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
		if err := n.ReadData(v.DataBackend, offset.ToActualOffset(), size, version); err != nil {
			errs = append(errs, fmt.Errorf("failed to read needle %d on volume %d: %v", id, v.Id, err))
		}

		totalRead += actualSize
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}

	// check total volume file size
	wantSize := totalRead + super_block.SuperBlockSize
	dataSize, _, err := v.DataBackend.GetStat()

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

	return v.scrubVolumeData(idxFile, idxFileSize)
}

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, err error) {
	var indexSize int64
	if indexSize, err = verifyIndexFileIntegrity(indexFile); err != nil {
		return 0, fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), err)
	}
	if indexSize == 0 {
		return 0, nil
	}
	// The deeper-than-tail structural check (every (offset + actual size)
	// fits inside .dat — issue #8928) lives in volume.load(): it reads
	// MaximumNeedleEnd from the needle map after the load walk, so we don't
	// need a redundant linear scan of the .idx here.

	// Verify the needle physically last in the .dat (the highest offset) and
	// that the .dat ends exactly at it. The .idx is not always in .dat append
	// order: weed fix and other rebuilds could rewrite it sorted by key, which
	// puts the highest-key needle last instead of the .dat-tail needle. Picking
	// the last file-position entry there compares a mid-file needle's tail
	// against the full .dat size and falsely flips the volume read-only on every
	// load (issue #9688).
	tailEntryPos, err := findDatTailEntryOffset(v, indexFile, indexSize)
	if err != nil {
		return 0, fmt.Errorf("CheckVolumeDataIntegrity %s: %v", indexFile.Name(), err)
	}
	if tailEntryPos < 0 {
		// pre-allocated / all-zero index, nothing to verify against the .dat
		return 0, nil
	}

	if lastAppendAtNs, err = doCheckAndFixVolumeData(v, indexFile, tailEntryPos); err != nil {
		return lastAppendAtNs, fmt.Errorf("CheckVolumeDataIntegrity %s failed: %v", indexFile.Name(), err)
	}
	return lastAppendAtNs, nil
}

// findDatTailEntryOffset returns the .idx file position of the entry whose
// needle is physically last in the .dat (highest offset). The common case — an
// append-ordered .idx — is resolved in O(1): the last entry's on-disk end
// equals the .dat size. A key-sorted .idx (e.g. legacy weed fix output) falls
// back to a single linear scan for the maximum offset. Returns -1 when no entry
// has a non-zero offset (a pre-allocated / all-zero index).
func findDatTailEntryOffset(v *Volume, indexFile *os.File, indexSize int64) (int64, error) {
	// Fast path: an append-ordered .idx ends with the .dat-tail needle, so the
	// last entry's on-disk end matches the .dat size.
	if v.DataBackend != nil {
		if datSize, _, statErr := v.DataBackend.GetStat(); statErr == nil && datSize > 0 {
			lastPos := indexSize - types.NeedleMapEntrySize
			entryBytes, err := readIndexEntryAtOffset(indexFile, lastPos)
			if err != nil {
				return 0, fmt.Errorf("read last index entry: %v", err)
			}
			_, offset, size := idx.IdxFileEntry(entryBytes)
			if !offset.IsZero() && needleDiskEnd(offset, size, v.Version()) == datSize {
				return lastPos, nil
			}
		}
	}

	// Slow path: scan the whole .idx for the entry at the maximum offset.
	maxEntryPos := int64(-1)
	maxActualOffset := int64(-1)
	entryPos := int64(0)
	if err := idx.WalkIndexFile(indexFile, 0, func(_ types.NeedleId, offset types.Offset, _ types.Size) error {
		if !offset.IsZero() {
			if ao := offset.ToActualOffset(); ao > maxActualOffset {
				maxActualOffset = ao
				maxEntryPos = entryPos
			}
		}
		entryPos += types.NeedleMapEntrySize
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan index: %v", err)
	}
	return maxEntryPos, nil
}

// needleDiskEnd returns the byte offset just past the needle's on-disk record.
// Deletion tombstones carry TombstoneFileSize (-1) in the .idx but are written
// with DataSize=0, so their on-disk record is sized as 0.
func needleDiskEnd(offset types.Offset, size types.Size, version needle.Version) int64 {
	onDiskSize := size
	if size.IsDeleted() {
		onDiskSize = 0
	}
	return offset.ToActualOffset() + needle.GetActualSize(onDiskSize, version)
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
		// Deletion tombstone: the .idx carries TombstoneFileSize (-1) but the
		// appended needle in .dat carries Size=0. verifyDeletedNeedleIntegrity
		// reads it with Size=0 (the size check / 32GB wrap-around retry would
		// otherwise read past EOF) and still verifies the .dat ends at it.
		if lastAppendAtNs, err = verifyDeletedNeedleIntegrity(v.DataBackend, v.Version(), offset.ToActualOffset(), key); err != nil {
			if err == io.EOF || err == ErrorSizeMismatch {
				return lastAppendAtNs, err
			}
			return lastAppendAtNs, fmt.Errorf("verifyDeletedNeedleIntegrity %s failed: %v", indexFile.Name(), err)
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

func verifyDeletedNeedleIntegrity(datFile backend.BackendStorageFile, v needle.Version, offset int64, key types.NeedleId) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	// Tombstones are appended with DataSize=0, so the on-disk header carries
	// Size=0. TombstoneFileSize (-1) lives only in the .idx; passing it to
	// ReadData fails the size check and triggers the 32GB wrap-around retry,
	// which reads past EOF and falsely marks the volume read-only.
	size := types.Size(0)
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		// A truncated tail surfaces here as io.EOF; pass it (and a size mismatch)
		// through so the volume is marked read-only.
		if err == io.EOF || err == ErrorSizeMismatch {
			return n.AppendAtNs, err
		}
		return n.AppendAtNs, fmt.Errorf("read data [%d,%d) : %v", offset, offset+needle.GetActualSize(size, v), err)
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %v does not match needle's Id %v", key, n.Id)
	}
	// This tombstone is the needle at the .dat tail (the highest offset), so the
	// .dat must end exactly at it; extra bytes mean an unindexed trailing record.
	fileSize, _, statErr := datFile.GetStat()
	if statErr != nil {
		return n.AppendAtNs, fmt.Errorf("stat file %s: %v", datFile.Name(), statErr)
	}
	if fileTailOffset := offset + needle.GetActualSize(size, v); fileSize > fileTailOffset {
		glog.Warningf("data file %s actual %d bytes expected %d bytes!", datFile.Name(), fileSize, fileTailOffset)
		return n.AppendAtNs, fmt.Errorf("data file %s actual %d bytes expected %d bytes", datFile.Name(), fileSize, fileTailOffset)
	}
	return n.AppendAtNs, nil
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
