package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	MarkNeedleDeleted = func(file *os.File, offset int64) error {
		b := make([]byte, types.SizeSize)
		types.SizeToBytes(b, types.TombstoneFileSize)
		n, err := file.WriteAt(b, offset+types.NeedleIdSize+types.OffsetSize)
		if err != nil {
			return fmt.Errorf("sorted needle write error: %w", err)
		}
		if n != types.SizeSize {
			return fmt.Errorf("sorted needle written %d bytes, expecting %d", n, types.SizeSize)
		}
		return nil
	}
)

func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	// Capture the entry's file offset during the mark callback so that we
	// can roll the tombstone back if the subsequent .ecj append fails. Go's
	// SearchNeedleFromSortedIndex only exposes the entry file offset via
	// processNeedleFn, so we wrap MarkNeedleDeleted in a closure.
	var entryFileOffset int64 = -1
	_, oldSize, err := SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId,
		func(f *os.File, fileOffset int64) error {
			entryFileOffset = fileOffset
			return MarkNeedleDeleted(f, fileOffset)
		})

	if err != nil {
		if err == NotFoundError {
			return nil
		}
		return err
	}

	// Already tombstoned: skip the .ecj append so that deleteCount (derived
	// from .ecj size) stays idempotent on re-delete.
	if oldSize.IsDeleted() {
		return nil
	}

	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	ev.ecjFileAccessLock.Lock()
	defer ev.ecjFileAccessLock.Unlock()

	if _, seekErr := ev.ecjFile.Seek(0, io.SeekEnd); seekErr != nil {
		ev.rollbackEcxTombstone(entryFileOffset, oldSize, needleId)
		return fmt.Errorf("seek ecj: %w", seekErr)
	}
	n, writeErr := ev.ecjFile.Write(b)
	if writeErr != nil {
		ev.rollbackEcxTombstone(entryFileOffset, oldSize, needleId)
		return fmt.Errorf("write ecj: %w", writeErr)
	}
	ev.ecjFileSize += int64(n)

	return nil
}

// rollbackEcxTombstone restores the original size bytes of an .ecx entry
// after a failed .ecj append, so a subsequent read does not see a needle
// that the heartbeat-reported delete_count never accounted for.
func (ev *EcVolume) rollbackEcxTombstone(entryFileOffset int64, oldSize types.Size, needleId types.NeedleId) {
	if entryFileOffset < 0 || ev.ecxFile == nil {
		return
	}
	sizeOffset := entryFileOffset + int64(types.NeedleIdSize) + int64(types.OffsetSize)
	buf := make([]byte, types.SizeSize)
	types.SizeToBytes(buf, oldSize)
	if _, writeErr := ev.ecxFile.WriteAt(buf, sizeOffset); writeErr != nil {
		glog.Errorf("ec volume %d: failed to rollback ecx tombstone for needle %d: %v",
			ev.VolumeId, needleId, writeErr)
	}
}

func RebuildEcxFile(baseFileName string) error {

	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecx file: %w", err)
	}
	defer ecxFile.Close()

	fstat, err := ecxFile.Stat()
	if err != nil {
		return err
	}

	ecxFileSize := fstat.Size()

	ecjFile, err := os.OpenFile(baseFileName+".ecj", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecj file: %w", err)
	}

	buf := make([]byte, types.NeedleIdSize)
	for {
		n, _ := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			break
		}

		needleId := types.BytesToNeedleId(buf)

		_, _, err = SearchNeedleFromSortedIndex(ecxFile, ecxFileSize, needleId, MarkNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			return err
		}

	}

	ecxFile.Close()

	os.Remove(baseFileName + ".ecj")

	return nil
}
