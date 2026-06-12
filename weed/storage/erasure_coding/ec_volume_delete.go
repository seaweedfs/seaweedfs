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

// DeleteNeedleFromEcx marks the given needle as deleted. .ecx is treated
// as an immutable sealed sorted index; runtime deletes are recorded by
// appending the needle id to the .ecj deletion journal and inserting it
// into the in-memory deletedNeedles set. A subsequent FindNeedleFromEcx
// masks the id out by returning TombstoneFileSize.
//
// The .ecj append is the durable commit point — only after it syncs do
// we publish the id into the in-memory set. A partial write is truncated
// back to the known-good size so the on-disk journal and the set cannot
// drift.
func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	// Look the needle up read-only. A missing needle is not an error
	// (already gone, e.g. from a race against encode); a pre-existing
	// .ecx tombstone means a prior decode/rebuild folded it in, in
	// which case there is nothing to journal but we still mirror it
	// into the in-memory set so delete_count stays consistent.
	_, oldSize, err := SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
	if err != nil {
		if err == NotFoundError {
			return nil
		}
		return err
	}
	if oldSize.IsDeleted() {
		ev.markNeedleDeletedInMemory(needleId)
		return nil
	}

	// Serialise runtime deletes on ecjFileAccessLock so the idempotence
	// check, the journal append and the set insertion happen atomically
	// with respect to one another.
	ev.ecjFileAccessLock.Lock()
	defer ev.ecjFileAccessLock.Unlock()

	if ev.IsNeedleDeleted(needleId) {
		return nil
	}

	// Close nils ecjFile under this same lock, so a delete that resolved its
	// .ecx lookup before an eviction (e.g. the generate-time UnloadEcVolume)
	// can reach here with no journal fd. Bail with a clear error rather than
	// operating on the closed/nil handle.
	if ev.ecjFile == nil {
		return fmt.Errorf("ec volume %d closed", ev.VolumeId)
	}

	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	prevEcjSize := ev.ecjFileSize
	if _, seekErr := ev.ecjFile.Seek(0, io.SeekEnd); seekErr != nil {
		return fmt.Errorf("seek ecj: %w", seekErr)
	}
	n, writeErr := ev.ecjFile.Write(b)
	if writeErr != nil {
		if truncErr := ev.ecjFile.Truncate(prevEcjSize); truncErr != nil {
			glog.Errorf("ec volume %d: failed to truncate ecj after write error: %v", ev.VolumeId, truncErr)
		}
		return fmt.Errorf("write ecj: %w", writeErr)
	}
	if syncErr := ev.ecjFile.Sync(); syncErr != nil {
		if truncErr := ev.ecjFile.Truncate(prevEcjSize); truncErr != nil {
			glog.Errorf("ec volume %d: failed to truncate ecj after sync error: %v", ev.VolumeId, truncErr)
		}
		return fmt.Errorf("sync ecj: %w", syncErr)
	}
	ev.ecjFileSize += int64(n)

	// Publish into the in-memory set only after the journal is durable.
	ev.markNeedleDeletedInMemory(needleId)

	return nil
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
		// io.ReadFull distinguishes a clean end (io.EOF) from a torn tail
		// (io.ErrUnexpectedEOF) and a transient short read; a bare n!=size
		// break would silently drop the rest of the journal and then unlink it.
		_, readErr := io.ReadFull(ecjFile, buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			// Torn or unreadable journal: abort and leave .ecj in place so a
			// retry can re-apply the deletions rather than resurrect them.
			ecxFile.Close()
			ecjFile.Close()
			return fmt.Errorf("rebuild: read ecj: %w", readErr)
		}

		needleId := types.BytesToNeedleId(buf)

		_, _, err = SearchNeedleFromSortedIndex(ecxFile, ecxFileSize, needleId, MarkNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			ecjFile.Close()
			return err
		}

	}

	// Flush the in-place tombstones before removing the journal; otherwise a
	// crash can persist the .ecj unlink ahead of the .ecx writes and resurrect
	// the deleted needles on the next load.
	if err = ecxFile.Sync(); err != nil {
		ecxFile.Close()
		ecjFile.Close()
		return fmt.Errorf("rebuild: sync ecx: %w", err)
	}
	ecxFile.Close()
	ecjFile.Close()

	os.Remove(baseFileName + ".ecj")

	return nil
}
