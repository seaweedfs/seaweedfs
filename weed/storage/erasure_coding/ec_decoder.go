package erasure_coding

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// EcNoLiveEntriesSubstring is used for server/client coordination when ec.decode determines that
// decoding should be a no-op (all entries are deleted).
const EcNoLiveEntriesSubstring = "has no live entries"

// HasLiveNeedles returns whether the EC index (.ecx) contains at least one live (non-deleted) entry.
// This is used by ec.decode to avoid generating an empty normal volume when all entries were deleted.
func HasLiveNeedles(indexBaseFileName string) (hasLive bool, err error) {
	err = iterateEcxFile(indexBaseFileName, func(_ types.NeedleId, _ types.Offset, size types.Size) error {
		if !size.IsDeleted() {
			hasLive = true
			return io.EOF // stop early
		}
		return nil
	})
	return
}

// write .idx file from .ecx and .ecj files
func WriteIdxFileFromEcIndex(baseFileName string) (err error) {

	ecxFile, openErr := os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecx: %v", baseFileName, openErr)
	}
	defer ecxFile.Close()

	// Write to a temp file and atomically rename into place, so a crash mid-write
	// never leaves a partial .idx at the final name beside the source shards.
	idxFileName := baseFileName + ".idx"
	tmpFileName := idxFileName + ".tmp"
	idxFile, openErr := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open %s: %v", tmpFileName, openErr)
	}
	committed := false
	defer func() {
		idxFile.Close()
		if !committed {
			os.Remove(tmpFileName)
		}
	}()

	if _, err = io.Copy(idxFile, ecxFile); err != nil {
		return fmt.Errorf("copy ecx to idx for %s: %v", baseFileName, err)
	}

	err = iterateEcjFile(baseFileName, func(key types.NeedleId) error {
		bytes := needle_map.ToBytes(key, types.Offset{}, types.TombstoneFileSize)
		if _, writeErr := idxFile.Write(bytes); writeErr != nil {
			return writeErr
		}
		return nil
	})
	if err != nil {
		return err
	}

	// fsync, rename, then fsync the dir so the decoded .idx is durable and
	// atomically published before the caller deletes the source shards.
	if err = idxFile.Sync(); err != nil {
		return fmt.Errorf("sync idx for %s: %v", baseFileName, err)
	}
	if err = idxFile.Close(); err != nil {
		return fmt.Errorf("close idx for %s: %v", baseFileName, err)
	}
	if err = os.Rename(tmpFileName, idxFileName); err != nil {
		return fmt.Errorf("rename idx for %s: %v", baseFileName, err)
	}
	if err = util.FsyncDir(filepath.Dir(idxFileName)); err != nil {
		return fmt.Errorf("fsync dir for %s: %v", baseFileName, err)
	}
	committed = true
	return nil
}

// FindDatFileSize calculate .dat file size from max offset entry
// there may be extra deletions after that entry
// but they are deletions anyway
func FindDatFileSize(dataBaseFileName, indexBaseFileName string) (datSize int64, err error) {

	version, err := readEcVolumeVersion(dataBaseFileName)
	if err != nil {
		return 0, fmt.Errorf("read ec volume %s version: %v", dataBaseFileName, err)
	}

	// Safety: ensure datSize is at least SuperBlockSize. While the caller typically
	// checks HasLiveNeedles first, this protects against direct calls to FindDatFileSize
	// when all needles are deleted (see issue #7748).
	datSize = int64(super_block.SuperBlockSize)

	err = iterateEcxFile(indexBaseFileName, func(key types.NeedleId, offset types.Offset, size types.Size) error {

		if size.IsDeleted() {
			return nil
		}

		entryStopOffset := offset.ToActualOffset() + needle.GetActualSize(size, version)
		if datSize < entryStopOffset {
			datSize = entryStopOffset
		}

		return nil
	})

	return
}

func readEcVolumeVersion(baseFileName string) (version needle.Version, err error) {

	// find volume version
	datFile, err := os.OpenFile(baseFileName+".ec00", os.O_RDONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("open ec volume %s superblock: %v", baseFileName, err)
	}
	datBackend := backend.NewDiskFile(datFile)

	superBlock, err := super_block.ReadSuperBlock(datBackend)
	datBackend.Close()
	if err != nil {
		return 0, fmt.Errorf("read ec volume %s superblock: %v", baseFileName, err)
	}

	return superBlock.Version, nil

}

func iterateEcxFile(baseFileName string, processNeedleFn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	ecxFile, openErr := os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecx: %v", baseFileName, openErr)
	}
	defer ecxFile.Close()

	buf := make([]byte, types.NeedleMapEntrySize)
	for {
		// .ecx is a sealed index: a partial trailing record means corruption, not a torn append.
		_, err := io.ReadFull(ecxFile, buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read ecx %s.ecx: %w", baseFileName, err)
		}
		key, offset, size := idx.IdxFileEntry(buf)
		if processNeedleFn != nil {
			err = processNeedleFn(key, offset, size)
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}
	}

}

func iterateEcjFile(baseFileName string, processNeedleFn func(key types.NeedleId) error) error {
	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}
	ecjFile, openErr := os.OpenFile(baseFileName+".ecj", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecj: %v", baseFileName, openErr)
	}
	defer ecjFile.Close()

	buf := make([]byte, types.NeedleIdSize)
	for {
		n, err := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if processNeedleFn != nil {
			err = processNeedleFn(types.BytesToNeedleId(buf))
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}

}

// WriteDatFile generates .dat from EC shard files (e.g., .ec00 ~ .ec09 for 10+4).
// datFileSize is the number of bytes to write, i.e. the live data extent from
// FindDatFileSize. encodedDatFileSize is the .dat size at encode time, which
// fixed the shard block layout: deletions can move the live extent below the
// large-block row boundary, and deriving the layout from the shrunk extent
// would read the shards in the wrong block order.
func WriteDatFile(baseFileName string, datFileSize int64, encodedDatFileSize int64, shardFileNames []string) error {
	return writeDatFile(baseFileName, datFileSize, encodedDatFileSize, shardFileNames, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize)
}

func writeDatFile(baseFileName string, datFileSize int64, encodedDatFileSize int64, shardFileNames []string, largeBlockSize int64, smallBlockSize int64) error {

	if datFileSize > encodedDatFileSize {
		return fmt.Errorf("dat file size %d exceeds encoded dat file size %d", datFileSize, encodedDatFileSize)
	}

	// Write to a temp file and atomically rename into place, so a crash mid-write
	// never leaves a partial .dat at the final name beside the source shards.
	datFileName := baseFileName + ".dat"
	tmpFileName := datFileName + ".tmp"
	datFile, openErr := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot write volume %s: %v", tmpFileName, openErr)
	}

	// Use the actual number of data shards passed in rather than the global
	// constant, so the de-striping matches the caller's shard set.
	dataShards := len(shardFileNames)
	inputFiles := make([]*os.File, dataShards)

	committed := false
	defer func() {
		datFile.Close()
		for shardId := 0; shardId < dataShards; shardId++ {
			if inputFiles[shardId] != nil {
				inputFiles[shardId].Close()
			}
		}
		if !committed {
			os.Remove(tmpFileName)
		}
	}()

	for shardId := 0; shardId < dataShards; shardId++ {
		inputFiles[shardId], openErr = os.OpenFile(shardFileNames[shardId], os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
	}

	for encodedDatFileSize >= int64(dataShards)*largeBlockSize && datFileSize > 0 {
		for shardId := 0; shardId < dataShards && datFileSize > 0; shardId++ {
			toRead := min(datFileSize, largeBlockSize)
			w, err := io.CopyN(datFile, inputFiles[shardId], toRead)
			if w != toRead {
				return fmt.Errorf("copy %s large block on shardId %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= toRead
		}
		encodedDatFileSize -= int64(dataShards) * largeBlockSize
	}

	for datFileSize > 0 {
		for shardId := 0; shardId < dataShards && datFileSize > 0; shardId++ {
			toRead := min(datFileSize, smallBlockSize)
			w, err := io.CopyN(datFile, inputFiles[shardId], toRead)
			if w != toRead {
				return fmt.Errorf("copy %s small block %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= toRead
		}
	}

	// fsync, rename, then fsync the dir so the decoded .dat is durable and
	// atomically published before the caller deletes the source shards.
	if err := datFile.Sync(); err != nil {
		return fmt.Errorf("sync dat for %s: %v", baseFileName, err)
	}
	if err := datFile.Close(); err != nil {
		return fmt.Errorf("close dat for %s: %v", baseFileName, err)
	}
	if err := os.Rename(tmpFileName, datFileName); err != nil {
		return fmt.Errorf("rename dat for %s: %v", baseFileName, err)
	}
	if err := util.FsyncDir(filepath.Dir(datFileName)); err != nil {
		return fmt.Errorf("fsync dir for %s: %v", baseFileName, err)
	}
	committed = true
	return nil
}

func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
