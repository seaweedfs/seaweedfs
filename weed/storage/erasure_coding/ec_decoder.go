package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// write .idx file from .ecx and .ecj files
func WriteIdxFileFromEcIndex(baseFileName string) (err error) {

	ecxFile, openErr := os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open ec index %s.ecx: %v", baseFileName, openErr)
	}
	defer ecxFile.Close()

	idxFile, openErr := os.OpenFile(baseFileName+".idx", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot open %s.idx: %v", baseFileName, openErr)
	}
	defer idxFile.Close()

	io.Copy(idxFile, ecxFile)

	err = iterateEcjFile(baseFileName, func(key types.NeedleId) error {

		bytes := needle_map.ToBytes(key, types.Offset{}, types.TombstoneFileSize)
		idxFile.Write(bytes)

		return nil
	})

	return err
}

// FindDatFileSize calculate .dat file size from max offset entry
// there may be extra deletions after that entry
// but they are deletions anyway
func FindDatFileSize(dataBaseFileName, indexBaseFileName string) (datSize int64, err error) {

	version, err := readEcVolumeVersion(dataBaseFileName)
	if err != nil {
		return 0, fmt.Errorf("read ec volume %s version: %v", dataBaseFileName, err)
	}

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
		n, err := ecxFile.Read(buf)
		if n != types.NeedleMapEntrySize {
			if err == io.EOF {
				return nil
			}
			return err
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

// WriteDatFile generates .dat from .ec00 ~ .ec09 files
func WriteDatFile(baseFileName string, datFileSize int64, shardFileNames []string) error {

	datFile, openErr := os.OpenFile(baseFileName+".dat", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if openErr != nil {
		return fmt.Errorf("cannot write volume %s.dat: %v", baseFileName, openErr)
	}
	defer datFile.Close()

	inputFiles := make([]*os.File, DataShardsCount)

	defer func() {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			if inputFiles[shardId] != nil {
				inputFiles[shardId].Close()
			}
		}
	}()

	for shardId := 0; shardId < DataShardsCount; shardId++ {
		inputFiles[shardId], openErr = os.OpenFile(shardFileNames[shardId], os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
	}

	for datFileSize >= DataShardsCount*ErasureCodingLargeBlockSize {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			w, err := io.CopyN(datFile, inputFiles[shardId], ErasureCodingLargeBlockSize)
			if w != ErasureCodingLargeBlockSize {
				return fmt.Errorf("copy %s large block on shardId %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= ErasureCodingLargeBlockSize
		}
	}

	for datFileSize > 0 {
		for shardId := 0; shardId < DataShardsCount; shardId++ {
			toRead := min(datFileSize, ErasureCodingSmallBlockSize)
			w, err := io.CopyN(datFile, inputFiles[shardId], toRead)
			if w != toRead {
				return fmt.Errorf("copy %s small block %d: %v", baseFileName, shardId, err)
			}
			datFileSize -= toRead
		}
	}

	return nil
}

func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
