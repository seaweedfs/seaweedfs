package erasure_coding

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

const (
	DataShardsCount             = 10
	ParityShardsCount           = 4
	TotalShardsCount            = DataShardsCount + ParityShardsCount
	MaxShardCount               = 32 // Maximum number of shards since ShardBits is uint32 (bits 0-31)
	MinTotalDisks               = TotalShardsCount/ParityShardsCount + 1
	ErasureCodingLargeBlockSize = 1024 * 1024 * 1024 // 1GB
	ErasureCodingSmallBlockSize = 1024 * 1024        // 1MB
)

// WriteSortedFileFromIdx generates .ecx file from existing .idx file
// all keys are sorted in ascending order
func WriteSortedFileFromIdx(baseFileName string, ext string) (e error) {

	nm, err := readNeedleMap(baseFileName)
	if nm != nil {
		defer nm.Close()
	}
	if err != nil {
		return fmt.Errorf("readNeedleMap: %w", err)
	}

	ecxFile, err := os.OpenFile(baseFileName+ext, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open ecx file: %w", err)
	}
	defer ecxFile.Close()

	err = nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		bytes := value.ToBytes()
		_, writeErr := ecxFile.Write(bytes)
		return writeErr
	})

	if err != nil {
		return fmt.Errorf("failed to visit idx file: %w", err)
	}

	return nil
}

// WriteEcFiles generates .ec00 ~ .ec13 files using default EC context
func WriteEcFiles(baseFileName string) error {
	ctx := NewDefaultECContext("", 0)
	return WriteEcFilesWithContext(baseFileName, ctx)
}

// WriteEcFilesWithContext generates EC files using the provided context
func WriteEcFilesWithContext(baseFileName string, ctx *ECContext) error {
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx)
}

// RebuildEcFiles rebuilds missing EC shard files.
// additionalDirs are extra directories to search for existing shard files,
// which handles multi-disk servers where shards may be spread across disks.
func RebuildEcFiles(baseFileName string, additionalDirs ...string) ([]uint32, error) {
	// Attempt to load EC config from .vif file to preserve original configuration
	var ctx *ECContext
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)
		ps := int(volumeInfo.EcShardConfig.ParityShards)

		// Validate EC config before using it
		if ds > 0 && ps > 0 && ds+ps <= MaxShardCount {
			ctx = &ECContext{
				DataShards:   ds,
				ParityShards: ps,
			}
			glog.V(0).Infof("Rebuilding EC files for %s with config from .vif: %s", baseFileName, ctx.String())
		} else {
			glog.Warningf("Invalid EC config in .vif for %s (data=%d, parity=%d), using default", baseFileName, ds, ps)
			ctx = NewDefaultECContext("", 0)
		}
	} else {
		glog.V(0).Infof("Rebuilding EC files for %s with default config", baseFileName)
		ctx = NewDefaultECContext("", 0)
	}

	return RebuildEcFilesWithContext(baseFileName, ctx, additionalDirs...)
}

// RebuildEcFilesWithContext rebuilds missing EC files using the provided context.
// additionalDirs are extra directories to search for existing shard files.
func RebuildEcFilesWithContext(baseFileName string, ctx *ECContext, additionalDirs ...string) ([]uint32, error) {
	return generateMissingEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx, additionalDirs)
}

func ToExt(ecIndex int) string {
	return fmt.Sprintf(".ec%02d", ecIndex)
}

func generateEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext) error {
	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open dat file: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat dat file: %w", err)
	}

	glog.V(0).Infof("encodeDatFile %s.dat size:%d with EC context %s", baseFileName, fi.Size(), ctx.String())
	err = encodeDatFile(fi.Size(), baseFileName, bufferSize, largeBlockSize, file, smallBlockSize, ctx)
	if err != nil {
		return fmt.Errorf("encodeDatFile: %w", err)
	}
	return nil
}

// findShardFile looks for a shard file at baseFileName+ext, then in additionalDirs.
// Returns the first non-empty shard file path found. Any 0-byte residue files
// (typically left behind by a previously aborted rebuild) are returned in ghosts
// so the caller can remove them — otherwise they shadow real shards on the next
// rebuild attempt and the volume gets silently mounted with empty stripes.
func findShardFile(baseFileName string, ext string, additionalDirs []string) (shardPath string, shardSize int64, ghosts []string) {
	candidates := make([]string, 0, 1+len(additionalDirs))
	candidates = append(candidates, baseFileName+ext)
	baseName := filepath.Base(baseFileName)
	for _, dir := range additionalDirs {
		candidates = append(candidates, filepath.Join(dir, baseName+ext))
	}
	for _, c := range candidates {
		fi, statErr := os.Stat(c)
		if statErr != nil {
			continue
		}
		if fi.Size() == 0 {
			ghosts = append(ghosts, c)
			continue
		}
		if shardPath == "" {
			shardPath = c
			shardSize = fi.Size()
		}
	}
	return
}

func generateMissingEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext, additionalDirs []string) (generatedShardIds []uint32, err error) {

	// Pass 1: discover which shards exist and which are missing,
	// opening input files but NOT creating output files yet.
	shardHasData := make([]bool, ctx.Total())
	shardPaths := make([]string, ctx.Total()) // non-empty for present shards
	inputFiles := make([]*os.File, ctx.Total())
	var ghostPaths []string
	expectedShardSize := int64(-1)
	presentCount := 0
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		ext := ctx.ToExt(shardId)
		shardPath, shardSize, ghosts := findShardFile(baseFileName, ext, additionalDirs)
		ghostPaths = append(ghostPaths, ghosts...)
		if shardPath == "" {
			generatedShardIds = append(generatedShardIds, uint32(shardId))
			continue
		}
		if expectedShardSize < 0 {
			expectedShardSize = shardSize
		} else if shardSize != expectedShardSize {
			return nil, fmt.Errorf("ec shard size mismatch: %s is %d bytes, expected %d (refusing to rebuild from inconsistent shards)",
				shardPath, shardSize, expectedShardSize)
		}
		shardHasData[shardId] = true
		shardPaths[shardId] = shardPath
		inputFiles[shardId], err = os.OpenFile(shardPath, os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		defer inputFiles[shardId].Close()
		presentCount++
	}

	// Pre-check: bail out before creating any output files.
	if presentCount < ctx.DataShards {
		return nil, fmt.Errorf("not enough shards to rebuild %s: found %d shards, need at least %d (data shards), missing shards: %v",
			baseFileName, presentCount, ctx.DataShards, generatedShardIds)
	}

	glog.V(0).Infof("rebuilding %s: %d shards present (size %d), %d missing %v, %d ghost residue %v, config %s",
		baseFileName, presentCount, expectedShardSize, len(generatedShardIds), generatedShardIds, len(ghostPaths), ghostPaths, ctx.String())

	// Pass 2: create output files for missing shards now that we know
	// reconstruction is possible.
	outputFiles := make([]*os.File, ctx.Total())
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		if shardHasData[shardId] {
			continue
		}
		outputFileName := baseFileName + ctx.ToExt(shardId)
		outputFiles[shardId], err = os.OpenFile(outputFileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		defer outputFiles[shardId].Close()
	}

	err = rebuildEcFiles(shardHasData, inputFiles, outputFiles, ctx, expectedShardSize)
	if err != nil {
		return nil, fmt.Errorf("rebuildEcFiles: %w", err)
	}

	// Reconstruction succeeded — remove any 0-byte residue from prior aborted
	// rebuilds so they don't shadow real shards on the next pass and don't get
	// mounted as empty stripes. Skip residue paths that the output-file step
	// has already overwritten with real data, since those entries in
	// ghostPaths refer to the just-rebuilt shard's location.
	freshOutputs := make(map[string]struct{}, len(outputFiles))
	for _, f := range outputFiles {
		if f != nil {
			freshOutputs[f.Name()] = struct{}{}
		}
	}
	for _, ghost := range ghostPaths {
		if _, overwritten := freshOutputs[ghost]; overwritten {
			continue
		}
		if removeErr := os.Remove(ghost); removeErr != nil && !os.IsNotExist(removeErr) {
			glog.Warningf("failed to remove 0-byte ec shard residue %s: %v", ghost, removeErr)
		} else {
			glog.V(0).Infof("removed 0-byte ec shard residue %s", ghost)
		}
	}
	return
}

func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext) error {

	bufferSize := int64(len(buffers[0]))
	if bufferSize == 0 {
		glog.Fatal("unexpected zero buffer size")
	}

	batchCount := blockSize / bufferSize
	if blockSize%bufferSize != 0 {
		glog.Fatalf("unexpected block size %d buffer size %d", blockSize, bufferSize)
	}

	for b := int64(0); b < batchCount; b++ {
		err := encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs, ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func openEcFiles(baseFileName string, forRead bool, ctx *ECContext) (files []*os.File, err error) {
	for i := 0; i < ctx.Total(); i++ {
		fname := baseFileName + ctx.ToExt(i)
		openOption := os.O_TRUNC | os.O_CREATE | os.O_WRONLY
		if forRead {
			openOption = os.O_RDONLY
		}
		f, err := os.OpenFile(fname, openOption, 0644)
		if err != nil {
			return files, fmt.Errorf("failed to open file %s: %v", fname, err)
		}
		files = append(files, f)
	}
	return
}

func closeEcFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}

func encodeDataOneBatch(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext) error {

	// read data into buffers
	for i := 0; i < ctx.DataShards; i++ {
		n, err := file.ReadAt(buffers[i], startOffset+blockSize*int64(i))
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if n < len(buffers[i]) {
			for t := len(buffers[i]) - 1; t >= n; t-- {
				buffers[i][t] = 0
			}
		}
	}

	err := enc.Encode(buffers)
	if err != nil {
		return err
	}

	for i := 0; i < ctx.Total(); i++ {
		_, err := outputs[i].Write(buffers[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func encodeDatFile(remainingSize int64, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64, ctx *ECContext) error {

	var processedSize int64

	enc, err := ctx.CreateEncoder()
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	buffers := make([][]byte, ctx.Total())
	for i := range buffers {
		buffers[i] = make([]byte, bufferSize)
	}

	outputs, err := openEcFiles(baseFileName, false, ctx)
	defer closeEcFiles(outputs)
	if err != nil {
		return fmt.Errorf("failed to open ec files %s: %v", baseFileName, err)
	}

	// Pre-calculate row sizes to avoid redundant calculations in loops
	largeRowSize := largeBlockSize * int64(ctx.DataShards)
	smallRowSize := smallBlockSize * int64(ctx.DataShards)

	for remainingSize >= largeRowSize {
		err = encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs, ctx)
		if err != nil {
			return fmt.Errorf("failed to encode large chunk data: %w", err)
		}
		remainingSize -= largeRowSize
		processedSize += largeRowSize
	}
	for remainingSize > 0 {
		err = encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs, ctx)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %w", err)
		}
		remainingSize -= smallRowSize
		processedSize += smallRowSize
	}
	return nil
}

func rebuildEcFiles(shardHasData []bool, inputFiles []*os.File, outputFiles []*os.File, ctx *ECContext, expectedShardSize int64) error {

	enc, err := ctx.CreateEncoder()
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	buffers := make([][]byte, ctx.Total())
	for i := range buffers {
		if shardHasData[i] {
			buffers[i] = make([]byte, ErasureCodingSmallBlockSize)
		}
	}

	for startOffset := int64(0); startOffset < expectedShardSize; {
		chunkSize := int64(ErasureCodingSmallBlockSize)
		if remaining := expectedShardSize - startOffset; remaining < chunkSize {
			chunkSize = remaining
		}

		// read the input data from files
		for i := 0; i < ctx.Total(); i++ {
			if shardHasData[i] {
				buf := buffers[i][:chunkSize]
				n, readErr := inputFiles[i].ReadAt(buf, startOffset)
				if int64(n) != chunkSize {
					return fmt.Errorf("short read on shard %s at offset %d: got %d, want %d (err=%v)",
						inputFiles[i].Name(), startOffset, n, chunkSize, readErr)
				}
				buffers[i] = buf
			} else {
				buffers[i] = nil
			}
		}

		// encode the data
		err = enc.Reconstruct(buffers)
		if err != nil {
			return fmt.Errorf("reconstruct: %w", err)
		}

		// write the data to output files
		for i := 0; i < ctx.Total(); i++ {
			if !shardHasData[i] {
				n, writeErr := outputFiles[i].WriteAt(buffers[i][:chunkSize], startOffset)
				if int64(n) != chunkSize {
					return fmt.Errorf("short write to %s at offset %d: got %d, want %d (err=%v)",
						outputFiles[i].Name(), startOffset, n, chunkSize, writeErr)
				}
			}
		}
		startOffset += chunkSize
	}
	return nil
}

func readNeedleMap(baseFileName string) (*needle_map.MemDb, error) {
	indexFile, err := os.OpenFile(baseFileName+".idx", os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot read Volume Index %s.idx: %v", baseFileName, err)
	}
	defer indexFile.Close()

	cm := needle_map.NewMemDb()
	err = idx.WalkIndexFile(indexFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if !offset.IsZero() && !size.IsDeleted() {
			cm.Set(key, offset, size)
		} else {
			cm.Delete(key)
		}
		return nil
	})
	return cm, err
}
