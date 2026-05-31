package erasure_coding

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

// WriteEcFiles generates .ec00 ~ .ec13 files from baseFileName.dat. Pass
// BackgroundECContext for the default ratio, or an explicit ctx for a configured
// (e.g. custom-ratio) layout. It returns the bitrot protection (per-shard block
// CRC32C) computed during the single encode pass; the caller persists it as a
// <base>.ecsum sidecar.
func WriteEcFiles(baseFileName string, ctx *ECContext) (*volume_server_pb.EcBitrotProtection, error) {
	if ctx == nil || ctx.Total() == 0 {
		ctx = NewDefaultECContext("", 0)
	}
	return generateEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx)
}

// RebuildEcFiles rebuilds missing EC shard files. Pass BackgroundECContext to
// resolve the layout from the volume's .vif (falling back to the default ratio),
// or an explicit ctx when the caller already knows the shard layout.
// additionalDirs are extra directories to search for existing shard files,
// which handles multi-disk servers where shards may be spread across disks.
// When a bitrot checksum sidecar is present for the (generation-0) volume,
// present input shards are verified against it and corrupt ones are excluded
// from Reed-Solomon and regenerated; unsafeIgnoreSidecar bypasses that guard.
func RebuildEcFiles(baseFileName string, ctx *ECContext, unsafeIgnoreSidecar bool, additionalDirs ...string) ([]uint32, error) {
	if ctx == nil || ctx.Total() == 0 {
		// Resolve the layout from the .vif to preserve the original configuration.
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
	}

	return generateMissingEcFiles(baseFileName, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx, unsafeIgnoreSidecar, additionalDirs)
}

func ToExt(ecIndex int) string {
	return fmt.Sprintf(".ec%02d", ecIndex)
}

func generateEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext) (*volume_server_pb.EcBitrotProtection, error) {
	file, err := os.OpenFile(baseFileName+".dat", os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open dat file: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat dat file: %w", err)
	}

	// One rolling-CRC builder per shard; fed as each shard's bytes are written.
	builders := make([]*shardChecksumBuilder, ctx.Total())
	for i := range builders {
		builders[i] = newShardChecksumBuilder(BitrotBlockSize)
	}

	glog.V(0).Infof("encodeDatFile %s.dat size:%d with EC context %s", baseFileName, fi.Size(), ctx.String())
	err = encodeDatFile(fi.Size(), baseFileName, bufferSize, largeBlockSize, file, smallBlockSize, ctx, builders)
	if err != nil {
		return nil, fmt.Errorf("encodeDatFile: %w", err)
	}
	return buildProtectionFromBuilders(ctx, builders, BitrotBlockSize), nil
}

// findShardFile looks for a shard file at baseFileName+ext, then in additionalDirs.
func findShardFile(baseFileName string, ext string, additionalDirs []string) string {
	primary := baseFileName + ext
	if util.FileExists(primary) {
		return primary
	}
	baseName := filepath.Base(baseFileName)
	for _, dir := range additionalDirs {
		candidate := filepath.Join(dir, baseName+ext)
		if util.FileExists(candidate) {
			return candidate
		}
	}
	return ""
}

func generateMissingEcFiles(baseFileName string, bufferSize int, largeBlockSize int64, smallBlockSize int64, ctx *ECContext, unsafeIgnoreSidecar bool, additionalDirs []string) (generatedShardIds []uint32, err error) {

	// Pass 1: discover which shards exist and which are missing,
	// opening input files but NOT creating output files yet.
	shardHasData := make([]bool, ctx.Total())
	shardPaths := make([]string, ctx.Total()) // non-empty for present shards (also the in-place output for a reclassified-corrupt shard)
	inputFiles := make([]*os.File, ctx.Total())
	presentCount := 0
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		ext := ctx.ToExt(shardId)
		shardPath := findShardFile(baseFileName, ext, additionalDirs)
		if shardPath != "" {
			shardHasData[shardId] = true
			shardPaths[shardId] = shardPath
			inputFiles[shardId], err = os.OpenFile(shardPath, os.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			defer inputFiles[shardId].Close()
			presentCount++
		} else {
			generatedShardIds = append(generatedShardIds, uint32(shardId))
		}
	}

	// Bitrot verify-and-exclude: when a generation-0 checksum sidecar is present
	// and valid, verify each present input shard against it and reclassify
	// corrupt ones as missing so Reed-Solomon regenerates them instead of
	// silently consuming corrupt bytes. corruptOwned marks shards whose
	// (corrupt) original file must be replaced in place at its discovered path.
	corruptOwned := make([]bool, ctx.Total())
	prot, status := loadRebuildSidecar(baseFileName, ctx, additionalDirs)
	switch status {
	case BitrotInvalid:
		if !unsafeIgnoreSidecar {
			return nil, fmt.Errorf("bitrot sidecar for %s is malformed/unverifiable; refusing to rebuild (pass unsafeIgnoreSidecar to override)", baseFileName)
		}
		glog.Warningf("bitrot sidecar for %s is malformed/unverifiable; proceeding because unsafeIgnoreSidecar is set", baseFileName)
	case BitrotOn:
		corrupt := make([]int, 0, ctx.Total())
		for shardId := 0; shardId < ctx.Total(); shardId++ {
			if !shardHasData[shardId] {
				continue
			}
			entry := shardChecksums(prot, uint32(shardId))
			if entry == nil {
				continue
			}
			mismatched, verr := verifyShardFileBlocks(shardPaths[shardId], entry, int64(prot.BlockSize))
			if verr != nil {
				// A read error means we cannot trust this shard as a Reed-Solomon
				// input. Exclude it (treat as corrupt) rather than silently
				// feeding possibly-corrupt bytes into reconstruction.
				glog.Warningf("bitrot: failed to verify present shard %d for %s: %v; excluding it", shardId, baseFileName, verr)
				corrupt = append(corrupt, shardId)
				continue
			}
			if len(mismatched) > 0 {
				corrupt = append(corrupt, shardId)
			}
		}
		if len(corrupt) > 0 {
			// Wholesale-mismatch guard (RS-arbiter conservative form): localized
			// bitrot touches a few shards; a stale/wrong sidecar mismatches more
			// than parity_shards. In that case refuse rather than excluding good
			// shards en masse.
			if len(corrupt) > ctx.ParityShards && !unsafeIgnoreSidecar {
				return nil, fmt.Errorf("bitrot sidecar suspect for %s: %d/%d present shards mismatch (> parity %d); refusing to rebuild (pass unsafeIgnoreSidecar to override)",
					baseFileName, len(corrupt), presentCount, ctx.ParityShards)
			}
			if presentCount-len(corrupt) < ctx.DataShards && !unsafeIgnoreSidecar {
				return nil, fmt.Errorf("bitrot: only %d verified-good shards for %s, need %d data shards; sidecar may be stale (pass unsafeIgnoreSidecar to override)",
					presentCount-len(corrupt), baseFileName, ctx.DataShards)
			}
			if !unsafeIgnoreSidecar {
				for _, shardId := range corrupt {
					glog.Warningf("bitrot: present shard %d for %s fails checksum; excluding from rebuild inputs and regenerating", shardId, baseFileName)
					shardHasData[shardId] = false
					corruptOwned[shardId] = true
					generatedShardIds = append(generatedShardIds, uint32(shardId))
					presentCount--
				}
			}
		}
	}

	// Pre-check: bail out before creating any output files.
	if presentCount < ctx.DataShards {
		return nil, fmt.Errorf("not enough shards to rebuild %s: found %d shards, need at least %d (data shards), missing shards: %v",
			baseFileName, presentCount, ctx.DataShards, generatedShardIds)
	}

	glog.V(0).Infof("rebuilding %s: %d shards present, %d missing %v, config %s",
		baseFileName, presentCount, len(generatedShardIds), generatedShardIds, ctx.String())

	// Pass 2: create output files for missing shards. A genuinely-absent shard
	// is written at baseFileName+ext; a reclassified-corrupt shard is written to
	// a temp file beside its discovered location and atomically renamed over the
	// corrupt original after the rebuild (and checksum) succeed, so we never
	// leave a duplicate shard id or a half-written file.
	outputFiles := make([]*os.File, ctx.Total())
	writePaths := make([]string, ctx.Total())
	finalPaths := make([]string, ctx.Total())
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		if shardHasData[shardId] {
			continue
		}
		finalPath := baseFileName + ctx.ToExt(shardId)
		writePath := finalPath
		if corruptOwned[shardId] && shardPaths[shardId] != "" {
			finalPath = shardPaths[shardId]
			writePath = shardPaths[shardId] + ".rebuilding"
		}
		outputFiles[shardId], err = os.OpenFile(writePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		defer outputFiles[shardId].Close()
		writePaths[shardId] = writePath
		finalPaths[shardId] = finalPath
	}

	if err = rebuildEcFiles(shardHasData, inputFiles, outputFiles, ctx); err != nil {
		return nil, fmt.Errorf("rebuildEcFiles: %w", err)
	}

	// Verify regenerated shards against the sidecar. Reed-Solomon is
	// deterministic, so a regenerated shard that does NOT match the sidecar
	// means the sidecar is wrong/stale (not the shard) — fail closed rather than
	// publishing bytes we cannot trust. On ANY verification failure (sync, read
	// error, or mismatch) remove every generated output so the rebuild publishes
	// nothing: a genuinely-missing shard returns to missing; a reclassified-
	// corrupt shard keeps its untouched original.
	if status == BitrotOn && !unsafeIgnoreSidecar {
		for shardId := 0; shardId < ctx.Total(); shardId++ {
			if writePaths[shardId] == "" {
				continue
			}
			entry := shardChecksums(prot, uint32(shardId))
			if entry == nil {
				continue
			}
			if err = outputFiles[shardId].Sync(); err != nil {
				cleanupRebuildOutputs(outputFiles, writePaths)
				return nil, fmt.Errorf("sync regenerated shard %d: %w", shardId, err)
			}
			mismatched, verr := verifyShardFileBlocks(writePaths[shardId], entry, int64(prot.BlockSize))
			if verr != nil {
				cleanupRebuildOutputs(outputFiles, writePaths)
				return nil, fmt.Errorf("bitrot: verify regenerated shard %d for %s: %w", shardId, baseFileName, verr)
			}
			if len(mismatched) > 0 {
				cleanupRebuildOutputs(outputFiles, writePaths)
				return nil, fmt.Errorf("bitrot: regenerated shard %d for %s does not match sidecar (%d blocks differ); sidecar likely stale — aborting (pass unsafeIgnoreSidecar to override)",
					shardId, baseFileName, len(mismatched))
			}
		}
	}

	// Atomically move reclassified-corrupt rebuilds over their originals.
	for shardId := 0; shardId < ctx.Total(); shardId++ {
		if writePaths[shardId] != "" && writePaths[shardId] != finalPaths[shardId] {
			outputFiles[shardId].Close()
			if rerr := os.Rename(writePaths[shardId], finalPaths[shardId]); rerr != nil {
				return nil, fmt.Errorf("bitrot: replace corrupt shard %d (%s -> %s): %w", shardId, writePaths[shardId], finalPaths[shardId], rerr)
			}
		}
	}
	return
}

// cleanupRebuildOutputs removes every generated output on a failed fail-closed
// rebuild: temp replacements AND genuinely-missing shards written directly at
// their final path, so no unverified bytes are published. A reclassified-corrupt
// shard's untouched original (at finalPath, distinct from its temp writePath) is
// left in place; a genuinely-missing shard (writePath == finalPath) returns to
// missing.
func cleanupRebuildOutputs(outputFiles []*os.File, writePaths []string) {
	for i := range writePaths {
		if writePaths[i] == "" {
			continue
		}
		if outputFiles[i] != nil {
			outputFiles[i].Close()
		}
		os.Remove(writePaths[i])
	}
}

// loadRebuildSidecar loads and validates the generation-0 checksum sidecar for a
// rebuild. RebuildEcFiles operates on the un-suffixed (generation 0) shard
// names, so only the legacy sidecar is relevant here. Returns BitrotOff when
// absent or describing a different generation/config, BitrotInvalid on a
// self-integrity/manifest failure, BitrotOn when usable.
func loadRebuildSidecar(baseFileName string, ctx *ECContext, additionalDirs []string) (*volume_server_pb.EcBitrotProtection, BitrotStatus) {
	path := findBitrotSidecar(0, baseFileName, baseFileName, additionalDirs...)
	if path == "" {
		return nil, BitrotOff
	}
	prot, err := LoadBitrotSidecar(path)
	if err != nil {
		glog.Warningf("bitrot: sidecar %s self-integrity failed: %v", path, err)
		return nil, BitrotInvalid
	}
	if prot.Generation != 0 {
		return nil, BitrotOff
	}
	if prot.EcShardConfig == nil ||
		int(prot.EcShardConfig.DataShards) != ctx.DataShards ||
		int(prot.EcShardConfig.ParityShards) != ctx.ParityShards {
		return nil, BitrotOff
	}
	if err := ValidateBitrotManifest(prot, ctx.DataShards, ctx.ParityShards); err != nil {
		glog.Warningf("bitrot: sidecar %s manifest invalid: %v", path, err)
		return nil, BitrotInvalid
	}
	return prot, BitrotOn
}

func encodeData(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext, builders []*shardChecksumBuilder) error {

	bufferSize := int64(len(buffers[0]))
	if bufferSize == 0 {
		glog.Fatal("unexpected zero buffer size")
	}

	batchCount := blockSize / bufferSize
	if blockSize%bufferSize != 0 {
		glog.Fatalf("unexpected block size %d buffer size %d", blockSize, bufferSize)
	}

	for b := int64(0); b < batchCount; b++ {
		err := encodeDataOneBatch(file, enc, startOffset+b*bufferSize, blockSize, buffers, outputs, ctx, builders)
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

func encodeDataOneBatch(file *os.File, enc reedsolomon.Encoder, startOffset, blockSize int64, buffers [][]byte, outputs []*os.File, ctx *ECContext, builders []*shardChecksumBuilder) error {

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
		// Accumulate this shard's block CRC over exactly the bytes written.
		if builders != nil && builders[i] != nil {
			builders[i].write(buffers[i])
		}
	}

	return nil
}

func encodeDatFile(remainingSize int64, baseFileName string, bufferSize int, largeBlockSize int64, file *os.File, smallBlockSize int64, ctx *ECContext, builders []*shardChecksumBuilder) error {

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
		err = encodeData(file, enc, processedSize, largeBlockSize, buffers, outputs, ctx, builders)
		if err != nil {
			return fmt.Errorf("failed to encode large chunk data: %w", err)
		}
		remainingSize -= largeRowSize
		processedSize += largeRowSize
	}
	for remainingSize > 0 {
		err = encodeData(file, enc, processedSize, smallBlockSize, buffers, outputs, ctx, builders)
		if err != nil {
			return fmt.Errorf("failed to encode small chunk data: %w", err)
		}
		remainingSize -= smallRowSize
		processedSize += smallRowSize
	}
	return nil
}

func rebuildEcFiles(shardHasData []bool, inputFiles []*os.File, outputFiles []*os.File, ctx *ECContext) error {

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

	var startOffset int64
	var inputBufferDataSize int
	for {

		// read the input data from files
		for i := 0; i < ctx.Total(); i++ {
			if shardHasData[i] {
				n, _ := inputFiles[i].ReadAt(buffers[i], startOffset)
				if n == 0 {
					return nil
				}
				if inputBufferDataSize == 0 {
					inputBufferDataSize = n
				}
				if inputBufferDataSize != n {
					return fmt.Errorf("ec shard size expected %d actual %d", inputBufferDataSize, n)
				}
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
				n, _ := outputFiles[i].WriteAt(buffers[i][:inputBufferDataSize], startOffset)
				if inputBufferDataSize != n {
					return fmt.Errorf("fail to write to %s", outputFiles[i].Name())
				}
			}
		}
		startOffset += int64(inputBufferDataSize)
	}

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
