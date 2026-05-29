package command

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdFix.Run = runFix // break init cycle
}

var cmdFix = &Command{
	UsageLine: "fix [-remoteFile=false] [-volumeId=234] [-collection=bigData] /tmp",
	Short:     "run weed tool fix on files or whole folders to recreate index file(s) if corrupted",
	Long: `Fix runs the SeaweedFS fix command on local dat files ( or remote files) or whole folders to re-create the index .idx file. If fixing remote files, you need to synchronize master.toml to the same directory on the current node as on the master node.
  You Need to stop the volume server when running this command.
  Use -ecx to rebuild a lost EC index (.ecx) — and the .vif when missing — from the local .ec## shards.
`,
}

var (
	fixVolumeCollection = cmdFix.Flag.String("collection", "", "an optional volume collection name, if specified only it will be processed")
	fixVolumeId         = cmdFix.Flag.Int64("volumeId", 0, "an optional volume id, if not 0 (default) only it will be processed")
	fixIncludeDeleted   = cmdFix.Flag.Bool("includeDeleted", true, "include deleted entries in the index file")
	fixIgnoreError      = cmdFix.Flag.Bool("ignoreError", false, "an optional, if true will be processed despite errors")
	fixRemoteFile       = cmdFix.Flag.Bool("remoteFile", false, "an optional, if true will not try to load the local .dat file, but only the remote file")
	fixGenerateEcx      = cmdFix.Flag.Bool("ecx", false, "regenerate a lost EC index (.ecx) — and the .vif when missing — from the local .ec## shards (missing shards are reconstructed from parity when enough survive). Run with the volume server stopped.")
	fixEcDataShards     = cmdFix.Flag.Int("ecDataShards", 0, "EC data shard count for -ecx (0 = read from .vif, otherwise default 10)")
	fixEcParityShards   = cmdFix.Flag.Int("ecParityShards", 0, "EC parity shard count for -ecx (0 = read from .vif, infer from shard count, otherwise default 4)")
)

type VolumeFileScanner4Fix struct {
	version        needle.Version
	nm             *needle_map.MemDb
	nmDeleted      *needle_map.MemDb
	includeDeleted bool
}

func (scanner *VolumeFileScanner4Fix) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil
}

func (scanner *VolumeFileScanner4Fix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4Fix) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	glog.V(2).Infof("key %v offset %d size %d disk_size %d compressed %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed())
	if n.Size.IsValid() {
		if pe := scanner.nm.Set(n.Id, types.ToOffset(offset), n.Size); pe != nil {
			return fmt.Errorf("saved %d with error %v", n.Size, pe)
		}
	} else {
		if scanner.includeDeleted {
			if pe := scanner.nmDeleted.Set(n.Id, types.ToOffset(offset), types.TombstoneFileSize); pe != nil {
				return fmt.Errorf("saved deleted %d with error %v", n.Size, pe)
			}
		} else {
			glog.V(2).Infof("skipping deleted file ...")
			return scanner.nm.Delete(n.Id)
		}
	}
	return nil
}

func runFix(cmd *Command, args []string) bool {
	for _, arg := range args {
		basePath, f := path.Split(util.ResolvePath(arg))
		if util.FolderExists(arg) {
			basePath = arg
			f = ""
		}

		files := []fs.DirEntry{}
		if f == "" {
			fileInfo, err := os.ReadDir(basePath)
			if err != nil {
				fmt.Println(err)
				return false
			}
			files = fileInfo
		} else {
			fileInfo, err := os.Stat(arg)
			if err != nil {
				fmt.Println(err)
				return false
			}
			files = []fs.DirEntry{fs.FileInfoToDirEntry(fileInfo)}
		}

		ext := ".dat"
		if *fixRemoteFile {
			ext = ".idx"
			util.LoadConfiguration("master", false)
			backend.LoadConfiguration(util.GetViper())
		}

		for _, file := range files {
			if !strings.HasSuffix(file.Name(), ext) {
				continue
			}
			if *fixVolumeCollection != "" {
				if !strings.HasPrefix(file.Name(), *fixVolumeCollection+"_") {
					continue
				}
			}
			baseFileName := file.Name()[:len(file.Name())-4]
			collection, volumeIdStr := "", baseFileName
			if sepIndex := strings.LastIndex(baseFileName, "_"); sepIndex > 0 {
				collection = baseFileName[:sepIndex]
				volumeIdStr = baseFileName[sepIndex+1:]
			}
			volumeId, parseErr := strconv.ParseInt(volumeIdStr, 10, 64)
			if parseErr != nil {
				fmt.Printf("Failed to parse volume id from %s: %v\n", baseFileName, parseErr)
				return false
			}
			if *fixVolumeId != 0 && *fixVolumeId != volumeId {
				continue
			}
			doFixOneVolume(basePath, baseFileName, collection, volumeId, *fixIncludeDeleted)
		}

		if *fixGenerateEcx {
			if !fixEcxFromShardsInDir(basePath, files) {
				return false
			}
		}
	}
	return true
}

// fixEcxFromShardsInDir finds EC volumes in files (identified by their .ec00
// data shard) and regenerates the .ecx (and .vif when missing) for each,
// honoring the -collection and -volumeId filters.
func fixEcxFromShardsInDir(basePath string, files []fs.DirEntry) bool {
	const shard0Ext = ".ec00"
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), shard0Ext) {
			continue
		}
		if *fixVolumeCollection != "" {
			if !strings.HasPrefix(file.Name(), *fixVolumeCollection+"_") {
				continue
			}
		}
		baseFileName := file.Name()[:len(file.Name())-len(shard0Ext)]
		collection, volumeIdStr := "", baseFileName
		if sepIndex := strings.LastIndex(baseFileName, "_"); sepIndex > 0 {
			collection = baseFileName[:sepIndex]
			volumeIdStr = baseFileName[sepIndex+1:]
		}
		volumeId, parseErr := strconv.ParseInt(volumeIdStr, 10, 64)
		if parseErr != nil {
			fmt.Printf("Failed to parse volume id from %s: %v\n", baseFileName, parseErr)
			return false
		}
		if *fixVolumeId != 0 && *fixVolumeId != volumeId {
			continue
		}
		doFixEcxFromShards(basePath, baseFileName, collection, volumeId)
	}
	return true
}

func SaveToIdx(scaner *VolumeFileScanner4Fix, idxName string) (ret error) {
	idxFile, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() {
		idxFile.Close()
	}()

	// Emit entries in .dat offset (append) order so the .idx stays the
	// append-ordered log the volume server writes at runtime — not sorted by
	// key, which is the .sdx / .ecx shape. A key-sorted .idx puts the
	// highest-key needle last instead of the .dat-tail needle, which used to
	// flip volumes read-only on load. Every tombstone is included (including
	// any whose live needle is gone) so the last entry is the real .dat tail.
	var values []needle_map.NeedleValue
	collect := func(value needle_map.NeedleValue) error {
		values = append(values, value)
		return nil
	}
	if err = scaner.nm.AscendingVisit(collect); err != nil {
		return err
	}
	if scaner.includeDeleted {
		if err = scaner.nmDeleted.AscendingVisit(collect); err != nil {
			return err
		}
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Offset.ToActualOffset() < values[j].Offset.ToActualOffset()
	})
	for _, value := range values {
		if _, err = idxFile.Write(value.ToBytes()); err != nil {
			return err
		}
	}
	return nil
}

func doFixOneVolume(basepath string, baseFileName string, collection string, volumeId int64, fixIncludeDeleted bool) {
	indexFileName := path.Join(basepath, baseFileName+".idx")

	nm := needle_map.NewMemDb()
	nmDeleted := needle_map.NewMemDb()
	defer nm.Close()
	defer nmDeleted.Close()

	// Validate volumeId range before converting to uint32
	if volumeId < 0 || volumeId > 0xFFFFFFFF {
		err := fmt.Errorf("volume ID out of range: %d", volumeId)
		if *fixIgnoreError {
			glog.Error(err)
			return
		} else {
			glog.Fatal(err)
		}
	}
	// lgtm[go/incorrect-integer-conversion]
	// Safe conversion: volumeId has been validated to be in range [0, 0xFFFFFFFF] above
	vid := needle.VolumeId(volumeId)
	scanner := &VolumeFileScanner4Fix{
		nm:             nm,
		nmDeleted:      nmDeleted,
		includeDeleted: fixIncludeDeleted,
	}

	if err := storage.ScanVolumeFile(basepath, collection, vid, storage.NeedleMapInMemory, scanner); err != nil {
		err := fmt.Errorf("scan .dat File: %w", err)
		if *fixIgnoreError {
			glog.Error(err)
		} else {
			glog.Fatal(err)
		}
	}

	if err := SaveToIdx(scanner, indexFileName); err != nil {
		err := fmt.Errorf("save to .idx File: %w", err)
		if *fixIgnoreError {
			glog.Error(err)
		} else {
			if err := os.Remove(indexFileName); err != nil {
				glog.Errorf("failed to cleanup file %s:%v", indexFileName, err)
			}
			glog.Fatal(err)
		}
	}
}

// doFixEcxFromShards rebuilds the sealed EC index (.ecx) for one EC volume
// directly from its local shards when both the .ecx and the original .dat are
// gone but the shards survive. When some data shards are missing but at least
// dataShards shards survive in total, the missing shards are first reconstructed
// from the survivors via Reed-Solomon. It then de-stripes the data shards into a
// temporary .dat, scans the needles, and writes a fresh ascending-sorted .ecx
// that matches what WriteSortedFileFromIdx emits at encode time (live entries
// only). When the .vif is also missing it is regenerated from the inferred EC
// ratio and the .dat size discovered during the scan.
func doFixEcxFromShards(basePath, baseFileName, collection string, volumeId int64) {
	base := path.Join(basePath, baseFileName)

	fail := func(err error) {
		if *fixIgnoreError {
			glog.Error(err)
		} else {
			glog.Fatal(err)
		}
	}

	ecxName := base + ".ecx"
	if info, err := os.Stat(ecxName); err == nil && info.Size() > 0 {
		glog.Infof("volume %d: %s already exists (%d bytes), skipping; remove it first to force regeneration", volumeId, ecxName, info.Size())
		return
	}

	// Discover which shards are present and their common size. Reed-Solomon
	// requires every shard to be the same size.
	present := make([]bool, erasure_coding.MaxShardCount)
	presentCount := 0
	maxPresentIdx := -1
	var shardSize int64
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		info, statErr := os.Stat(base + erasure_coding.ToExt(i))
		if statErr != nil || info.Size() == 0 {
			continue
		}
		if shardSize == 0 {
			shardSize = info.Size()
		} else if info.Size() != shardSize {
			fail(fmt.Errorf("volume %d: shard %s size %d does not match %d", volumeId, base+erasure_coding.ToExt(i), info.Size(), shardSize))
			return
		}
		present[i] = true
		presentCount++
		maxPresentIdx = i
	}
	if presentCount == 0 {
		fail(fmt.Errorf("volume %d: no EC shards found under %s", volumeId, base))
		return
	}

	// Resolve the EC ratio and the original .dat size.
	// Priority: explicit flags > existing .vif > defaults (10+4).
	vifName := base + ".vif"
	vifExists := util.FileExists(vifName)
	dataShards := erasure_coding.DataShardsCount
	parityShards := erasure_coding.ParityShardsCount
	var datFileSize int64
	if vifExists {
		// MaybeLoadVolumeInfo returns a non-nil error when the .vif exists but
		// cannot be read or unmarshalled; fail loudly rather than silently
		// falling back to defaults (which would be wrong for a custom ratio).
		if vi, _, found, loadErr := volume_info.MaybeLoadVolumeInfo(vifName); loadErr != nil {
			fail(fmt.Errorf("volume %d: read %s: %w", volumeId, vifName, loadErr))
			return
		} else if found && vi != nil {
			if cfg := vi.GetEcShardConfig(); cfg != nil && cfg.GetDataShards() > 0 {
				dataShards = int(cfg.GetDataShards())
				parityShards = int(cfg.GetParityShards())
			}
			datFileSize = vi.GetDatFileSize()
		}
	}
	if *fixEcDataShards > 0 {
		dataShards = *fixEcDataShards
	}
	if *fixEcParityShards > 0 {
		parityShards = *fixEcParityShards
	}
	// Ensure the configured total covers every shard index actually present
	// (a custom-ratio volume with more than the default 14 shards and no .vif).
	// This never lowers parity below the default, so the common 10+4 case stays
	// correct for any subset of missing shards.
	if maxPresentIdx+1 > dataShards+parityShards {
		parityShards = maxPresentIdx + 1 - dataShards
	}
	if dataShards <= 0 || parityShards <= 0 || dataShards+parityShards > erasure_coding.MaxShardCount {
		fail(fmt.Errorf("volume %d: cannot determine EC ratio (data=%d parity=%d); set -ecDataShards/-ecParityShards", volumeId, dataShards, parityShards))
		return
	}

	// Need at least dataShards shards (any data+parity mix) to recover anything.
	if presentCount < dataShards {
		fail(fmt.Errorf("volume %d: only %d shards present, need at least %d (data shards) to recover", volumeId, presentCount, dataShards))
		return
	}

	// If any data shard is missing, reconstruct the missing shards from the
	// survivors via Reed-Solomon before de-striping. This writes the rebuilt
	// shard files back to disk, fully repairing the volume locally.
	dataComplete := true
	for i := 0; i < dataShards; i++ {
		if !present[i] {
			dataComplete = false
			break
		}
	}
	if !dataComplete {
		ctx := &erasure_coding.ECContext{DataShards: dataShards, ParityShards: parityShards}
		glog.Infof("volume %d: %d/%d shards present; reconstructing missing shards (%s) before index rebuild", volumeId, presentCount, dataShards+parityShards, ctx.String())
		if _, err := erasure_coding.RebuildEcFilesWithContext(base, ctx); err != nil {
			fail(fmt.Errorf("volume %d: reconstruct missing shards from %d survivors: %w", volumeId, presentCount, err))
			return
		}
	}

	// Collect the data shards (now all present).
	shardFileNames := make([]string, dataShards)
	for i := 0; i < dataShards; i++ {
		shardPath := base + erasure_coding.ToExt(i)
		if !util.FileExists(shardPath) {
			fail(fmt.Errorf("volume %d: data shard %s still missing after reconstruction", volumeId, shardPath))
			return
		}
		shardFileNames[i] = shardPath
	}

	// Without a recorded original size, reconstruct the fully padded layout; the
	// scan below detects the trailing zero padding and recovers the true size.
	reconstructSize := datFileSize
	if reconstructSize <= 0 {
		reconstructSize = int64(dataShards) * shardSize
		glog.V(0).Infof("volume %d: no .dat size in .vif; reconstructing padded .dat (%d bytes) from %d data shards", volumeId, reconstructSize, dataShards)
	}

	// De-stripe the data shards into a temporary .dat next to the shards.
	tmpBase := base + ".ecxrecover"
	tmpDat := tmpBase + ".dat"
	if err := erasure_coding.WriteDatFile(tmpBase, reconstructSize, shardFileNames); err != nil {
		os.Remove(tmpDat)
		fail(fmt.Errorf("volume %d: reconstruct .dat from data shards: %w", volumeId, err))
		return
	}
	defer os.Remove(tmpDat)

	realDatSize, version, err := writeEcxFromDat(tmpDat, ecxName)
	if err != nil {
		os.Remove(ecxName)
		fail(fmt.Errorf("volume %d: build .ecx from reconstructed .dat: %w", volumeId, err))
		return
	}
	glog.Infof("volume %d: wrote %s from %d data shards", volumeId, ecxName, dataShards)

	// Regenerate the .vif when missing so the volume can mount and future
	// rebuilds know the EC ratio and original .dat size.
	if !vifExists {
		size := datFileSize
		if size <= 0 {
			size = realDatSize
		}
		volumeInfo := &volume_server_pb.VolumeInfo{
			Version:     uint32(version),
			DatFileSize: size,
			EcShardConfig: &volume_server_pb.EcShardConfig{
				DataShards:   uint32(dataShards),
				ParityShards: uint32(parityShards),
			},
		}
		if err := volume_info.SaveVolumeInfo(vifName, volumeInfo); err != nil {
			fail(fmt.Errorf("volume %d: write %s: %w", volumeId, vifName, err))
			return
		}
		glog.Infof("volume %d: wrote %s (version %d, datFileSize %d, ec %d+%d)", volumeId, vifName, version, size, dataShards, parityShards)
	}
}

// writeEcxFromDat scans a (reconstructed) .dat and writes an ascending-sorted
// .ecx containing only live needles — the same on-disk shape
// WriteSortedFileFromIdx produces when an EC volume is first encoded. It returns
// the physical .dat size (the offset where the EC zero padding begins) and the
// volume version read from the superblock.
func writeEcxFromDat(datPath, ecxPath string) (datFileSize int64, version needle.Version, err error) {
	f, err := os.OpenFile(datPath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("open %s: %w", datPath, err)
	}
	datBackend := backend.NewDiskFile(f)
	defer datBackend.Close()

	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		return 0, 0, fmt.Errorf("read superblock: %w", err)
	}
	version = superBlock.Version

	fileSize, _, err := datBackend.GetStat()
	if err != nil {
		return 0, version, fmt.Errorf("stat %s: %w", datPath, err)
	}

	nm := needle_map.NewMemDb()
	defer nm.Close()

	offset := int64(superBlock.BlockSize())
	for offset < fileSize {
		n, _, rest, readErr := needle.ReadNeedleHeader(datBackend, version, offset)
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return 0, version, fmt.Errorf("read needle header at offset %d: %w", offset, readErr)
		}
		// EC encoding zero-pads the tail of the last block row. An all-zero
		// header marks the start of that padding, i.e. the end of real needles.
		if n.Cookie == 0 && n.Id == 0 && n.Size == 0 {
			break
		}
		if n.Size.IsValid() {
			if pe := nm.Set(n.Id, types.ToOffset(offset), n.Size); pe != nil {
				return 0, version, fmt.Errorf("set needle %d: %w", n.Id, pe)
			}
		} else {
			// Deleted/invalid: drop it so the .ecx carries only live entries,
			// matching the encode-time WriteSortedFileFromIdx behavior.
			if pe := nm.Delete(n.Id); pe != nil {
				return 0, version, fmt.Errorf("delete needle %d: %w", n.Id, pe)
			}
		}
		offset += types.NeedleHeaderSize + rest
	}
	datFileSize = offset

	ecxFile, err := os.OpenFile(ecxPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, version, fmt.Errorf("open %s: %w", ecxPath, err)
	}
	defer ecxFile.Close()

	if err := nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		_, writeErr := ecxFile.Write(value.ToBytes())
		return writeErr
	}); err != nil {
		return 0, version, fmt.Errorf("write %s: %w", ecxPath, err)
	}

	return datFileSize, version, nil
}
