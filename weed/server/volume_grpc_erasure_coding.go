package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcShardsGenerate to generate the .ecx and .ec00 ~ .ec13 files
2. client ask master for possible servers to hold the ec files
3. client call VolumeEcShardsCopy on above target servers to copy ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

*/

// VolumeEcShardsGenerate generates the .ecx and .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsGenerate: %v", req)

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	baseFileName := v.DataFileName()

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// Create EC context - prefer existing .vif config if present (for regeneration scenarios)
	ecCtx := erasure_coding.NewDefaultECContext(req.Collection, needle.VolumeId(req.VolumeId))
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(baseFileName + ".vif"); found && volumeInfo.EcShardConfig != nil {
		ds := int(volumeInfo.EcShardConfig.DataShards)
		ps := int(volumeInfo.EcShardConfig.ParityShards)

		// Validate and use existing EC config
		if ds > 0 && ps > 0 && ds+ps <= erasure_coding.MaxShardCount {
			ecCtx.DataShards = ds
			ecCtx.ParityShards = ps
			glog.V(0).Infof("Using existing EC config for volume %d: %s", req.VolumeId, ecCtx.String())
		} else {
			glog.Warningf("Invalid EC config in .vif for volume %d (data=%d, parity=%d), using defaults", req.VolumeId, ds, ps)
		}
	} else {
		glog.V(0).Infof("Using default EC config for volume %d: %s", req.VolumeId, ecCtx.String())
	}

	shouldCleanup := true
	defer func() {
		if !shouldCleanup {
			return
		}
		for i := 0; i < ecCtx.Total(); i++ {
			os.Remove(baseFileName + ecCtx.ToExt(i))
		}
		os.Remove(v.IndexFileName() + ".ecx")
		os.Remove(erasure_coding.BitrotSidecarPath(baseFileName, 0))
	}()

	// Wipe any EC artifacts from a prior encode so a retry never mixes two runs.
	// Evict the in-memory EcVolume first so the unlink frees the inodes instead
	// of leaving open fds serving the old bytes. Sweep every disk: stale shards
	// can sit on a sibling disk and would otherwise survive and be mounted
	// against the new index at reconcile. Scans the cap for custom ratios.
	vs.store.UnloadEcVolume(needle.VolumeId(req.VolumeId))
	for _, loc := range vs.store.Locations {
		dataBase := storage.VolumeFileName(loc.Directory, req.Collection, int(req.VolumeId))
		idxBase := storage.VolumeFileName(loc.IdxDirectory, req.Collection, int(req.VolumeId))
		if err := removeStaleEcArtifacts(dataBase, idxBase, erasure_coding.MaxShardCount); err != nil {
			return nil, fmt.Errorf("wipe stale EC artifacts for volume %d on %s: %w", req.VolumeId, loc.Directory, err)
		}
	}

	// IMPORTANT: Generate .ecx BEFORE EC shards to prevent a race condition.
	// If .ecx were generated after EC shards, any write (e.g. from WriteNeedleBlob
	// during replica sync) between the two steps would add entries to .idx that
	// end up in .ecx but whose data is NOT in the EC shards — causing "shard too
	// short" and "size mismatch" errors on reads.
	//
	// By generating .ecx first, it reflects the .idx state at or before the .dat
	// is read for EC encoding. If a write sneaks in after .ecx but before/during
	// EC encoding, the shards contain MORE data than .ecx references, which is
	// harmless (the extra data is simply not indexed).

	// write .ecx file from the current .idx
	if err := erasure_coding.WriteSortedFileFromIdx(v.IndexFileName(), ".ecx"); err != nil {
		return nil, fmt.Errorf("WriteSortedFileFromIdx %s: %v", v.IndexFileName(), err)
	}

	// snapshot .dat file size before encoding — must match what .ecx references
	datSize, _, _ := v.FileStat()

	// write .ec00 ~ .ec[TotalShards-1] files using context
	ecBitrot, err := erasure_coding.WriteEcFiles(baseFileName, ecCtx)
	if err != nil {
		return nil, fmt.Errorf("WriteEcFiles %s: %v", baseFileName, err)
	}
	// Persist the generation-0 bitrot checksum sidecar (<base>.ecsum) alongside
	// the shards so it travels with them during distribution (copy_ecsum_file).
	// The source loading its own canonical sidecar is correct — it holds all
	// shards and a complete manifest. Best-effort: a failed sidecar write leaves
	// the generation unprotected rather than failing the encode.
	if erasure_coding.BitrotProtectionEnabled && ecBitrot != nil {
		if serr := erasure_coding.SaveBitrotSidecar(erasure_coding.BitrotSidecarPath(baseFileName, 0), ecBitrot); serr != nil {
			glog.Warningf("failed to write EC bitrot sidecar for volume %d: %v", req.VolumeId, serr)
		}
	}

	// write .vif files
	var expireAtSec uint64
	if v.Ttl != nil {
		ttlSecond := v.Ttl.ToSeconds()
		if ttlSecond > 0 {
			expireAtSec = uint64(time.Now().Unix()) + ttlSecond //calculated expiration time
		}
	}
	volumeInfo := &volume_server_pb.VolumeInfo{Version: uint32(v.Version())}
	volumeInfo.ExpireAtSec = expireAtSec
	volumeInfo.DatFileSize = int64(datSize)

	// Validate EC configuration before saving to .vif
	if ecCtx.DataShards <= 0 || ecCtx.ParityShards <= 0 || ecCtx.Total() > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid EC config before saving: data=%d, parity=%d, total=%d (max=%d)",
			ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total(), erasure_coding.MaxShardCount)
	}

	// EncodeTsNs stamps this run's identity into the .vif (copied with the
	// shards), so a read served from a different run's shard is rejected.
	volumeInfo.EcShardConfig = &volume_server_pb.EcShardConfig{
		DataShards:   uint32(ecCtx.DataShards),
		ParityShards: uint32(ecCtx.ParityShards),
		EncodeTsNs:   time.Now().UnixNano(),
	}
	glog.V(1).Infof("Saving EC config to .vif for volume %d: %d+%d (total: %d)",
		req.VolumeId, ecCtx.DataShards, ecCtx.ParityShards, ecCtx.Total())

	if err := volume_info.SaveVolumeInfo(baseFileName+".vif", volumeInfo); err != nil {
		return nil, fmt.Errorf("SaveVolumeInfo %s: %v", baseFileName, err)
	}

	shouldCleanup = false

	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

func recordEcRebuild(result string, d time.Duration) {
	stats.VolumeServerECRebuildHistogram.WithLabelValues(result).Observe(d.Seconds())
	stats.VolumeServerECRebuildCounter.WithLabelValues(result).Inc()
}

// VolumeEcShardsRebuild generates the any of the missing .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsRebuild: %v", req)
	baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	var rebuiltShardIds []uint32

	// Find the rebuild location: the location with the most shards and an .ecx file.
	// With multi-disk servers, shards may be spread across different locations.
	var rebuildLocation *storage.DiskLocation
	var rebuildShardCount int
	var otherLocationsWithShards []*storage.DiskLocation

	for _, location := range vs.store.Locations {
		_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, location)
		if err != nil {
			return nil, err
		}

		indexBaseFileName := path.Join(location.IdxDirectory, baseFileName)
		if !util.FileExists(indexBaseFileName+".ecx") && location.IdxDirectory != location.Directory {
			indexBaseFileName = path.Join(location.Directory, baseFileName)
		}
		hasEcx := util.FileExists(indexBaseFileName + ".ecx")

		// Skip locations that have neither shard files nor an .ecx file.
		if existingShardCount == 0 && !hasEcx {
			continue
		}

		if hasEcx && (rebuildLocation == nil || existingShardCount > rebuildShardCount) {
			if rebuildLocation != nil {
				otherLocationsWithShards = append(otherLocationsWithShards, rebuildLocation)
			}
			rebuildLocation = location
			rebuildShardCount = existingShardCount
		} else {
			otherLocationsWithShards = append(otherLocationsWithShards, location)
		}
	}

	if rebuildLocation == nil {
		return &volume_server_pb.VolumeEcShardsRebuildResponse{}, nil
	}

	// Collect additional directories where shard files may exist.
	// On multi-disk servers, existing local shards may be on a different disk
	// than where copied shards were placed during ec.rebuild.
	rebuildDataDir := rebuildLocation.Directory
	var additionalDirs []string
	for _, otherLocation := range otherLocationsWithShards {
		additionalDirs = append(additionalDirs, otherLocation.Directory)
	}

	// Rebuild missing EC files, searching all disk locations for input shards.
	// Present input shards are verified against the bitrot sidecar (when present)
	// and corrupt ones are regenerated; unsafe_ignore_sidecar bypasses the guard.
	start := time.Now()
	dataBaseFileName := path.Join(rebuildDataDir, baseFileName)
	generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName, erasure_coding.BackgroundECContext(), req.UnsafeIgnoreSidecar, additionalDirs...)
	if err != nil {
		recordEcRebuild("failure", time.Since(start))
		return nil, fmt.Errorf("RebuildEcFiles %s: %v", dataBaseFileName, err)
	}
	rebuiltShardIds = generatedShardIds

	indexBaseFileName := path.Join(rebuildLocation.IdxDirectory, baseFileName)
	if !util.FileExists(indexBaseFileName+".ecx") && rebuildLocation.IdxDirectory != rebuildLocation.Directory {
		indexBaseFileName = path.Join(rebuildLocation.Directory, baseFileName)
	}
	if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
		recordEcRebuild("failure", time.Since(start))
		return nil, fmt.Errorf("RebuildEcxFile %s: %v", indexBaseFileName, err)
	}

	recordEcRebuild("success", time.Since(start))

	// Opportunistic bitrot backfill: if protection is enabled, no sidecar exists
	// yet (a volume encoded before this feature), and this rebuilder can reach
	// every shard, compute and write a generation-0 sidecar. The TOFU baseline
	// blesses current bytes; ComputeProtectionFromShards refuses a partial
	// manifest, so a multi-server rebuild that cannot reach all shards just skips.
	if erasure_coding.BitrotProtectionEnabled {
		sidecarPath := erasure_coding.BitrotSidecarPath(dataBaseFileName, 0)
		if _, statErr := os.Stat(sidecarPath); os.IsNotExist(statErr) {
			ctx := erasure_coding.NewDefaultECContext("", 0)
			if vi, _, found, _ := volume_info.MaybeLoadVolumeInfo(dataBaseFileName + ".vif"); found && vi.EcShardConfig != nil {
				if ds, ps := int(vi.EcShardConfig.DataShards), int(vi.EcShardConfig.ParityShards); ds > 0 && ps > 0 && ds+ps <= erasure_coding.MaxShardCount {
					ctx = &erasure_coding.ECContext{DataShards: ds, ParityShards: ps}
				}
			}
			if prot, berr := erasure_coding.ComputeProtectionFromShards(dataBaseFileName, ctx, 0, additionalDirs); berr != nil {
				glog.V(2).Infof("bitrot backfill skipped for %s: %v", dataBaseFileName, berr)
			} else if werr := erasure_coding.SaveBitrotSidecar(sidecarPath, prot); werr != nil {
				glog.Warningf("bitrot backfill: write sidecar for %s: %v", dataBaseFileName, werr)
			} else {
				glog.V(0).Infof("bitrot backfill: wrote sidecar for %s after rebuild", dataBaseFileName)
			}
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	var location *storage.DiskLocation

	// Select the target location for storing EC shard files.
	//
	// When req.DiskId > 0 the caller is explicitly choosing a disk:
	//   location = vs.store.Locations[req.DiskId]
	//   (DiskId=1 → Locations[1], DiskId=2 → Locations[2], etc.)
	//
	// When req.DiskId == 0 (the protobuf default, meaning "not specified")
	// we auto-select location by preferring the disk that already holds EC
	// shards for this volume, then falling back to any HDD, then any disk.
	//
	// Note: Locations[0] cannot be targeted explicitly via DiskId because 0
	// is indistinguishable from "unset". It can still be chosen by the
	// auto-select logic.
	if req.DiskId > 0 {
		// Validate disk ID is within bounds
		if int(req.DiskId) >= len(vs.store.Locations) {
			return nil, fmt.Errorf("invalid disk_id %d: only have %d disks", req.DiskId, len(vs.store.Locations))
		}

		// Use the specific disk location
		location = vs.store.Locations[req.DiskId]
		glog.V(1).Infof("Using disk %d for EC shard copy: %s", req.DiskId, location.Directory)
	} else {
		// Auto-select the target disk: prefer a disk that already has the
		// EC volume mounted, then a disk that owns the .ecx on disk (the
		// volume hasn't been mounted yet — relevant for ec.rebuild, where
		// only the first shard carries .ecx and subsequent shards must
		// land on the same disk; see #9212), then any HDD, then any disk.
		// Pass the build's default data-shard count for free-slot maths;
		// the helper takes it as a parameter so custom-ratio builds (e.g.
		// enterprise) can swap it without touching this file.
		location = vs.store.FindEcShardTargetLocation(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.DataShardsCount)
		if location == nil {
			return nil, fmt.Errorf("no space left")
		}
	}

	dataBaseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy ec data slices
		for _, shardId := range req.ShardIds {
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.ToExt(int(shardId)), false, false, nil); err != nil {
				return err
			}
		}

		if req.CopyEcxFile {

			// copy ecx file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecx", false, false, nil); err != nil {
				return err
			}
			// Defense in depth: writeToFile now removes partial files on
			// stream error, but a source that genuinely held a 0-byte
			// .ecx (e.g. a corrupted upstream replica) would otherwise
			// leave a 0-byte file here and the mount path would reject
			// it later. Catch that at distribute time so the orchestrator
			// can pick a different source rather than learning about it
			// at mount.
			// Stat failure must not silently pass. doCopyFile reported
			// success, but if the file is gone, unreadable, or a directory
			// somehow, the orchestrator should learn now — at mount time
			// the operator only sees "no .ecx found" with no useful context
			// about which step actually failed.
			ecxPath := indexBaseFileName + ".ecx"
			info, statErr := os.Stat(ecxPath)
			if statErr != nil {
				return fmt.Errorf("VolumeEcShardsCopy volume %d: stat copied .ecx %s: %w", req.VolumeId, ecxPath, statErr)
			}
			if info.IsDir() {
				return fmt.Errorf("VolumeEcShardsCopy volume %d: copied .ecx path %s is a directory", req.VolumeId, ecxPath)
			}
			if info.Size() == 0 {
				if removeErr := os.Remove(ecxPath); removeErr != nil && !os.IsNotExist(removeErr) {
					glog.Warningf("VolumeEcShardsCopy volume %d: remove 0-byte .ecx %s: %v", req.VolumeId, ecxPath, removeErr)
				}
				return fmt.Errorf("VolumeEcShardsCopy volume %d: source .ecx is 0 bytes", req.VolumeId)
			}
		}

		if req.CopyEcjFile {
			// copy ecj file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecj", true, true, nil); err != nil {
				return err
			}
		}

		if req.CopyVifFile {
			// copy vif file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				return err
			}
		}

		if req.CopyEcsumFile {
			// Propagate the generation-0 bitrot checksum sidecar when the source
			// has one. This non-2PC copy path (balance / fresh-encode / rebuild
			// distribution) has no Prepare backstop, and fresh-encode sidecar
			// writes are best-effort, so a missing source sidecar is a no-op
			// (ignore-not-found): the holder is simply unprotected.
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.BitrotSidecarExt, false, true, nil); err != nil {
				return fmt.Errorf("VolumeEcShardsCopy volume %d: copy %s sidecar: %w", req.VolumeId, erasure_coding.BitrotSidecarExt, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete local delete the .ecx and some ec data slices if not needed
// the shard should not be mounted before calling this.
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		return nil, err
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	bName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	if req.FullTeardown {
		if req.EncodeTsNs == 0 {
			// Blanket teardown (shell pre-encode cleanup / pre-upgrade caller): evict the
			// volume and wipe every EC artifact for it on every disk, not just the listed
			// shards, so a remote node retains no stale generation a fresh copy collides with.
			glog.V(0).Infof("ec volume %s full teardown", bName)
			vs.store.UnloadEcVolume(needle.VolumeId(req.VolumeId))
			for _, location := range vs.store.Locations {
				dataBase := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
				idxBase := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))
				if err := removeStaleEcArtifacts(dataBase, idxBase, erasure_coding.MaxShardCount); err != nil {
					return nil, fmt.Errorf("full teardown of ec volume %d on %s: %w", req.VolumeId, location.Directory, err)
				}
			}
			return &volume_server_pb.VolumeEcShardsDeleteResponse{FullTeardownDone: true}, nil
		}
		// Generation-fenced teardown (stale-worker pre-distribute cleanup): wipe only a
		// disk whose .vif generation is strictly OLDER than the request; preserve
		// same-or-newer, generation 0 (recovered/pre-upgrade live volume), and an
		// unreadable .vif, so a stale run can never wipe a newer run's live shards.
		// Unload and remove only the strictly-older disks, never node-wide.
		glog.V(0).Infof("ec volume %s full teardown fenced at generation %d", bName, req.EncodeTsNs)
		for _, location := range vs.store.Locations {
			dataBase := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
			idxBase := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))
			diskGen, readable := readEcGenerationTsNs(dataBase, idxBase)
			if !readable || diskGen == 0 || diskGen >= req.EncodeTsNs {
				glog.V(1).Infof("ec volume %d on %s preserved: disk generation %d (readable=%v) not older than request %d", req.VolumeId, location.Directory, diskGen, readable, req.EncodeTsNs)
				continue
			}
			location.UnloadEcVolume(needle.VolumeId(req.VolumeId))
			if err := removeStaleEcArtifacts(dataBase, idxBase, erasure_coding.MaxShardCount); err != nil {
				return nil, fmt.Errorf("fenced teardown of ec volume %d on %s: %w", req.VolumeId, location.Directory, err)
			}
		}
		return &volume_server_pb.VolumeEcShardsDeleteResponse{FullTeardownDone: true}, nil
	}

	glog.V(0).Infof("ec volume %s shard delete %v", bName, req.ShardIds)

	// Pass 1: delete the requested shard files (and any now-orphaned per-disk bitrot
	// sidecars) on every disk.
	for diskId, location := range vs.store.Locations {
		if err := deleteEcShardIdsForEachLocation(bName, location, req.ShardIds); err != nil {
			glog.Errorf("deleteEcShards from disk_id:%d %s %s.%v: %v", diskId, location.Directory, bName, req.ShardIds, err)
			return nil, err
		}
	}

	// Pass 2: the shared .ecx/.ecj index (and the .vif) is removed only when NO shard
	// of this volume remains on ANY disk of this node. A per-disk check would orphan a
	// sibling disk's shards (split-disk reconciled volumes) by deleting their index.
	nodeWideShards := 0
	type ecLocationStatus struct {
		location   *storage.DiskLocation
		hasEcxFile bool
		hasIdxFile bool
	}
	statuses := make([]ecLocationStatus, 0, len(vs.store.Locations))
	for _, location := range vs.store.Locations {
		hasEcxFile, hasIdxFile, existingShardCount, err := checkEcVolumeStatus(bName, location)
		if err != nil {
			return nil, err
		}
		nodeWideShards += existingShardCount
		statuses = append(statuses, ecLocationStatus{location, hasEcxFile, hasIdxFile})
	}
	if nodeWideShards == 0 {
		// Reuse the status from the count pass above so the directory listing is not
		// repeated per location.
		for _, st := range statuses {
			if err := removeEcSharedIndexFiles(bName, st.location, st.hasEcxFile, st.hasIdxFile); err != nil {
				return nil, err
			}
		}
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func deleteEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32) error {

	found := false

	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)

	// Delete the requested shard files unconditionally. Gating on a local .ecx
	// (still used for index-file routing below) would leak an orphan shard left
	// by a failed copy that reconciliation later mounts under a foreign index.
	for _, shardId := range shardIds {
		shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
		if util.FileExists(shardFileName) {
			found = true
			if err := removeFileIfExists(shardFileName); err != nil {
				return fmt.Errorf("remove ec shard %s: %w", shardFileName, err)
			}
		}
	}

	if !found {
		return nil
	}

	_, _, existingShardCount, err := checkEcVolumeStatus(bName, location)
	if err != nil {
		return err
	}

	if existingShardCount == 0 {
		// This disk's shards for the volume are gone. Remove the bitrot checksum
		// sidecar(s) (.ecsum and any .ecsum.v<N>) here, since they protect this
		// disk's shards and are now orphaned. The shared .ecx/.ecj/.vif index is
		// NOT removed here: a sibling disk may still hold shards that need it; the
		// caller removes the shared index only once no shard remains node-wide.
		if err := removeBitrotSidecars(dataBaseFilename); err != nil {
			return err
		}
		if location.IdxDirectory != location.Directory {
			if err := removeBitrotSidecars(indexBaseFilename); err != nil {
				return err
			}
		}
	}

	return nil
}

// removeEcSharedIndexFiles removes the shared .ecx/.ecj index (and the .vif when no
// .idx is present) for an EC volume on one disk. The caller invokes it only after
// the whole node's shards for the volume are gone, so a sibling disk's shards are
// never orphaned by deleting their index. A surviving stale .ecx is the orphan-index
// condition this prevents, so a real removal failure is surfaced. hasEcxFile and
// hasIdxFile come from the caller's checkEcVolumeStatus so the directory is not
// re-listed here.
func removeEcSharedIndexFiles(bName string, location *storage.DiskLocation, hasEcxFile, hasIdxFile bool) error {
	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)
	if hasEcxFile {
		// .ecx/.ecj may be in either dir depending on when -dir.idx was configured.
		for _, p := range []string{indexBaseFilename + ".ecx", indexBaseFilename + ".ecj"} {
			if err := removeFileIfExists(p); err != nil {
				return err
			}
		}
		if location.IdxDirectory != location.Directory {
			for _, p := range []string{dataBaseFilename + ".ecx", dataBaseFilename + ".ecj"} {
				if err := removeFileIfExists(p); err != nil {
					return err
				}
			}
		}
	}
	// Remove the .vif when no .idx is present (so this is not a live normal/tiered
	// volume), independent of .ecx presence: the caller only reaches here once no
	// shard remains node-wide, so an EC .vif left without its .ecx is stale
	// generation metadata that would otherwise leak.
	if !hasIdxFile {
		if err := removeFileIfExists(dataBaseFilename + ".vif"); err != nil {
			return err
		}
	}
	return nil
}

// removeFileIfExists removes path, treating "already gone" as success and
// returning only a real failure (so a stale shard left behind is not silently
// reported as cleaned).
func removeFileIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// readEcGenerationTsNs returns the EC encode generation recorded in a disk's .vif
// (data dir first, then idx dir for the split-disk layout) and whether a .vif file
// was present. A present .vif with no EcShardConfig — or one that failed to parse —
// yields (0, true): generation 0, which the fenced teardown preserves anyway (a
// recovered or pre-upgrade live volume). (0, false) means no .vif file was found.
// Both 0 and a missing .vif are preserved, so the read is fail-safe in every case.
func readEcGenerationTsNs(dataBaseFileName, indexBaseFileName string) (int64, bool) {
	for _, base := range []string{dataBaseFileName, indexBaseFileName} {
		if vi, _, found, _ := volume_info.MaybeLoadVolumeInfo(base + ".vif"); found {
			return vi.GetEcShardConfig().GetEncodeTsNs(), true
		}
		if dataBaseFileName == indexBaseFileName {
			break
		}
	}
	return 0, false
}

// removeStaleEcArtifacts deletes the shard, index, journal, and bitrot sidecar
// files of a prior encode so a fresh encode never mixes runs. total is the
// shard-id range to scan (pass the cap for custom ratios). Returns the first
// real removal failure; does not touch the source .dat/.idx/.vif.
func removeStaleEcArtifacts(dataBaseFileName, indexBaseFileName string, total int) error {
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for i := 0; i < total; i++ {
		record(removeFileIfExists(dataBaseFileName + erasure_coding.ToExt(i)))
	}
	// .ecx/.ecj/.ecsum may sit in either dir depending on -dir.idx; clear both.
	record(removeFileIfExists(indexBaseFileName + ".ecx"))
	record(removeFileIfExists(indexBaseFileName + ".ecj"))
	record(removeBitrotSidecars(indexBaseFileName))
	if dataBaseFileName != indexBaseFileName {
		record(removeFileIfExists(dataBaseFileName + ".ecx"))
		record(removeFileIfExists(dataBaseFileName + ".ecj"))
		record(removeBitrotSidecars(dataBaseFileName))
	}

	// Canonical <base>.vif. A shard copy installs shards + .ecx before .vif, so an
	// interrupted copy can leave a stale .vif whose run identity / shard ratio /
	// dat_file_size a fresh generation would inherit. Remove it only on a shard-only
	// EC node: where a normal <base>.idx exists this is the source volume holder and
	// the .vif belongs to that live volume — keep it. This mirrors the !hasIdxFile
	// gate in the per-shard delete path.
	if _, statErr := os.Stat(indexBaseFileName + ".idx"); os.IsNotExist(statErr) {
		record(removeFileIfExists(indexBaseFileName + ".vif"))
		if dataBaseFileName != indexBaseFileName {
			if _, dStatErr := os.Stat(dataBaseFileName + ".idx"); os.IsNotExist(dStatErr) {
				record(removeFileIfExists(dataBaseFileName + ".vif"))
			}
		}
	}
	return firstErr
}

// removeBitrotSidecars removes the legacy <base>.ecsum and any versioned
// <base>.ecsum.v<N> sidecars, returning the first real removal failure.
func removeBitrotSidecars(baseFilename string) error {
	var firstErr error
	if err := removeFileIfExists(baseFilename + erasure_coding.BitrotSidecarExt); err != nil {
		firstErr = err
	}
	matches, _ := filepath.Glob(baseFilename + erasure_coding.BitrotSidecarExt + ".v*")
	for _, m := range matches {
		if err := removeFileIfExists(m); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func checkEcVolumeStatus(bName string, location *storage.DiskLocation) (hasEcxFile bool, hasIdxFile bool, existingShardCount int, err error) {
	// check whether to delete the .ecx and .ecj file also
	fileInfos, err := os.ReadDir(location.Directory)
	if err != nil {
		return false, false, 0, err
	}
	if location.IdxDirectory != location.Directory {
		idxFileInfos, err := os.ReadDir(location.IdxDirectory)
		if err != nil {
			return false, false, 0, err
		}
		fileInfos = append(fileInfos, idxFileInfos...)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.Name() == bName+".ecx" || fileInfo.Name() == bName+".ecj" {
			hasEcxFile = true
			continue
		}
		if fileInfo.Name() == bName+".idx" {
			hasIdxFile = true
			continue
		}
		if isEcDataShardFile(fileInfo.Name(), bName) {
			existingShardCount++
		}
	}
	return hasEcxFile, hasIdxFile, existingShardCount, nil
}

func isEcDataShardFile(fileName, baseName string) bool {
	const ecDataShardSuffixLen = 2 // ".ecNN"
	prefix := baseName + ".ec"
	if !strings.HasPrefix(fileName, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(fileName, prefix)
	if len(suffix) != ecDataShardSuffixLen {
		return false
	}
	shardId, err := strconv.Atoi(suffix)
	if err != nil {
		return false
	}
	return shardId >= 0 && shardId < erasure_coding.MaxShardCount
}

func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsMount: %v", req)

	// Fetch a missing .ecx from a peer first so on-disk shards that never had a
	// local index can be mounted (issue #10104). Driven on demand by ec.rebuild.
	// volume_id 0 recovers every orphan on this server, including volumes the
	// master never learned about.
	if req.RecoverMissingIndex {
		vs.recoverMissingEcIndexes(req.VolumeId)
	}

	for _, shardId := range req.ShardIds {
		err := vs.store.MountEcShards(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId), req.SourceDiskType)

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsUnmount: %v", req)

	for _, shardId := range req.ShardIds {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId), req.EncodeTsNs)

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {

	// Resolve the shard together with the EcVolume on the disk that owns it,
	// rather than a first-match volume on a sibling disk: on a multi-disk server
	// those can belong to different encode generations, and the guard must
	// validate the identity of the volume whose bytes we serve.
	ecVolume, ecShard, found := vs.store.FindEcVolumeWithShard(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(req.ShardId))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.VolumeId, req.ShardId)
	}
	// Reject a shard whose identity doesn't match the caller's index; the caller
	// then recovers from parity. Lenient only when the caller has no identity
	// (pre-upgrade reader): a known caller must not accept an unstamped holder,
	// which would serve a stale pre-upgrade shard.
	if req.EncodeTsNs != 0 && req.EncodeTsNs != ecVolume.EncodeTsNs {
		return fmt.Errorf("ec shard %d.%d belongs to a different encode run", req.VolumeId, req.ShardId)
	}

	if req.FileKey != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.FileKey))
		if size.IsDeleted() {
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted:  true,
				EncodeTsNs: ecVolume.EncodeTsNs,
			})
		}
	}

	bufSize := req.Size
	if bufSize > BufferSizeLimit {
		bufSize = BufferSizeLimit
	}
	buffer := make([]byte, bufSize)

	startOffset, bytesToRead := req.Offset, req.Size

	for bytesToRead > 0 {
		// min of bytesToRead and bufSize
		bufferSize := bufSize
		if bufferSize > bytesToRead {
			bufferSize = bytesToRead
		}
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// println("read", ecShard.FileName(), "startOffset", startOffset, bytesread, "bytes, with target", bufferSize)
		if bytesread > 0 {

			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data:       buffer[:bytesread],
				EncodeTsNs: ecVolume.EncodeTsNs,
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}

			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)

		}

		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

	}

	return nil

}

func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcBlobDelete: %v", req)

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {

			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.FileKey), needle.Version(req.Version))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %w", err)
			}
			if size.IsDeleted() {
				return resp, nil
			}

			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.FileKey))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}

// VolumeEcShardsToVolume generates the .idx, .dat files from .ecx, .ecj and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsToVolume(ctx context.Context, req *volume_server_pb.VolumeEcShardsToVolumeRequest) (*volume_server_pb.VolumeEcShardsToVolumeResponse, error) {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return nil, err
	}

	glog.V(0).Infof("VolumeEcShardsToVolume: %v", req)

	// Collect all EC shards (NewEcVolume will load EC config from .vif into v.ECContext)
	// Use MaxShardCount (32) to support custom EC ratios up to 32 total shards
	tempShards := make([]string, erasure_coding.MaxShardCount)
	v, found := vs.store.CollectEcShards(needle.VolumeId(req.VolumeId), tempShards)
	if !found {
		return nil, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// Use EC context (already loaded from .vif) to determine data shard count
	dataShards := v.ECContext.DataShards

	// Defensive validation to prevent panics from corrupted ECContext
	if dataShards <= 0 || dataShards > erasure_coding.MaxShardCount {
		return nil, fmt.Errorf("invalid data shard count %d for volume %d (must be 1..%d)", dataShards, req.VolumeId, erasure_coding.MaxShardCount)
	}

	shardFileNames := tempShards[:dataShards]
	glog.V(1).Infof("Using EC config from volume %d: %d data shards", req.VolumeId, dataShards)

	// Verify all data shards are present
	for shardId := 0; shardId < dataShards; shardId++ {
		if shardFileNames[shardId] == "" {
			return nil, fmt.Errorf("ec volume %d missing shard %d", req.VolumeId, shardId)
		}
	}

	dataBaseFileName, indexBaseFileName := v.DataBaseFileName(), v.IndexBaseFileName()
	if !util.FileExists(indexBaseFileName + ".ecx") {
		indexBaseFileName = dataBaseFileName
	}

	// Merge .ecj deletions into .ecx so that HasLiveNeedles and FindDatFileSize
	// see the full set of deleted needles. Without this, needles deleted after the
	// last ecx rebuild would still appear live, causing the decoded .dat to include
	// data that should be skipped and HasLiveNeedles to return a false positive.
	if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("RebuildEcxFile %s: %v", indexBaseFileName, err)
	}

	// If the EC index contains no live entries, decoding should be a no-op:
	// just allow the caller to purge EC shards and do not generate an empty normal volume.
	hasLive, err := erasure_coding.HasLiveNeedles(indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("HasLiveNeedles %s: %w", indexBaseFileName, err)
	}
	if !hasLive {
		return nil, status.Errorf(codes.FailedPrecondition, "ec volume %d %s", req.VolumeId, erasure_coding.EcNoLiveEntriesSubstring)
	}

	// calculate .dat file size
	datFileSize, err := erasure_coding.FindDatFileSize(dataBaseFileName, indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("FindDatFileSize %s: %v", dataBaseFileName, err)
	}

	// write .dat file from .ec00 ~ .ec09 files
	if err := erasure_coding.WriteDatFile(dataBaseFileName, datFileSize, shardFileNames); err != nil {
		return nil, fmt.Errorf("WriteDatFile %s: %v", dataBaseFileName, err)
	}

	// write .idx file from .ecx and .ecj files
	if err := erasure_coding.WriteIdxFileFromEcIndex(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("WriteIdxFileFromEcIndex %s: %v", v.IndexBaseFileName(), err)
	}

	// The EC generation is gone; drop its bitrot sidecar(s) so a later EC
	// re-encode cannot mistake a stale .ecsum for protection.
	removeBitrotSidecars(dataBaseFileName)
	if indexBaseFileName != dataBaseFileName {
		removeBitrotSidecars(indexBaseFileName)
	}

	var volumeLocation *storage.DiskLocation
	for _, location := range vs.store.Locations {
		if candidate, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found && candidate == v {
			volumeLocation = location
			break
		}
	}
	if volumeLocation == nil {
		return nil, fmt.Errorf("ec volume %d location not found for offline compaction", req.VolumeId)
	}

	if err := vs.store.CompactVolumeFiles(
		needle.VolumeId(req.VolumeId),
		v.Collection,
		volumeLocation,
		vs.needleMapKind,
		vs.ldbTimout,
		0,
		vs.compactionBytePerSecond,
	); err != nil {
		glog.Errorf("CompactVolumeFiles %s: %v", dataBaseFileName, err)
	}

	return &volume_server_pb.VolumeEcShardsToVolumeResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsInfo(ctx context.Context, req *volume_server_pb.VolumeEcShardsInfoRequest) (*volume_server_pb.VolumeEcShardsInfoResponse, error) {
	glog.V(0).Infof("VolumeEcShardsInfo: volume %d", req.VolumeId)

	vid := needle.VolumeId(req.GetVolumeId())

	// Multi-disk volume servers register one EcVolume per DiskLocation
	// that holds shards for the same vid: shards may be spread across
	// disks while the .ecx lives on whichever disk owned the original
	// .dat. Walk every DiskLocation here so the response reflects the
	// full local shard set; the per-disk ecVolumesLock is taken inside
	// DiskLocation.FindEcVolume.
	var primary *erasure_coding.EcVolume
	var seenShards erasure_coding.ShardBits
	shardInfos := make([]*volume_server_pb.EcShardInfo, 0, erasure_coding.MaxShardCount)
	for _, location := range vs.store.Locations {
		ecv, ok := location.FindEcVolume(vid)
		if !ok {
			continue
		}
		if primary == nil {
			primary = ecv
		}
		for _, s := range ecv.Shards {
			if seenShards.Has(s.ShardId) {
				continue
			}
			seenShards = seenShards.Set(s.ShardId)
			shardInfos = append(shardInfos, s.ToEcShardInfo())
		}
	}
	if primary == nil {
		return nil, fmt.Errorf("VolumeEcShardsInfo: EC volume %d not found", vid)
	}

	var files, filesDeleted, totalSize uint64
	err := primary.WalkIndex(func(_ types.NeedleId, _ types.Offset, size types.Size) error {
		// deleted files are counted when computing EC volume sizes. this aligns with VolumeStatus(),
		// which reports the raw data backend file size, regardless of deleted files.
		totalSize += uint64(size.Raw())

		if size.IsDeleted() {
			filesDeleted++
		} else {
			files++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	res := &volume_server_pb.VolumeEcShardsInfoResponse{
		EcShardInfos:     shardInfos,
		FileCount:        files,
		FileDeletedCount: filesDeleted,
		VolumeSize:       totalSize,
	}

	return res, nil
}
