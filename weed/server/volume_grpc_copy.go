package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const BufferSizeLimit = 1024 * 1024 * 2

// volumeCopyGuard serializes a copy's registration recheck and file
// clearing against mounts and configures of the same volume.
type volumeCopyGuard struct {
	sync.Mutex
	copying bool
}

func (vs *VolumeServer) volumeCopyGuard(volumeId uint32) *volumeCopyGuard {
	guard, _ := vs.volumeCopyGuards.LoadOrStore(volumeId, &volumeCopyGuard{})
	return guard.(*volumeCopyGuard)
}

// volumeLoadGuard is the storage-layer hook for volume loads: a vid with a
// copy in flight is skipped, otherwise the returned release keeps the load's
// registration ordered against the copy's own recheck. A copy holding the
// guard means the same skip.
func (vs *VolumeServer) volumeLoadGuard(vid needle.VolumeId) (func(), bool) {
	guard := vs.volumeCopyGuard(uint32(vid))
	if !guard.TryLock() {
		return func() {}, true
	}
	if guard.copying {
		guard.Unlock()
		return func() {}, true
	}
	return guard.Unlock, false
}

// VolumeCopy copy the .idx .dat .vif files, and mount the volume
func (vs *VolumeServer) VolumeCopy(req *volume_server_pb.VolumeCopyRequest, stream volume_server_pb.VolumeServer_VolumeCopyServer) error {
	if err := vs.checkGrpcAdminAuth(stream.Context()); err != nil {
		return err
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}
	if !vs.AllowUntrustedRemoteEndpoints {
		if err := validateReplicaTarget(stream.Context(), req.SourceDataNode); err != nil {
			return fmt.Errorf("invalid source data node %s: %w", req.SourceDataNode, err)
		}
	}

	// One copy of a volume at a time: the staged file names are fixed per
	// volume, so a racing second copy would overwrite the files this copy
	// still owns. Retryable once the in-flight copy settles.
	guard := vs.volumeCopyGuard(req.VolumeId)
	guard.Lock()
	if guard.copying {
		guard.Unlock()
		return status.Errorf(codes.Unavailable, "volume %d is already being copied", req.VolumeId)
	}
	guard.copying = true
	guard.Unlock()
	defer func() {
		guard.Lock()
		guard.copying = false
		guard.Unlock()
	}()

	// A pre-existing local replica is NOT deleted up front. Deleting before the
	// source is confirmed reachable destroys a healthy copy on a transient
	// source outage (and, on retry, can lose the volume entirely). The copy is
	// staged next to it and validated first; the delete only runs once the
	// replacement is known good (validation details below).

	// the master will not start compaction for read-only volumes, so it is safe to just copy files directly
	// copy .dat and .idx files
	//   read .idx .dat file size and timestamp
	//   send .idx file
	//   send .dat file
	//   confirm size and timestamp
	var volFileInfoResp *volume_server_pb.ReadVolumeFileStatusResponse
	var sourceVolumeStatus *volume_server_pb.VolumeStatusResponse
	var sourceVolumeStatusAfterCopy *volume_server_pb.VolumeStatusResponse
	var dataBaseFileName, indexBaseFileName, idxFileName, datFileName, vifFileName string
	var stagedDataBaseFileName, stagedIndexBaseFileName string
	var stagedIdxFileName, stagedDatFileName, stagedVifFileName string
	var movedFiles []string
	var replacedLinks []string
	var replacedRemoved, mounted bool
	var hasRemoteDatFile bool
	var copyLocation *storage.DiskLocation
	var copyReservedSpace uint64
	err := operation.WithVolumeServerClientOptions(true, pb.ServerAddress(req.SourceDataNode), func(client volume_server_pb.VolumeServerClient) (err error) {
		sourceVolumeStatus, err = client.VolumeStatus(stream.Context(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: req.VolumeId,
		})
		if err != nil {
			glog.Warningf("failed to read source volume %d status before copy; skip record count validation: %v", req.VolumeId, err)
			sourceVolumeStatus = nil
		}

		volFileInfoResp, err = client.ReadVolumeFileStatus(stream.Context(),
			&volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: req.VolumeId,
			})
		if nil != err {
			return fmt.Errorf("read volume file status failed, %w", err)
		}

		diskType := volFileInfoResp.DiskType
		if req.DiskType != "" {
			diskType = req.DiskType
		}
		hasRemoteDatFile = volFileInfoResp.VolumeInfo != nil && len(volFileInfoResp.VolumeInfo.Files) > 0

		var preallocateSize int64
		if grpcErr := pb.WithMasterClient(context.Background(), false, vs.GetMaster(context.Background()), vs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get master %s configuration: %v", vs.GetMaster(context.Background()), err)
			}
			if resp.VolumePreallocate {
				preallocateSize = int64(resp.VolumeSizeLimitMB) * (1 << 20)
			}
			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", vs.GetMaster(context.Background()), grpcErr)
		}

		// a remote-backed volume only lands its .idx/.vif locally; the .dat stays in the tier
		neededSpace := volFileInfoResp.DatFileSize
		if hasRemoteDatFile {
			neededSpace = volFileInfoResp.IdxFileSize
		} else if backend.PreallocateSupported && uint64(preallocateSize) > neededSpace {
			// the .dat reserves the master's volume size limit up front, so the
			// location must fit the reservation, not just the copied bytes
			neededSpace = uint64(preallocateSize)
		}
		location := vs.store.ReserveFreeLocation(neededSpace, func(location *storage.DiskLocation) bool {
			return location.DiskType == types.ToDiskType(diskType) &&
				location.AvailableSpace.Load() > neededSpace
		}, needle.VolumeId(req.VolumeId))
		if location == nil {
			return fmt.Errorf("%s %s", util.ErrVolumeNoSpaceLeft, types.ToDiskType(diskType).ReadableString())
		}
		copyLocation = location
		copyReservedSpace = neededSpace

		// The headroom charge covers bytes promised but not yet visible to
		// statfs; as the copy's bytes land they stop needing the charge, so a
		// slow copy does not count its own .dat against the free space a
		// second reservation needs.
		commitCopy := func(n uint64) {
			if n > copyReservedSpace {
				n = copyReservedSpace
			}
			if n > 0 {
				vs.store.CommitFreeLocation(location, n)
				copyReservedSpace -= n
			}
		}
		newCopyProgress := func(skip uint64) func(int64) {
			counted := int64(skip)
			return func(processed int64) {
				if processed > counted {
					commitCopy(uint64(processed - counted))
					counted = processed
				}
			}
		}

		dataBaseFileName = storage.VolumeFileName(location.Directory, volFileInfoResp.Collection, int(req.VolumeId))
		indexBaseFileName = storage.VolumeFileName(location.IdxDirectory, volFileInfoResp.Collection, int(req.VolumeId))

		// The copy writes into a copying/ staging dir the volume loader never
		// scans: every staged byte is this copy's own, a live replica at the
		// final basename is never touched by the copy, and a failed copy can
		// always clean up after itself.
		stagedDataBaseFileName = filepath.Join(location.Directory, "copying", filepath.Base(dataBaseFileName))
		stagedIndexBaseFileName = filepath.Join(location.IdxDirectory, "copying", filepath.Base(indexBaseFileName))
		if mkErr := os.MkdirAll(filepath.Dir(stagedDataBaseFileName), 0755); mkErr != nil {
			err = mkErr
			return fmt.Errorf("create copying dir for volume %d: %w", req.VolumeId, mkErr)
		}
		if mkErr := os.MkdirAll(filepath.Dir(stagedIndexBaseFileName), 0755); mkErr != nil {
			err = mkErr
			return fmt.Errorf("create copying dir for volume %d: %w", req.VolumeId, mkErr)
		}
		stagedIdxFileName = stagedIndexBaseFileName + ".idx"
		stagedDatFileName = stagedDataBaseFileName + ".dat"
		stagedVifFileName = stagedDataBaseFileName + ".vif"

		defer func() {
			if err != nil {
				os.Remove(stagedDatFileName)
				os.Remove(stagedIdxFileName)
				os.Remove(stagedVifFileName)
				os.Remove(stagedVifFileName + ".copying")
				if dataBaseFileName != "" {
					// A .note only marks an in-flight copy; a partial write
					// must not be left to fail the next volume load.
					os.Remove(dataBaseFileName + ".note")
				}
				// Zero before the outer defer sees the same error, or the
				// charge would be released twice.
				vs.store.ReleaseFreeLocation(location, copyReservedSpace)
				copyReservedSpace = 0
			}
		}()

		var copyDatAllocated uint64
		if preallocateSize > 0 && !hasRemoteDatFile {
			volumeFile := stagedDataBaseFileName + ".dat"
			_, err := backend.CreateVolumeFile(volumeFile, preallocateSize, 0)
			if err != nil {
				return fmt.Errorf("create volume file %s: %v", volumeFile, err)
			}
			// fallocate already holds those bytes on disk; when the
			// filesystem did not reserve them the write progress below
			// settles the charge as the bytes land instead.
			if fi, statErr := os.Stat(volumeFile); statErr == nil {
				copyDatAllocated = storage.AllocatedSize(fi)
				commitCopy(copyDatAllocated)
			}
		}

		// println("source:", volFileInfoResp.String())
		copyResponse := &volume_server_pb.VolumeCopyResponse{}
		reportInterval := int64(1024 * 1024 * 128)
		nextReportTarget := reportInterval
		var modifiedTsNs int64
		var sendErr error
		var ioBytePerSecond int64
		if req.IoBytePerSecond <= 0 {
			ioBytePerSecond = vs.maintenanceBytePerSecond
		} else {
			ioBytePerSecond = req.IoBytePerSecond
		}
		throttler := util.NewWriteThrottler(ioBytePerSecond)

		if !hasRemoteDatFile {
			datProgress := newCopyProgress(copyDatAllocated)
			// Write into the .dat CreateVolumeFile just made so the
			// fallocated blocks carry over to the final name at rename.
			if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.DatFileSize, stagedDataBaseFileName, ".dat", preallocateSize > 0, true, func(processed int64) bool {
				datProgress(processed)
				if processed > nextReportTarget {
					copyResponse.ProcessedBytes = processed
					if sendErr = stream.Send(copyResponse); sendErr != nil {
						return false
					}
					nextReportTarget = processed + reportInterval
				}
				return true
			}, throttler); err != nil {
				return err
			}
			if sendErr != nil {
				return sendErr
			}
			if modifiedTsNs > 0 {
				os.Chtimes(stagedDataBaseFileName+".dat", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
			}
		}

		idxProgress := newCopyProgress(0)
		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, volFileInfoResp.IdxFileSize, stagedIndexBaseFileName, ".idx", false, false, func(processed int64) bool {
			idxProgress(processed)
			return true
		}, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(stagedIndexBaseFileName+".idx", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		vifProgress := newCopyProgress(0)
		if modifiedTsNs, err = vs.doCopyFileWithThrottler(client, false, req.Collection, req.VolumeId, volFileInfoResp.CompactionRevision, 1024*1024, stagedDataBaseFileName, ".vif", false, true, func(processed int64) bool {
			vifProgress(processed)
			return true
		}, throttler); err != nil {
			return err
		}
		if modifiedTsNs > 0 {
			os.Chtimes(stagedDataBaseFileName+".vif", time.Unix(0, modifiedTsNs), time.Unix(0, modifiedTsNs))
		}

		var statusErr error
		sourceVolumeStatusAfterCopy, statusErr = client.VolumeStatus(stream.Context(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: req.VolumeId,
		})
		if statusErr != nil {
			err = fmt.Errorf("read source volume %d status after copy failed: %w", req.VolumeId, statusErr)
			return err
		}

		return nil
	}, vs.grpcDialOption, vs.guardedGrpcDialOption(req.SourceDataNode))

	if err != nil {
		return err
	}
	if dataBaseFileName == "" {
		return fmt.Errorf("not found volume %d file", req.VolumeId)
	}

	idxFileName = indexBaseFileName + ".idx"
	datFileName = dataBaseFileName + ".dat"
	vifFileName = dataBaseFileName + ".vif"

	defer func() {
		if copyLocation == nil {
			return
		}
		if err != nil {
			keepFiles := false
			if mounted {
				// A mounted copy was already announced to the master; delete
				// through the store so the announcement is withdrawn before
				// the files go away. keepRemoteData: the .vif may point at
				// the same cloud-tier object the source still references.
				if delErr := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), false, false, true); delErr != nil {
					glog.Warningf("discard mounted copy of volume %d: %v", req.VolumeId, delErr)
					// A refused delete leaves it registered; unlinking its
					// files now would strand a live replica.
					keepFiles = vs.store.GetVolume(needle.VolumeId(req.VolumeId)) != nil
				}
			}
			os.Remove(stagedIdxFileName)
			os.Remove(stagedDatFileName)
			os.Remove(stagedVifFileName)
			os.Remove(stagedVifFileName + ".copying")
			if !keepFiles {
				// Only files this copy moved into place come off; before the
				// renames the final basenames can still be the replica being
				// replaced, and a rejected copy must leave it intact.
				for _, moved := range movedFiles {
					// A regular volume and an EC volume for the same id share
					// <base>.vif; keep it while the EC index is here.
					if moved == vifFileName && copyLocation.HasEcxFileOnDisk(volFileInfoResp.Collection, needle.VolumeId(req.VolumeId)) {
						continue
					}
					os.Remove(moved)
				}
				// A failed mount leaves the needle-map backends it built;
				// drop them so the next load does not trust a stale index.
				// Only after the renames could those be this copy's — before
				// them the basenames belong to a replica still on disk, and
				// an unmounted one reports no store volume. A replica still
				// standing keeps its files either way.
				if len(movedFiles) > 0 && vs.store.GetVolume(needle.VolumeId(req.VolumeId)) == nil {
					if cleanErr := removeStaleIndexFiles(indexBaseFileName, dataBaseFileName); cleanErr != nil {
						glog.Warningf("clear stale index files of failed copy of volume %d: %v", req.VolumeId, cleanErr)
					}
				}
				if dataBaseFileName != "" {
					os.Remove(dataBaseFileName + ".note")
				}
				if len(replacedLinks) > 0 {
					// The swap failed after the previous replica's files were
					// displaced; put the preserved files back. A surviving
					// partial — like an incoming .vif kept for a coexisting
					// EC volume — is dropped so the original comes back.
					restoreReplacedVolumeFiles(replacedLinks)
					replacedLinks = nil
					// Only a replica the store deleted gets remounted; an
					// unmounted one goes back to files on disk, as found.
					if replacedRemoved {
						if remountErr := vs.store.MountVolumeClaimed(needle.VolumeId(req.VolumeId), &req.Collection); remountErr != nil {
							glog.Warningf("remount replaced replica of volume %d: %v", req.VolumeId, remountErr)
						}
					}
				}
			}
			for _, link := range replacedLinks {
				os.Remove(link)
			}
			vs.store.ReleaseFreeLocation(copyLocation, copyReservedSpace)
		} else {
			for _, link := range replacedLinks {
				os.Remove(link)
			}
			vs.store.CommitFreeLocation(copyLocation, copyReservedSpace)
		}
		// copying/ is shared by copies of other volumes on the disk, so it
		// stays behind; a leftover staged file is rewritten or removed by the
		// next copy of its volume.
	}()

	if err = checkCopyFiles(volFileInfoResp, hasRemoteDatFile, stagedIdxFileName, stagedDatFileName); err != nil { // added by panyc16
		return err
	}

	var lastAppendAtNs = volFileInfoResp.DatFileTimestampSeconds * uint64(time.Second)
	if !hasRemoteDatFile {
		if appendAtNs, appendErr := findLastAppendAtNsFromCopiedFiles(stagedIdxFileName, stagedDatFileName, needle.Version(volFileInfoResp.Version)); appendErr == nil && appendAtNs > 0 {
			lastAppendAtNs = appendAtNs
		} else if appendErr != nil {
			glog.V(1).Infof("failed to find last append timestamp for volume %d: %v", req.VolumeId, appendErr)
		}
	}

	shouldValidateCopyCounts := copyCountsStable(sourceVolumeStatus, sourceVolumeStatusAfterCopy)
	if !shouldValidateCopyCounts {
		glog.V(1).Infof("source volume %d changed during copy; skip record count validation", req.VolumeId)
	}

	// The staged copy is checked before the existing replica is dropped, so
	// a rejected copy leaves the replica it was meant to replace in place.
	if shouldValidateCopyCounts {
		stagedIdx, openErr := os.Open(stagedIdxFileName)
		if openErr != nil {
			err = fmt.Errorf("open copied index of volume %d for validation: %w", req.VolumeId, openErr)
			return err
		}
		copiedNeedleMap, mapErr := storage.LoadCompactNeedleMap(stagedIdx, needle.Version(volFileInfoResp.Version))
		stagedIdx.Close()
		if mapErr != nil {
			err = fmt.Errorf("load copied index of volume %d for validation: %w", req.VolumeId, mapErr)
			return err
		}
		if countErr := checkCopyCounts(req.VolumeId, sourceVolumeStatusAfterCopy, uint64(copiedNeedleMap.FileCount()), uint64(copiedNeedleMap.DeletedCount())); countErr != nil {
			err = countErr
			return countErr
		}
	}

	// A caller that left during the copy cannot use the result; bail before
	// the swap deletes the replica it would replace.
	if ctxErr := stream.Context().Err(); ctxErr != nil {
		err = ctxErr
		return ctxErr
	}

	// A replica that mounted mid-copy must leave before the swap too, so check
	// the store again rather than the stale pre-copy snapshot.
	if replaced := vs.store.GetVolume(needle.VolumeId(req.VolumeId)); replaced != nil {
		glog.V(0).Infof("volume %d already exists. deleting before copying from %s...", req.VolumeId, req.SourceDataNode)
		// Hard-link the outgoing replica's files into copying/ first: the
		// inodes survive the delete, so a failed swap can put the previous
		// replica back instead of leaving the node with none. The paths come
		// from the mounted volume — it can sit on another disk, with its .idx
		// beside the .dat. A partial backup cannot restore the replica.
		var backupComplete bool
		replacedLinks, backupComplete = linkReplacedVolumeFiles(
			replaced.DataFileName()+".dat",
			replaced.IndexFileName()+".idx",
			replaced.DataFileName()+".vif",
			replaced.DataFileName()+".idx",
		)
		if !backupComplete {
			err = fmt.Errorf("preserve files of existing volume %d for rollback", req.VolumeId)
			return err
		}
		// keep remote data: the inbound copy carries a .vif that may point at
		// the same cloud-tier object the existing volume references.
		if delErr := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId), false, false, true); delErr != nil {
			err = fmt.Errorf("failed to delete existing volume %d: %v", req.VolumeId, delErr)
			return err
		}
		replacedRemoved = true
		glog.V(0).Infof("deleted existing volume %d before copying.", req.VolumeId)
	} else {
		// An unmounted replica at the final basenames gets no store delete:
		// the stale-index cleanup and the renames displace it silently, and a
		// copy without a staged .dat would leave the old one orphaned. Link
		// its files the same way so a failed swap restores them (no remount —
		// it was not mounted), then take them off; refuse the copy when they
		// cannot be preserved.
		var backupComplete bool
		replacedLinks, backupComplete = linkReplacedVolumeFiles(
			datFileName,
			idxFileName,
			vifFileName,
			dataBaseFileName+".idx",
			dataBaseFileName+".note",
		)
		if !backupComplete {
			err = fmt.Errorf("preserve files of existing volume %d for rollback", req.VolumeId)
			return err
		}
		for _, replacedFile := range []string{datFileName, idxFileName, dataBaseFileName + ".idx"} {
			os.Remove(replacedFile)
		}
		// The .vif may be shared with an EC volume for the same id: while a
		// sealed .ecx is here it must stay — an incoming .vif overwrites it
		// on rename, but with no incoming one the EC volume would lose the
		// settings it only stores there.
		if !copyLocation.HasEcxFileOnDisk(volFileInfoResp.Collection, needle.VolumeId(req.VolumeId)) {
			os.Remove(vifFileName)
		}
	}

	// A stale .note at the final basename fails the next load; the staged
	// copy carries none, so clear leftovers before the files land. A stale
	// derived index would likewise be trusted over the fresh .idx, unless a
	// live replica landed mid-copy and still owns it.
	os.Remove(dataBaseFileName + ".note")
	if vs.store.GetVolume(needle.VolumeId(req.VolumeId)) == nil {
		if cleanErr := removeStaleIndexFiles(indexBaseFileName, dataBaseFileName); cleanErr != nil {
			err = fmt.Errorf("clear stale index files before copying volume %d: %w", req.VolumeId, cleanErr)
			return err
		}
	}

	// The .note marks the copy as in-progress through the renames: a crash
	// mid-rename leaves the note and the load scan sweeps the partial set.
	// The .vif lands first so the scan always has a basename to find.
	if noteErr := util.WriteFile(dataBaseFileName+".note", []byte(fmt.Sprintf("copying from %s", req.SourceDataNode)), 0755); noteErr != nil {
		err = noteErr
		return fmt.Errorf("write .note for volume %d: %w", req.VolumeId, noteErr)
	}

	for _, pair := range [][2]string{
		{stagedVifFileName, vifFileName},
		{stagedDatFileName, datFileName},
		{stagedIdxFileName, idxFileName},
	} {
		if renameErr := os.Rename(pair[0], pair[1]); renameErr != nil {
			if os.IsNotExist(renameErr) {
				continue
			}
			err = fmt.Errorf("move copied %s into place for volume %d: %w", pair[0], req.VolumeId, renameErr)
			return err
		}
		movedFiles = append(movedFiles, pair[1])
	}

	// With a separate index directory the .idx lands outside the data dir, so
	// a copy whose source has no .vif leaves nothing the volume scan can find.
	// An empty .vif marks the basename for discovery; the loader treats it as
	// absent and writes a real one on mount.
	if filepath.Dir(indexBaseFileName) != filepath.Dir(dataBaseFileName) && !util.FileExists(vifFileName) {
		if writeErr := util.WriteFile(vifFileName, nil, 0644); writeErr != nil {
			err = fmt.Errorf("write .vif marker for volume %d: %w", req.VolumeId, writeErr)
			return err
		}
		movedFiles = append(movedFiles, vifFileName)
	}

	// A leftover .note fails the load on the next restart, so a removal
	// failure must fail the copy rather than be silently swallowed.
	if noteErr := os.Remove(dataBaseFileName + ".note"); noteErr != nil && !os.IsNotExist(noteErr) {
		err = noteErr
		return fmt.Errorf("remove .note for volume %d: %w", req.VolumeId, noteErr)
	}

	// Load and validate the volume before announcing it to the master. A failed
	// validation is unloaded by the store without ever making the replica
	// routable.
	err = vs.store.MountVolumeClaimed(needle.VolumeId(req.VolumeId), &req.Collection, func(targetVolume *storage.Volume) error {
		if !shouldValidateCopyCounts {
			return nil
		}
		return checkCopyCounts(req.VolumeId, sourceVolumeStatusAfterCopy, targetVolume.FileCount(), targetVolume.DeletedCount())
	})
	if err != nil {
		return fmt.Errorf("failed to mount or validate volume %d: %w", req.VolumeId, err)
	}
	mounted = true

	if err = stream.Send(&volume_server_pb.VolumeCopyResponse{
		LastAppendAtNs: lastAppendAtNs,
	}); err != nil {
		glog.Errorf("send response: %v", err)
	}

	return err
}

// linkReplacedVolumeFiles hard-links each file into a copying/ sibling so
// the inodes survive the replacement's delete and a failed swap can put the
// previous replica's files back. The loader never scans copying/. A repeated
// path links once: data and index can share a directory and so a basename.
// It reports false when a file that exists could not be preserved — deleting
// the replica then would leave nothing to roll back to.
func linkReplacedVolumeFiles(baseFileNames ...string) (links []string, complete bool) {
	complete = true
	seen := make(map[string]bool)
	for _, base := range baseFileNames {
		if seen[base] {
			continue
		}
		seen[base] = true
		if _, err := os.Stat(base); err != nil {
			if !os.IsNotExist(err) {
				glog.Warningf("preserve replaced file %s: %v", base, err)
				complete = false
			}
			continue
		}
		link := filepath.Join(filepath.Dir(base), "copying", "replaced_"+filepath.Base(base))
		if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil {
			glog.Warningf("preserve replaced file %s: %v", base, err)
			complete = false
			continue
		}
		os.Remove(link)
		if err := os.Link(base, link); err != nil {
			glog.Warningf("preserve replaced file %s: %v", base, err)
			complete = false
			continue
		}
		links = append(links, link)
	}
	return links, complete
}

// restoreReplacedVolumeFiles puts linkReplacedVolumeFiles' links back at
// their base names after a failed swap. A leftover at the basename is a
// surviving partial of the copy — it comes off first so the restored set
// stays one generation. The link is kept whenever it cannot go back: an
// existing path does not mean the copy's write succeeded.
func restoreReplacedVolumeFiles(links []string) {
	for _, link := range links {
		base := replacedBaseOf(link)
		if _, err := os.Stat(base); err == nil {
			if rmErr := os.Remove(base); rmErr != nil {
				glog.Warningf("restore replaced file %s: cannot remove leftover %s: %v", link, base, rmErr)
				continue
			}
		}
		if err := os.Rename(link, base); err != nil {
			glog.Warningf("restore replaced file %s: %v", link, err)
		}
	}
}

func replacedBaseOf(link string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(link)), strings.TrimPrefix(filepath.Base(link), "replaced_"))
}

// removeStaleIndexFiles drops the needle-map files a mount would trust over
// a fresh copy. A load prefers an .idx beside the .dat over one in the idx
// dir, so a leftover .idx is cleared along with the backends a mount builds
// (.ldb/.sdx/.rdb); both bases are covered. The first removal error is
// returned: a surviving index would be trusted over the copied .idx, so a
// caller that can still abort must know the removal failed.
func removeStaleIndexFiles(indexBaseFileName, dataBaseFileName string) error {
	var firstErr error
	for _, base := range []string{indexBaseFileName, dataBaseFileName} {
		for _, name := range []string{base + ".ldb", base + ".sdx", base + ".rdb", base + ".idx"} {
			if rmErr := os.RemoveAll(name); rmErr != nil && firstErr == nil {
				firstErr = rmErr
			}
		}
	}
	return firstErr
}

func (vs *VolumeServer) doCopyFile(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	return vs.doCopyFileWithThrottler(client, isEcVolume, collection, vid, compactRevision, stopOffset, baseFileName, ext, isAppend, ignoreSourceFileNotFound, progressFn, util.NewWriteThrottler(vs.maintenanceBytePerSecond))
}

func (vs *VolumeServer) doCopyFileWithThrottler(client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid, compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc, throttler *util.WriteThrottler) (modifiedTsNs int64, err error) {

	copyFileClient, err := client.CopyFile(context.Background(), &volume_server_pb.CopyFileRequest{
		VolumeId:                 vid,
		Ext:                      ext,
		CompactionRevision:       compactRevision,
		StopOffset:               stopOffset,
		Collection:               collection,
		IsEcVolume:               isEcVolume,
		IgnoreSourceFileNotFound: ignoreSourceFileNotFound,
	})
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to start copying volume %d %s file: %v", vid, ext, err)
	}

	modifiedTsNs, err = writeToFile(copyFileClient, baseFileName+ext, throttler, isAppend, ignoreSourceFileNotFound, progressFn)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("failed to copy %s file: %v", baseFileName+ext, err)
	}

	return modifiedTsNs, nil

}

// checkCopyFiles verifies the copied file sizes. Record counts are compared
// after the target volume is mounted, when the target needle map is available.
func checkCopyFiles(originFileInf *volume_server_pb.ReadVolumeFileStatusResponse, hasRemoteDatFile bool, idxFileName, datFileName string) error {
	stat, err := os.Stat(idxFileName)
	if err != nil {
		// If the idx file doesn't exist but the expected size is 0, that's OK (empty volume)
		if os.IsNotExist(err) && originFileInf.IdxFileSize == 0 {
			// empty volume, idx file not needed
		} else {
			return fmt.Errorf("stat idx file %s failed: %v", idxFileName, err)
		}
	} else if originFileInf.IdxFileSize != uint64(stat.Size()) {
		return fmt.Errorf("idx file %s size [%v] is not same as origin file size [%v]",
			idxFileName, stat.Size(), originFileInf.IdxFileSize)
	}

	if hasRemoteDatFile {
		return nil
	}

	stat, err = os.Stat(datFileName)
	if err != nil {
		return fmt.Errorf("get dat file info failed, %w", err)
	}
	if originFileInf.DatFileSize != uint64(stat.Size()) {
		return fmt.Errorf("the dat file size [%v] is not same as origin file size [%v]",
			stat.Size(), originFileInf.DatFileSize)
	}
	return nil
}

func checkCopyCounts(vid uint32, origin *volume_server_pb.VolumeStatusResponse, targetFileCount, targetDeletedCount uint64) error {
	// The file count is exact on both sides, so a mismatch on a stable source
	// means the copy is genuinely bad and rejects. The deleted count is a
	// rebuild-time estimate (bloom-filter dedup on some needle-map backends,
	// rows appended between the status snapshots), so it legitimately drifts
	// and only warns. The .dat/.idx size checks already gate integrity.
	if origin.FileCount != targetFileCount {
		return fmt.Errorf("target file count [%d] is not same as origin file count [%d]", targetFileCount, origin.FileCount)
	}
	if origin.FileDeletedCount != targetDeletedCount {
		glog.Warningf("copied volume %d deleted count [%d] differs from origin deleted count [%d] (counter drift, copy kept)", vid, targetDeletedCount, origin.FileDeletedCount)
	}
	return nil
}

func copyCountsStable(before, after *volume_server_pb.VolumeStatusResponse) bool {
	// A writable source may receive writes while its files are copied; then
	// the before/after file counts do not describe one stable snapshot and
	// strict target-count validation would report a false error. Deletes do
	// not move the file count — they append tombstone rows the deleted
	// counter records — so drift there leaves the strict check exact, and
	// checkCopyCounts warns on it instead.
	return before != nil && after != nil &&
		before.FileCount == after.FileCount
}

func findLastAppendAtNsFromCopiedFiles(idxFileName, datFileName string, version needle.Version) (uint64, error) {
	if version < needle.Version3 {
		return 0, nil
	}

	idxFile, err := os.Open(idxFileName)
	if err != nil {
		return 0, fmt.Errorf("open idx file %s: %w", idxFileName, err)
	}
	defer idxFile.Close()

	fi, err := idxFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat idx file %s: %w", idxFileName, err)
	}
	if fi.Size() == 0 {
		return 0, nil
	}
	if fi.Size()%int64(types.NeedleMapEntrySize) != 0 {
		return 0, fmt.Errorf("unexpected idx file %s size: %d", idxFileName, fi.Size())
	}

	buf := make([]byte, types.NeedleMapEntrySize)
	if _, err := idxFile.ReadAt(buf, fi.Size()-int64(types.NeedleMapEntrySize)); err != nil {
		return 0, fmt.Errorf("read idx file %s: %w", idxFileName, err)
	}
	_, offset, _ := idx.IdxFileEntry(buf)
	if offset.IsZero() {
		return 0, nil
	}

	datFile, err := os.Open(datFileName)
	if err != nil {
		return 0, fmt.Errorf("open dat file %s: %w", datFileName, err)
	}
	defer datFile.Close()

	datBackend := backend.NewDiskFile(datFile)
	n, _, _, err := needle.ReadNeedleHeader(datBackend, version, offset.ToActualOffset())
	if err != nil {
		return 0, fmt.Errorf("read needle header %s offset %d: %w", datFileName, offset.ToActualOffset(), err)
	}

	tailOffset := offset.ToActualOffset() + int64(types.NeedleHeaderSize) + int64(n.Size)
	tail := make([]byte, needle.NeedleChecksumSize+types.TimestampSize)
	readCount, readErr := datBackend.ReadAt(tail, tailOffset)
	if readErr == io.EOF && readCount == len(tail) {
		readErr = nil
	}
	if readErr != nil {
		return 0, fmt.Errorf("read needle tail %s offset %d: %w", datFileName, tailOffset, readErr)
	}

	return util.BytesToUint64(tail[needle.NeedleChecksumSize : needle.NeedleChecksumSize+types.TimestampSize]), nil
}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string, wt *util.WriteThrottler, isAppend, ignoreSourceFileNotFound bool, progressFn storage.ProgressFunc) (modifiedTsNs int64, err error) {
	glog.V(4).Infof("writing to %s", fileName)

	// For an optional copy (ignoreSourceFileNotFound), stage into a temp sibling
	// and atomically rename on success, so a source that lacks the file cannot
	// truncate a valid pre-existing destination. Mandatory copies write in place.
	writePath := fileName
	stageThenCommit := ignoreSourceFileNotFound && !isAppend
	if stageThenCommit {
		writePath = fileName + ".copying"
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if isAppend {
		flags = os.O_WRONLY | os.O_CREATE
	}
	dst, err := os.OpenFile(writePath, flags, 0644)
	if err != nil {
		return modifiedTsNs, fmt.Errorf("open file %s: %w", writePath, err)
	}
	// Track the destination handle through a closer that runs at most once.
	// On Windows os.Remove fails while the file is still open, so any path
	// that wants to delete the file we just created must close the handle
	// first. The deferred call here is the safety net for normal returns.
	dstClosed := false
	closeDst := func() {
		if dstClosed {
			return
		}
		dstClosed = true
		_ = dst.Close()
	}
	defer closeDst()

	// removeIncomplete deletes the partially-written file we just opened
	// with O_TRUNC. Used on stream / write / cancellation errors so a
	// caller (notably VolumeEcShardsCopy distributing .ecx) doesn't end
	// up with a 0-byte stub that downstream code mistakes for a valid
	// empty file. Skip in isAppend mode — the existing content is not
	// ours to remove, and resumable appends rely on partial state.
	removeIncomplete := func(reason string) {
		if isAppend {
			return
		}
		closeDst()
		if removeErr := os.Remove(writePath); removeErr != nil && !os.IsNotExist(removeErr) {
			glog.Warningf("failed to remove incomplete file %s after %s: %v", writePath, reason, removeErr)
		} else if removeErr == nil {
			glog.V(1).Infof("removed incomplete file %s after %s", writePath, reason)
		}
	}

	var progressedBytes int64
	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if resp != nil && resp.ModifiedTsNs != 0 {
			modifiedTsNs = resp.ModifiedTsNs
		}
		if receiveErr != nil {
			removeIncomplete("receive error")
			return modifiedTsNs, fmt.Errorf("receiving %s: %w", fileName, receiveErr)
		}
		if _, writeErr := dst.Write(resp.FileContent); writeErr != nil {
			removeIncomplete("write error")
			return modifiedTsNs, fmt.Errorf("write file %s: %w", fileName, writeErr)
		}
		progressedBytes += int64(len(resp.FileContent))
		if progressFn != nil {
			if !progressFn(progressedBytes) {
				removeIncomplete("progress cancelled")
				return modifiedTsNs, fmt.Errorf("interrupted copy operation")
			}
		}
		wt.MaybeSlowdown(int64(len(resp.FileContent)))
	}
	// If we never received a modifiedTsNs, it means the source file did not exist.
	// Remove the empty file we created to avoid leaving corrupted empty files.
	// Note: We check modifiedTsNs (not progressedBytes) because an empty source file
	// is valid and should result in an empty destination file.
	if modifiedTsNs == 0 && !isAppend {
		closeDst()
		if removeErr := os.Remove(writePath); removeErr != nil && !os.IsNotExist(removeErr) {
			glog.V(1).Infof("failed to remove empty file %s: %v", writePath, removeErr)
		} else if removeErr == nil {
			glog.V(1).Infof("removed empty file %s (source file not found)", writePath)
		}
		return modifiedTsNs, nil
	}

	// Commit the staged temp into place.
	if stageThenCommit {
		closeDst()
		if renameErr := os.Rename(writePath, fileName); renameErr != nil {
			os.Remove(writePath)
			return modifiedTsNs, fmt.Errorf("commit copied file %s: %w", fileName, renameErr)
		}
	}
	return modifiedTsNs, nil
}

func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId
	datSize, idxSize, modTime := v.FileStat()
	resp.DatFileSize = datSize
	resp.IdxFileSize = idxSize
	resp.DatFileTimestampSeconds = uint64(modTime.Unix())
	resp.IdxFileTimestampSeconds = uint64(modTime.Unix())
	resp.FileCount = v.FileCount()
	resp.CompactionRevision = uint32(v.CompactionRevision)
	resp.Collection = v.Collection
	resp.DiskType = string(v.DiskType())
	resp.VolumeInfo = v.GetVolumeInfo()
	resp.Version = uint32(v.Version())
	resp.ExpireAtSec = v.ExpireAtSec()
	return resp, nil
}

// checkVolumeFileExtension guards the client-supplied Ext that CopyFile and
// ReceiveFile turn into an on-disk path. Both RPCs are intentionally ungated
// for cluster-internal peers (see volume_grpc_admin_auth_coverage_test.go), so
// this is the only check standing between a peer request and the os.Open /
// os.Create target: without it an Ext like "/../../x" is joined onto the volume
// directory and, once path-cleaned, resolves outside it. A genuine extension is
// a leading dot followed by alphanumerics -- ".dat", ".idx", ".vif", ".ecx",
// ".ecj", ".ecsum", ".ec00".. -- and never contains a separator or "..".
func checkVolumeFileExtension(ext string) error {
	if len(ext) < 2 || ext[0] != '.' {
		return fmt.Errorf("invalid file extension %q", ext)
	}
	for _, r := range ext[1:] {
		if r < '0' || (r > '9' && r < 'A') || (r > 'Z' && r < 'a') || r > 'z' {
			return fmt.Errorf("invalid file extension %q", ext)
		}
	}
	return nil
}

// checkVolumeCollection guards the client-supplied Collection, which CopyFile
// and ReceiveFile fold into a path component ("<collection>_<vid>"). An empty
// collection is the default; any other value must be a single path element so a
// collection like "../../x" cannot climb out of the volume directory once
// path-cleaned. Collection names are user-facing and may hold '.' or '-', so
// this rejects only separators and bare parent references rather than the
// stricter alphanumeric rule used for extensions.
func checkVolumeCollection(collection string) error {
	if collection == "." || collection == ".." || strings.ContainsAny(collection, `/\`) {
		return fmt.Errorf("invalid collection %q", collection)
	}
	return nil
}

// CopyFile client pulls the volume related file from the source server.
// if req.CompactionRevision != math.MaxUint32, it ensures the compact revision is as expected
// The copying still stop at req.StopOffset, but you can set it to math.MaxUint64 in order to read all data.
func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {

	if err := checkVolumeFileExtension(req.Ext); err != nil {
		return err
	}
	if err := checkVolumeCollection(req.Collection); err != nil {
		return err
	}

	var fileName string
	if !req.IsEcVolume {
		v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
		if v == nil {
			return fmt.Errorf("not found volume id %d", req.VolumeId)
		}

		if uint32(v.CompactionRevision) != req.CompactionRevision && req.CompactionRevision != math.MaxUint32 {
			return fmt.Errorf("volume %d is compacted", req.VolumeId)
		}
		v.SyncToDisk()
		fileName = v.FileName(req.Ext)
	} else {
		// Sync EC volume files to disk before copying to ensure deletions are visible
		// This fixes issue #7751 where deleted files in encoded volumes were not
		// properly marked as deleted when decoded.
		if ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.VolumeId)); found {
			ecVolume.Sync()
		}

		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + req.Ext
		for _, location := range vs.store.Locations {
			tName := util.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
			tName = util.Join(location.IdxDirectory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			if req.IgnoreSourceFileNotFound {
				return nil
			}
			return fmt.Errorf("CopyFile not found ec volume id %d", req.VolumeId)
		}
	}

	bytesToRead := int64(req.StopOffset)

	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// If file doesn't exist and we're asked to copy 0 bytes (empty file),
			// or if IgnoreSourceFileNotFound is set, treat as success
			if req.IgnoreSourceFileNotFound || req.StopOffset == 0 {
				return nil
			}
		}
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileModTsNs := fileInfo.ModTime().UnixNano()

	buffer := make([]byte, BufferSizeLimit)

	for bytesToRead > 0 {
		bytesread, err := file.Read(buffer)

		// println(fileName, "read", bytesread, "bytes, with target", bytesToRead)

		if err != nil {
			if err != io.EOF {
				return err
			}
			// println(fileName, "read", bytesread, "bytes, with target", bytesToRead, "err", err.Error())
			break
		}

		if int64(bytesread) > bytesToRead {
			bytesread = int(bytesToRead)
		}
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent:  buffer[:bytesread],
			ModifiedTsNs: fileModTsNs,
		})
		if err != nil {
			// println("sending", bytesread, "bytes err", err.Error())
			return err
		}
		fileModTsNs = 0 // only send once

		bytesToRead -= int64(bytesread)

	}

	// If no data has been sent in the loop (e.g. for an empty file, or when stopOffset is 0),
	// we still need to send the ModifiedTsNs so the client knows the source file exists.
	// fileModTsNs is set to 0 after the first send, so if it's still non-zero,
	// we haven't sent anything yet.
	if fileModTsNs != 0 {
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			ModifiedTsNs: fileModTsNs,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// diskHoldsEcShardFile reports whether dir contains any <vid>.ecNN shard file,
// so a decoded <vid>.dat is never staged beside a shard. Unlike FindEcVolume,
// it also catches shards present on disk but not mounted.
func diskHoldsEcShardFile(dir, collection string, vid needle.VolumeId) bool {
	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		if fi, err := os.Stat(base + erasure_coding.ToExt(i)); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return true
		}
	}
	return false
}

// ReceiveFile receives a file stream from client and writes it to storage
func (vs *VolumeServer) ReceiveFile(stream volume_server_pb.VolumeServer_ReceiveFileServer) error {
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}

	var fileInfo *volume_server_pb.ReceiveFileInfo
	var targetFile *os.File
	var filePath string
	var bytesWritten uint64
	// Set while an EC .ecj is being received; released only after the file is
	// closed and any partial copy removed.
	var ecjWriteDone func()

	defer func() {
		if targetFile != nil {
			targetFile.Close()
		}
		if ecjWriteDone != nil {
			ecjWriteDone()
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Stream completed successfully
			if targetFile != nil {
				targetFile.Sync()
				glog.V(1).Infof("Successfully received file %s (%d bytes)", filePath, bytesWritten)
			}
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				BytesWritten: bytesWritten,
			})
		}
		if err != nil {
			// Clean up on error
			if targetFile != nil {
				targetFile.Close()
				os.Remove(filePath)
			}
			glog.Errorf("Failed to receive stream: %v", err)
			return fmt.Errorf("failed to receive stream: %v", err)
		}

		switch data := req.Data.(type) {
		case *volume_server_pb.ReceiveFileRequest_Info:
			// First message contains file info
			fileInfo = data.Info
			glog.V(1).Infof("ReceiveFile: volume %d, ext %s, collection %s, shard %d, size %d",
				fileInfo.VolumeId, fileInfo.Ext, fileInfo.Collection, fileInfo.ShardId, fileInfo.FileSize)

			if err := checkVolumeFileExtension(fileInfo.Ext); err != nil {
				glog.Errorf("ReceiveFile: %v", err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: err.Error(),
				})
			}
			if err := checkVolumeCollection(fileInfo.Collection); err != nil {
				glog.Errorf("ReceiveFile: %v", err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: err.Error(),
				})
			}

			if fileInfo.IsEcVolume {
				// os.Create below truncates in place; a mounted EcVolume
				// holds fds on the same inodes, so overwriting corrupts
				// live readers.
				if _, mounted := vs.store.FindEcVolume(needle.VolumeId(fileInfo.VolumeId)); mounted {
					mountedDisks := vs.store.FindEcVolumeDiskIds(needle.VolumeId(fileInfo.VolumeId))
					glog.Errorf("ReceiveFile: ec volume %d is mounted on disk_ids:%v; refusing overwrite for %s", fileInfo.VolumeId, mountedDisks, fileInfo.Ext)
					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: fmt.Sprintf("ec volume %d is mounted on disk_ids:%v; unmount before ReceiveFile", fileInfo.VolumeId, mountedDisks),
					})
				}

				// disk_id=0 means "unset" (protobuf default), so auto-select
				// using the same primitive as VolumeEcShardsCopy: prefer a
				// disk that has the EC volume mounted, then a disk that owns
				// the .ecx on disk (the volume hasn't been mounted yet —
				// relevant when shards stream in mid-rebuild before any
				// mount has happened; see #9212), then any HDD, then any
				// disk.
				var targetLocation *storage.DiskLocation
				if fileInfo.DiskId > 0 {
					if fileInfo.DiskId >= uint32(len(vs.store.Locations)) {
						glog.Errorf("ReceiveFile: invalid disk_id %d: only have %d disks", fileInfo.DiskId, len(vs.store.Locations))
						return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
							Error: fmt.Sprintf("invalid disk_id %d: only have %d disks", fileInfo.DiskId, len(vs.store.Locations)),
						})
					}
					targetLocation = vs.store.Locations[fileInfo.DiskId]
				} else {
					// Pass the build's default data-shard count for the helper's
					// free-slot maths; it's a parameter so custom-ratio builds
					// (e.g. enterprise) can swap it without touching this file.
					targetLocation = vs.store.FindEcShardTargetLocation(fileInfo.Collection, needle.VolumeId(fileInfo.VolumeId), erasure_coding.DataShardsCount)
				}
				if targetLocation == nil {
					glog.Errorf("ReceiveFile: no storage location available")
					return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
						Error: "no storage location available",
					})
				}

				// Create EC shard file path
				baseFileName := erasure_coding.EcShardBaseFileName(fileInfo.Collection, int(fileInfo.VolumeId))
				filePath = util.Join(targetLocation.Directory, baseFileName+fileInfo.Ext)
				// The mounted check above runs once; a volume can still mount on
				// this journal while the stream writes it. Registered as a writer
				// before the file is created, that mount cannot compact the
				// journal and leave the rest of the stream in an unlinked inode.
				if fileInfo.Ext == ".ecj" && ecjWriteDone == nil {
					ecjWriteDone = erasure_coding.BeginEcjWrite(filePath)
				}
			} else {
				// Regular volume file
				v := vs.store.GetVolume(needle.VolumeId(fileInfo.VolumeId))
				if v == nil {
					if fileInfo.DiskType == "" {
						glog.Errorf("ReceiveFile: volume %d not found", fileInfo.VolumeId)
						return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
							Error: fmt.Sprintf("volume %d not found", fileInfo.VolumeId),
						})
					}
					// Staged-new-volume mode (EC decode onto a clean peer): the
					// volume does not exist here yet. Pick a free-slot disk location
					// of the requested medium and stage the file as
					// <base><ext>.copying, to be renamed into place and mounted by
					// VolumeEcShardsToVolume(from_staged). .idx.copying/.vif.copying
					// are not valid volume names, so the scanner never half-loads.
					want := types.ToDiskType(fileInfo.DiskType)
					stagedVid := needle.VolumeId(fileInfo.VolumeId)
					loc := vs.store.FindFreeLocation(func(l *storage.DiskLocation) bool {
						if l.DiskType != want {
							return false
						}
						// Don't stage the decoded .dat onto a disk that holds a shard
						// of this vid. Check the mounted map and the on-disk files, so
						// an unmounted or orphan shard (on disk, absent from the map)
						// is caught too.
						if _, holds := l.FindEcVolume(stagedVid); holds {
							return false
						}
						return !diskHoldsEcShardFile(l.Directory, fileInfo.Collection, stagedVid)
					})
					if loc == nil {
						return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
							Error: fmt.Sprintf("no %s disk location with a free slot for volume %d", fileInfo.DiskType, fileInfo.VolumeId),
						})
					}
					filePath = storage.VolumeFileName(loc.Directory, fileInfo.Collection, int(fileInfo.VolumeId)) + fileInfo.Ext + ".copying"
				} else {
					filePath = v.FileName(fileInfo.Ext)
				}
			}

			// Create target file
			targetFile, err = os.Create(filePath)
			if err != nil {
				glog.Errorf("ReceiveFile: failed to create file %s: %v", filePath, err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to create file: %v", err),
				})
			}
			glog.V(1).Infof("ReceiveFile: created target file %s", filePath)

		case *volume_server_pb.ReceiveFileRequest_FileContent:
			// Subsequent messages contain file content
			if targetFile == nil {
				glog.Errorf("ReceiveFile: file info must be sent first")
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: "file info must be sent first",
				})
			}

			n, err := targetFile.Write(data.FileContent)
			if err != nil {
				targetFile.Close()
				os.Remove(filePath)
				glog.Errorf("ReceiveFile: failed to write to file %s: %v", filePath, err)
				return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
					Error: fmt.Sprintf("failed to write file: %v", err),
				})
			}
			bytesWritten += uint64(n)
			glog.V(2).Infof("ReceiveFile: wrote %d bytes to %s (total: %d)", n, filePath, bytesWritten)

		default:
			glog.Errorf("ReceiveFile: unknown message type")
			return stream.SendAndClose(&volume_server_pb.ReceiveFileResponse{
				Error: "unknown message type",
			})
		}
	}
}
