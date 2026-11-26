package storage

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	idx2 "github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// isSkippableNeedleReadError returns true when a needle read failed because
// the on-disk bytes are unreadable in a permanent way (offset past EOF, header
// corruption, CRC mismatch, malformed v2/v3/v4 fields). Vacuum can safely drop
// such entries during compaction. Anything else (real disk EIO on Unix,
// ERROR_CRC / ERROR_IO_DEVICE on Windows, network timeouts, EROFS, etc.) is
// transient or environmental and must abort the compaction so an operator
// notices, rather than silently dropping recoverable data.
func isSkippableNeedleReadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, needle.ErrorSizeMismatch) ||
		errors.Is(err, needle.ErrorSizeInvalid) ||
		errors.Is(err, needle.ErrorCorrupted)
}

// isVacuumTtlExpired applies the volume TTL when the volume has one, and
// otherwise the needle's own TTL the way reads do.
func isVacuumTtlExpired(n *needle.Needle, volumeTtl *needle.TTL, now time.Time) bool {
	if !n.HasTtl() {
		return false
	}
	if volumeTtl == nil || volumeTtl.Minutes() == 0 {
		return isNeedleTtlExpired(n, now)
	}
	return uint64(now.Unix()) >= n.LastModified+uint64(volumeTtl.Minutes())*60
}

// exceedsExpectedCompactedSize reports whether the compacted .dat is short of
// the live bytes the pre-compaction index snapshot expected.
func exceedsExpectedCompactedSize(expectedLiveBytes uint64, dstDatSize int64) bool {
	return expectedLiveBytes > uint64(dstDatSize)
}

type liveTally struct {
	count int
	bytes uint64
}

func (t *liveTally) add(size Size) {
	t.count++
	t.bytes += uint64(size)
}

func isLiveIndexEntry(offset Offset, size Size) bool {
	return !offset.IsZero() && !size.IsDeleted()
}

// loadIdxSnapshot loads idxPath into nm and tallies its live entries by
// replaying the rows, independently of any later traversal of nm.
func loadIdxSnapshot(nm *needle_map.MemDb, idxPath string) (live liveTally, err error) {
	idxFile, err := os.Open(idxPath)
	if err != nil {
		return live, err
	}
	defer idxFile.Close()
	err = idx2.WalkIndexFile(idxFile, 0, func(key NeedleId, offset Offset, size Size) error {
		if prior, found := nm.Get(key); found && isLiveIndexEntry(prior.Offset, prior.Size) {
			live.count--
			live.bytes -= uint64(prior.Size)
		}
		if !isLiveIndexEntry(offset, size) {
			return nm.Delete(key)
		}
		live.add(size)
		return nm.Set(key, offset, size)
	})
	return live, err
}

// checkCompactedAgainstSnapshot fails when the copy did not account for
// every live entry of the snapshot, or the compacted .dat is short of them.
func checkCompactedAgainstSnapshot(vid needle.VolumeId, snapshot, copied, expired, unreadable liveTally, dstDatSize int64) error {
	if copied.count+expired.count+unreadable.count != snapshot.count || expired.bytes+unreadable.bytes > snapshot.bytes {
		return fmt.Errorf("volume %s compaction accounted for %d of %d live index entries (%d copied, %d expired, %d unreadable)",
			vid, copied.count+expired.count+unreadable.count, snapshot.count, copied.count, expired.count, unreadable.count)
	}
	expectedLiveBytes := snapshot.bytes - expired.bytes - unreadable.bytes
	if exceedsExpectedCompactedSize(expectedLiveBytes, dstDatSize) {
		return fmt.Errorf("volume %s unexpected new data size: %d does not match expected live content size %d from the pre-compaction snapshot",
			vid, dstDatSize, expectedLiveBytes)
	}
	return nil
}

type ProgressFunc func(processed int64) bool

func (v *Volume) garbageLevel() float64 {
	if v.ContentSize() == 0 {
		return 0
	}
	deletedSize := v.DeletedSize()
	fileSize := v.ContentSize()
	if v.DeletedCount() > 0 && v.DeletedSize() == 0 {
		// this happens for .sdx converted back to normal .idx
		// where deleted entry size is missing
		datFileSize, _, _ := v.FileStat()
		deletedSize = datFileSize - fileSize - super_block.SuperBlockSize
		fileSize = datFileSize
	}
	return float64(deletedSize) / float64(fileSize)
}

// errTieredVolumeCompact refuses a compaction of a volume whose .dat is in the
// remote tier: the copy reads the local .dat, which tiering removes (or leaves
// behind as a stale copy), and the reload after a commit would read the remote
// object through the compacted index.
func errTieredVolumeCompact(vid needle.VolumeId) error {
	return fmt.Errorf("volume %d is tiered to remote storage, cannot compact", vid)
}

type CompactOptions struct {
	PreallocateBytes  int64
	MaxBytesPerSecond int64
	ProgressCallback  ProgressFunc

	// internal state settings
	srcDatPath  string
	srcIdxPath  string
	destDatPath string
	destIdxPath string
	superBlock  super_block.SuperBlock
	version     needle.Version
}

// compact a volume based on deletions in .dat files
func (v *Volume) CompactByVolumeData(opts *CompactOptions) error {
	if opts == nil {
		// default settings
		opts = &CompactOptions{}
	}

	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(3).Infof("Compacting volume %d ...", v.Id)
	if v.HasRemoteFile() {
		return errTieredVolumeCompact(v.Id)
	}
	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")
	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		return v.compactingError()
	}
	defer v.isCompactionInProgress.Store(false)

	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision
	glog.V(3).Infof("creating copies for volume %d ,last offset %d...", v.Id, v.lastCompactIndexOffset)
	if v.DataBackend == nil {
		return fmt.Errorf("volume %d backend is empty remote:%v", v.Id, v.HasRemoteFile())
	}
	nm := v.nm
	if nm == nil {
		return fmt.Errorf("volume %d needle map is nil", v.Id)
	}
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume %d", v.Id)
	}
	if err := nm.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume idx %d", v.Id)
	}

	opts.destDatPath = v.FileName(".cpd")
	opts.destIdxPath = v.FileName(".cpx")
	opts.superBlock = v.SuperBlock
	opts.version = v.Version()
	return v.copyDataAndGenerateIndexFile(opts)
}

// compact a volume based on deletions in .idx files
func (v *Volume) CompactByIndex(opts *CompactOptions) error {
	if opts == nil {
		// default settings
		opts = &CompactOptions{}
	}

	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(3).Infof("Compact2 volume %d ...", v.Id)
	if v.HasRemoteFile() {
		return errTieredVolumeCompact(v.Id)
	}

	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		return v.compactingError()
	}
	defer v.isCompactionInProgress.Store(false)

	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	if v.DataBackend == nil {
		return fmt.Errorf("volume %d backend is empty remote:%v", v.Id, v.HasRemoteFile())
	}
	nm := v.nm
	if nm == nil {
		return fmt.Errorf("volume %d needle map is nil", v.Id)
	}
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume dat %d: %v", v.Id, err)
	}
	if err := nm.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume idx %d: %v", v.Id, err)
	}

	opts.srcDatPath = v.FileName(".dat")
	opts.srcIdxPath = v.FileName(".idx")
	opts.destDatPath = v.FileName(".cpd")
	opts.destIdxPath = v.FileName(".cpx")
	opts.superBlock = v.SuperBlock
	opts.version = v.Version()
	return v.copyDataBasedOnIndexFile(opts)
}

func (v *Volume) CommitCompact() error {
	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(0).Infof("Committing volume %d vacuuming...", v.Id)

	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		return v.compactingError()
	}
	defer v.isCompactionInProgress.Store(false)

	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	glog.V(3).Infof("Got volume %d committing lock...", v.Id)
	if v.HasRemoteFile() {
		// The volume was tiered after the copy started: the reload would read
		// the remote object through the compacted index. Drop the copy and its
		// temporary needle map; no later vacuum of this volume would.
		if v.tmpNm != nil {
			v.tmpNm.Close()
			v.tmpNm = nil
		}
		for _, ext := range []string{".cpd", ".cpx", ".cpldb"} {
			if e := os.RemoveAll(v.FileName(ext)); e != nil {
				glog.V(0).Infof("remove %s: %v", v.FileName(ext), e)
			}
		}
		return errTieredVolumeCompact(v.Id)
	}
	// A duplicate commit must fail before it closes the live files.
	if err := v.checkCompactFiles(); err != nil {
		return err
	}
	if compactErr := v.makeupDiff(v.FileName(".cpd"), v.FileName(".cpx"), v.FileName(".dat"), v.FileName(".idx")); compactErr != nil {
		glog.V(0).Infof("makeupDiff in CommitCompact volume %d failed %v", v.Id, compactErr)
		if e := os.Remove(v.FileName(".cpd")); e != nil && !os.IsNotExist(e) {
			glog.V(0).Infof("remove %s: %v", v.FileName(".cpd"), e)
		}
		if e := os.Remove(v.FileName(".cpx")); e != nil && !os.IsNotExist(e) {
			glog.V(0).Infof("remove %s: %v", v.FileName(".cpx"), e)
		}
		// Report the abandoned compaction rather than a cleanup failure that
		// reconcile rolls back anyway, and never fall through to the reload.
		return compactErr
	}

	if v.nm != nil {
		v.nm.Close()
		v.nm = nil
	}
	if v.DataBackend != nil {
		if err := v.DataBackend.Close(); err != nil {
			glog.V(0).Infof("failed to close volume %d", v.Id)
		}
	}
	v.DataBackend = nil
	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "volume").Dec()

	// makeupDiff has fsynced the .cpd/.cpx contents. Persist a durable .cpc
	// commit marker BEFORE renaming so the two renames are atomic across a
	// crash: a marker on disk means the swap is decided and reconcile rolls
	// forward; no marker means roll back. Without it, a crash between the
	// two renames leaves a stale .idx that a later vacuum compacts to empty.
	if e := v.writeCompactCommitMarker(); e != nil {
		return e
	}
	if e := v.applyCompactSwap(); e != nil {
		return e
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)

	glog.V(3).Infof("Loading volume %d commit file...", v.Id)
	// The write watermark already equals what recoverLastModifiedTs would
	// rescan, so keep the clock instead of paying for the scan under the lock.
	// If its write was itself deleted, the reload recovers the newest
	// surviving write instead.
	if v.lastWriteAppendAtNs != 0 {
		v.lastModifiedTsSeconds = v.lastWriteAppendAtNs / uint64(time.Second)
	}
	v.keepLastModifiedTsOnLoad = v.lastModifiedTsSeconds != 0 && !v.lastWriteDeleted
	e := v.load(true, false, v.needleMapKind, 0, v.Version())
	v.keepLastModifiedTsOnLoad = false
	if e != nil {
		return e
	}
	glog.V(3).Infof("Finish committing volume %d", v.Id)
	return nil
}

func (v *Volume) compactingError() error {
	return fmt.Errorf("volume %d is compacting", v.Id)
}

// writeCompactCommitMarker writes and fsyncs the .cpc marker, then fsyncs the
// directory so the marker's existence survives a crash before applyCompactSwap.
func (v *Volume) writeCompactCommitMarker() error {
	markerPath := v.FileName(".cpc")
	f, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create commit marker %s: %v", markerPath, err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync commit marker %s: %v", markerPath, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close commit marker %s: %v", markerPath, err)
	}
	return fsyncDir(filepath.Dir(markerPath))
}

// applyCompactSwap performs the durable two-rename compaction commit. It is
// idempotent and is run both at the tail of CommitCompact and by
// reconcileCompactState rolling forward after a crash. It requires the .cpc
// marker and BOTH .cpd/.cpx to be present, so a stale or duplicate commit
// returns an error without deleting the live .dat/.idx.
func (v *Volume) applyCompactSwap() error {
	if err := v.checkCompactFiles(); err != nil {
		return err
	}
	return v.finishCompactSwap()
}

// checkCompactFiles requires both temp files of a normal commit, or a
// stale/duplicate commit could clobber the live .dat/.idx.
func (v *Volume) checkCompactFiles() error {
	if !util.FileExists(v.FileName(".cpd")) || !util.FileExists(v.FileName(".cpx")) {
		return fmt.Errorf("volume %d compact swap aborted: missing .cpd/.cpx", v.Id)
	}
	return nil
}

// finishCompactSwap renames whichever compaction temp file is still present
// (.cpd->.dat, .cpx->.idx), fsyncs, drops the stale .ldb/.rdb, then clears the
// .cpc marker. It tolerates a partial state: a crash after the .dat rename but
// before the .idx rename leaves only .cpx, which must still be applied -- not
// abandoned, which would pair a fresh .dat with a stale .idx.
func (v *Volume) finishCompactSwap() error {
	cpdExists := util.FileExists(v.FileName(".cpd"))
	cpxExists := util.FileExists(v.FileName(".cpx"))

	if cpdExists {
		if runtime.GOOS == "windows" {
			if e := os.RemoveAll(v.FileName(".dat")); e != nil {
				return e
			}
		}
		if e := os.Rename(v.FileName(".cpd"), v.FileName(".dat")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpd"), e)
		}
	}
	if cpxExists {
		if runtime.GOOS == "windows" {
			if e := os.RemoveAll(v.FileName(".idx")); e != nil {
				return e
			}
		}
		if e := os.Rename(v.FileName(".cpx"), v.FileName(".idx")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpx"), e)
		}
	}
	if cpdExists || cpxExists {
		if e := fsyncDir(filepath.Dir(v.FileName(".dat"))); e != nil {
			return e
		}
		if v.dir != v.dirIdx {
			if e := fsyncDir(filepath.Dir(v.FileName(".idx"))); e != nil {
				return e
			}
		}
		// A stale .ldb/.rdb mirrors the old .idx; remove it so it can never
		// poison the needle map built from the freshly renamed .idx.
		os.RemoveAll(v.FileName(".ldb"))
		os.Remove(v.FileName(".rdb"))
	}

	// Clear the marker last and fsync the dir so a restart does not re-run a
	// completed swap.
	if e := os.Remove(v.FileName(".cpc")); e != nil && !os.IsNotExist(e) {
		return e
	}
	return fsyncDir(filepath.Dir(v.FileName(".cpc")))
}

// reconcileCompactState recovers an interrupted compaction commit on load. When
// the .cpc marker is present the swap was decided, so roll FORWARD by finishing
// the renames; when it is absent any leftover .cpd/.cpx are an abandoned
// generation, so roll BACK by deleting them (and any stale .ldb left next to a
// healthy .idx). It is keyed only on .cpc/.cpd existence so a crash that left
// just the marker, or an already-renamed .idx, is still handled.
func (v *Volume) reconcileCompactState() error {
	cpcPath := v.FileName(".cpc")
	if util.FileExists(cpcPath) {
		// Marker present: the swap was decided. Finish whichever rename is
		// still pending -- a crash may have completed only the .dat rename, so
		// the lone remaining .cpx must still be applied, not abandoned. If
		// neither temp file remains, finishCompactSwap just clears the marker.
		glog.V(0).Infof("volume %d: rolling forward interrupted compaction commit", v.Id)
		return v.finishCompactSwap()
	}

	// No marker: roll back any orphan compaction temp files.
	rolledBack := false
	for _, ext := range []string{".cpd", ".cpx"} {
		p := v.FileName(ext)
		if util.FileExists(p) {
			glog.V(0).Infof("volume %d: rolling back orphan compaction file %s", v.Id, ext)
			if e := os.Remove(p); e != nil && !os.IsNotExist(e) {
				return e
			}
			rolledBack = true
		}
	}
	if rolledBack {
		// A stale .ldb may mirror an .idx that never got swapped; drop it so the
		// reload rebuilds the needle map from the surviving .idx.
		os.RemoveAll(v.FileName(".ldb"))
		os.Remove(v.FileName(".rdb"))
	}
	return nil
}

func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up volume %d vacuuming...", v.Id)

	// An in-flight copy is still writing .cpd/.cpx; claim the volume so
	// none starts while the files are removed.
	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		return v.compactingError()
	}
	defer v.isCompactionInProgress.Store(false)

	// Serialize with CommitCompact's swap and refuse to unlink .cpd/.cpx while a
	// .cpc marker exists: those temp files are the only inputs reconcile can roll
	// forward to, so removing them mid-commit would strand a decided swap.
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if util.FileExists(v.FileName(".cpc")) {
		return fmt.Errorf("volume %d: refusing cleanup while commit marker present", v.Id)
	}

	e1 := os.Remove(v.FileName(".cpd"))
	e2 := os.Remove(v.FileName(".cpx"))
	e3 := os.RemoveAll(v.FileName(".cpldb"))
	e4 := os.Remove(v.FileName(".cpc"))
	if e1 != nil && !os.IsNotExist(e1) {
		return e1
	}
	if e2 != nil && !os.IsNotExist(e2) {
		return e2
	}
	if e3 != nil && !os.IsNotExist(e3) {
		return e3
	}
	if e4 != nil && !os.IsNotExist(e4) {
		return e4
	}
	return nil
}

// fsyncDir fsyncs a directory so a rename/create/unlink inside it is durable.
// Windows has no directory fsync, so the .cpc protocol leans on NTFS metadata
// ordering there. An unopenable directory is tolerated, unlike util.FsyncDir,
// because the caller has already closed the needle map.
func fsyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	if err = d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return fmt.Errorf("sync dir %s: %v", dir, err)
	}
	return nil
}

func fetchCompactRevisionFromDatFile(datBackend backend.BackendStorageFile) (compactRevision uint16, err error) {
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		return 0, err
	}
	return superBlock.CompactionRevision, nil
}

// if old .dat and .idx files are updated, this func tries to apply the same changes to new files accordingly
func (v *Volume) makeupDiff(newDatFileName, newIdxFileName, oldDatFileName, oldIdxFileName string) (err error) {
	var indexSize int64

	oldIdxFile, err := os.Open(oldIdxFileName)
	if err != nil {
		return fmt.Errorf("makeupDiff open %s failed: %v", oldIdxFileName, err)
	}
	defer oldIdxFile.Close()

	oldDatFile, err := os.Open(oldDatFileName)
	if err != nil {
		return fmt.Errorf("makeupDiff open %s failed: %v", oldDatFileName, err)
	}
	oldDatBackend := backend.NewDiskFile(oldDatFile)
	defer oldDatBackend.Close()

	// skip if the old .idx file has not changed
	if indexSize, err = verifyIndexFileIntegrity(oldIdxFile); err != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", oldIdxFileName, err)
	}
	if indexSize == 0 || uint64(indexSize) <= v.lastCompactIndexOffset {
		return nil
	}

	// fail if the old .dat file has changed to a new revision
	oldDatCompactRevision, err := fetchCompactRevisionFromDatFile(oldDatBackend)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile src %s failed: %v", oldDatFile.Name(), err)
	}
	if oldDatCompactRevision != v.lastCompactRevision {
		return fmt.Errorf("current old dat file's compact revision %d is not the expected one %d", oldDatCompactRevision, v.lastCompactRevision)
	}

	type keyField struct {
		offset Offset
		size   Size
	}
	incrementedHasUpdatedIndexEntry := make(map[NeedleId]keyField)

	for idxOffset := indexSize - NeedleMapEntrySize; uint64(idxOffset) >= v.lastCompactIndexOffset; idxOffset -= NeedleMapEntrySize {
		var IdxEntry []byte
		if IdxEntry, err = readIndexEntryAtOffset(oldIdxFile, idxOffset); err != nil {
			return fmt.Errorf("readIndexEntry %s at offset %d failed: %v", oldIdxFileName, idxOffset, err)
		}
		key, offset, size := idx2.IdxFileEntry(IdxEntry)
		glog.V(4).Infof("key %d offset %d size %d", key, offset, size)
		if _, found := incrementedHasUpdatedIndexEntry[key]; !found {
			incrementedHasUpdatedIndexEntry[key] = keyField{
				offset: offset,
				size:   size,
			}
		}
	}

	// no updates during commit step
	if len(incrementedHasUpdatedIndexEntry) == 0 {
		return nil
	}

	// deal with updates during commit step
	var (
		dst, idx *os.File
	)
	if dst, err = os.OpenFile(newDatFileName, os.O_RDWR, 0644); err != nil {
		return fmt.Errorf("open dat file %s failed: %v", newDatFileName, err)
	}
	dstDatBackend := backend.NewDiskFile(dst)
	defer dstDatBackend.Close()

	if idx, err = os.OpenFile(newIdxFileName, os.O_RDWR, 0644); err != nil {
		return fmt.Errorf("open idx file %s failed: %v", newIdxFileName, err)
	}

	defer func() {
		// makeupDiff appends new needles/tombstones to the .cpx; its fsync is the
		// durability gate that must succeed before CommitCompact writes the .cpc
		// marker and swaps the files. Sync the dat file once after all appends
		// rather than per-needle — a fsync per entry scales poorly on slow disks
		// and can exceed test timeouts with large entry counts.
		if syncErr := dstDatBackend.Sync(); syncErr != nil && err == nil {
			err = fmt.Errorf("sync dat %s: %v", newDatFileName, syncErr)
		}
		if syncErr := idx.Sync(); syncErr != nil && err == nil {
			err = fmt.Errorf("sync idx %s: %v", newIdxFileName, syncErr)
		}
		idx.Close()
	}()

	stat, err := idx.Stat()
	if err != nil {
		return fmt.Errorf("stat file %s: %v", idx.Name(), err)
	}
	idxSize := stat.Size()

	var newDatCompactRevision uint16
	newDatCompactRevision, err = fetchCompactRevisionFromDatFile(dstDatBackend)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile dst %s failed: %v", dst.Name(), err)
	}
	if oldDatCompactRevision+1 != newDatCompactRevision {
		return fmt.Errorf("oldDatFile %s 's compact revision is %d while newDatFile %s 's compact revision is %d", oldDatFileName, oldDatCompactRevision, newDatFileName, newDatCompactRevision)
	}

	for key, increIdxEntry := range incrementedHasUpdatedIndexEntry {

		idxEntryBytes := needle_map.ToBytes(key, increIdxEntry.offset, increIdxEntry.size)

		var offset int64
		if offset, err = dst.Seek(0, 2); err != nil {
			glog.V(0).Infof("failed to seek the end of file: %v", err)
			return
		}
		//ensure file writing starting from aligned positions
		if offset%NeedlePaddingSize != 0 {
			offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
			if offset, err = dst.Seek(offset, 0); err != nil {
				glog.V(0).Infof("failed to align in datafile %s: %v", dst.Name(), err)
				return
			}
		}
		//updated needle
		if !increIdxEntry.offset.IsZero() && !increIdxEntry.size.IsDeleted() {
			//even the needle cache in memory is hit, the need_bytes is correct
			glog.V(4).Infof("file %d offset %d size %d", key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size)
			var needleBytes []byte
			needleBytes, err = needle.ReadNeedleBlob(oldDatBackend, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, v.Version())
			if err != nil {
				v.checkReadWriteError(err)
				return fmt.Errorf("ReadNeedleBlob %s key %d offset %d size %d failed: %w", oldDatFile.Name(), key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, err)
			}
			if _, err = dstDatBackend.Write(needleBytes); err != nil {
				return fmt.Errorf("write %s key %d failed: %w", dst.Name(), key, err)
			}
			idxEntryBytes = needle_map.ToBytes(key, ToOffset(offset), increIdxEntry.size)
		} else { //deleted needle
			//fakeDelNeedle's default Data field is nil
			fakeDelNeedle := new(needle.Needle)
			fakeDelNeedle.Id = key
			fakeDelNeedle.Cookie = 0x12345678
			fakeDelNeedle.AppendAtNs = uint64(time.Now().UnixNano())
			fakeDelOffset, _, _, appendErr := fakeDelNeedle.Append(dstDatBackend, v.Version())
			if appendErr != nil {
				return fmt.Errorf("append deleted %d failed: %v", key, appendErr)
			}
			// Record the tombstone's real .dat offset, like the normal delete path,
			// so a deletion left at the .dat tail stays visible to the integrity
			// check on reload. Offset 0 hid the trailing tombstone and falsely
			// flipped the volume read-only.
			idxEntryBytes = needle_map.ToBytes(key, ToOffset(int64(fakeDelOffset)), increIdxEntry.size)
		}

		if _, err := idx.Seek(0, 2); err != nil {
			return fmt.Errorf("cannot seek end of indexfile %s: %v",
				newIdxFileName, err)
		}
		_, err = idx.Write(idxEntryBytes)
		if err != nil {
			return fmt.Errorf("cannot write indexfile %s: %v", newIdxFileName, err)
		}
	}

	return v.tmpNm.DoOffsetLoading(v, idx, uint64(idxSize)/NeedleMapEntrySize)
}

type VolumeFileScanner4Vacuum struct {
	version        needle.Version
	v              *Volume
	dstBackend     backend.BackendStorageFile
	nm             *needle_map.MemDb
	newOffset      int64
	now            uint64
	writeThrottler *util.WriteThrottler
	dropped        map[int64]bool
	resumed        map[int64]bool
	datSize        int64
}

func (scanner *VolumeFileScanner4Vacuum) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	superBlock.CompactionRevision++
	_, err := scanner.dstBackend.WriteAt(superBlock.Bytes(), 0)
	scanner.newOffset = int64(superBlock.BlockSize())
	return err

}
func (scanner *VolumeFileScanner4Vacuum) ReadNeedleBody() bool {
	return false
}

// reached reports whether the scan already copied or dropped the live record
// at offset.
func (scanner *VolumeFileScanner4Vacuum) reached(key NeedleId, offset int64) bool {
	_, copied := scanner.nm.Get(key)
	return copied || scanner.dropped[offset]
}

// checkRecordEnd fails a record whose header size runs past the end of the
// .dat, before that size is used to allocate the read buffer.
func (scanner *VolumeFileScanner4Vacuum) checkRecordEnd(offset int64, size Size) error {
	end := offset + needle.GetActualSize(size, scanner.v.Version())
	if end <= scanner.datSize {
		return nil
	}
	datSize, _, err := scanner.v.DataBackend.GetStat()
	if err != nil {
		return err
	}
	scanner.datSize = datSize
	if end > datSize {
		return fmt.Errorf("record of size %d at offset %d ends past the %d-byte file: %w", size, offset, datSize, io.ErrUnexpectedEOF)
	}
	return nil
}

func (scanner *VolumeFileScanner4Vacuum) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	if scanner.resumed != nil {
		if scanner.resumed[offset] {
			return io.EOF
		}
		scanner.resumed[offset] = true
	}
	nv, ok := scanner.v.nm.Get(n.Id)
	glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
	if !ok || nv.Offset.ToActualOffset() != offset {
		return nil
	}
	if scanner.reached(n.Id, offset) {
		return io.EOF
	}
	err := scanner.checkRecordEnd(offset, n.Size)
	if err == nil {
		err = n.ReadData(scanner.v.DataBackend, offset, n.Size, scanner.v.Version())
	}
	if err != nil {
		if !isSkippableNeedleReadError(err) {
			return fmt.Errorf("read needle %d at offset %d: %w", n.Id, offset, err)
		}
		glog.Errorf("compact volume %d: dropping unreadable needle key=%d offset=%d size=%d: %v", scanner.v.Id, n.Id, offset, n.Size, err)
		scanner.dropped[offset] = true
		return nil
	}
	if isVacuumTtlExpired(n, scanner.v.Ttl, time.Unix(int64(scanner.now), 0)) {
		scanner.dropped[offset] = true
		return nil
	}
	if ok && nv.Offset.ToActualOffset() == offset && nv.Size > 0 && nv.Size.IsValid() {
		if err := scanner.nm.Set(n.Id, ToOffset(scanner.newOffset), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}
		if _, _, _, err := n.Append(scanner.dstBackend, scanner.v.Version()); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}
		delta := n.DiskSize(scanner.version)
		scanner.newOffset += delta
		scanner.writeThrottler.MaybeSlowdown(delta)
		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", scanner.newOffset, "data_size", n.Size)
	}
	return nil
}

func (v *Volume) copyDataAndGenerateIndexFile(opts *CompactOptions) (err error) {
	var dst backend.BackendStorageFile
	if dst, err = backend.CreateVolumeFile(opts.destDatPath, opts.PreallocateBytes, 0); err != nil {
		return err
	}
	defer dst.Close()

	nm := needle_map.NewMemDb()
	defer nm.Close()

	scanner := &VolumeFileScanner4Vacuum{
		v:              v,
		now:            uint64(time.Now().Unix()),
		nm:             nm,
		dropped:        make(map[int64]bool),
		dstBackend:     dst,
		writeThrottler: util.NewWriteThrottler(opts.MaxBytesPerSecond),
	}
	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind, scanner)
	if scanner.newOffset > 0 {
		err = v.copyPastUnreadableRecords(scanner, err)
	}
	if err != nil {
		v.checkReadWriteError(err)
		return err
	}

	if err = nm.SaveToIdx(opts.destIdxPath); err != nil {
		return err
	}
	return v.loadTmpNeedleMap(opts.destIdxPath)
}

// copyPastUnreadableRecords resumes the scan at each live record it did not
// reach. The scan follows record headers, so a corrupt one ends it early or
// makes it jump over records; compacting by index copies past such a record,
// and so does this. A read error still fails the compaction.
func (v *Volume) copyPastUnreadableRecords(scanner *VolumeFileScanner4Vacuum, scanErr error) error {
	if scanErr != nil {
		if !errors.Is(scanErr, needle.ErrorCorrupted) {
			return scanErr
		}
		glog.Warningf("compact volume %d: %v", v.Id, scanErr)
	}
	missed, err := v.liveRecordsNotReached(scanner)
	if err != nil {
		return err
	}
	scanner.resumed = make(map[int64]bool)
	for _, record := range missed {
		offset := record.Offset.ToActualOffset()
		if scanner.reached(record.Key, offset) || scanner.resumed[offset] {
			continue
		}
		glog.Warningf("compact volume %d: scan did not reach live record %d at offset %d, resuming there", v.Id, record.Key, offset)
		if err := ScanVolumeFileFrom(v.Version(), v.DataBackend, offset, scanner); err != nil {
			if !errors.Is(err, needle.ErrorCorrupted) {
				return err
			}
			glog.Warningf("compact volume %d: %v", v.Id, err)
		}
	}
	return nil
}

// liveRecordsNotReached returns, in .dat order, the live records of the index
// the scan has not reached.
func (v *Volume) liveRecordsNotReached(scanner *VolumeFileScanner4Vacuum) ([]needle_map.NeedleValue, error) {
	indexFile, err := os.Open(v.FileName(".idx"))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", v.FileName(".idx"), err)
	}
	defer indexFile.Close()
	var records []needle_map.NeedleValue
	err = idx2.WalkIndexFile(indexFile, 0, func(key NeedleId, offset Offset, size Size) error {
		if nv, ok := v.nm.Get(key); ok && nv.Offset == offset && nv.Size > 0 && nv.Size.IsValid() && !scanner.reached(key, offset.ToActualOffset()) {
			records = append(records, *nv)
		}
		return nil
	})
	slices.SortFunc(records, func(a, b needle_map.NeedleValue) int {
		return cmp.Compare(a.Offset.ToActualOffset(), b.Offset.ToActualOffset())
	})
	return records, err
}

func (v *Volume) copyDataBasedOnIndexFile(opts *CompactOptions) (err error) {
	var (
		srcDatBackend, dstDatBackend backend.BackendStorageFile
		dataFile                     *os.File
		snapshot                     liveTally
		copied, expired, unreadable  liveTally
	)
	if dstDatBackend, err = backend.CreateVolumeFile(opts.destDatPath, opts.PreallocateBytes, 0); err != nil {
		return err
	}
	defer func() {
		// DiskFile.Close performs the final fsync, so its error is the durability
		// signal for the .cpd contents. Surface it (only when no earlier error is
		// already being returned) so a failed flush aborts the compaction instead
		// of leaving a half-written .cpd that CommitCompact would rename live.
		if closeErr := dstDatBackend.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close compacted dat %s: %v", opts.destDatPath, closeErr)
		}
		if err == nil && unreadable.count > 0 {
			stats.VolumeServerVacuumingDroppedNeedlesCounter.WithLabelValues(v.Collection).Add(float64(unreadable.count))
		}
	}()

	oldNm := needle_map.NewMemDb()
	defer oldNm.Close()
	newNm := needle_map.NewMemDb()
	defer newNm.Close()
	if snapshot, err = loadIdxSnapshot(oldNm, opts.srcIdxPath); err != nil {
		return err
	}
	if dataFile, err = os.Open(opts.srcDatPath); err != nil {
		return err
	}
	srcDatBackend = backend.NewDiskFile(dataFile)
	defer srcDatBackend.Close()

	now := time.Now()

	opts.superBlock.CompactionRevision++
	newOffset := int64(opts.superBlock.BlockSize())
	if _, err := dstDatBackend.WriteAt(opts.superBlock.Bytes(), 0); err != nil {
		return fmt.Errorf("failed to write superblock: %v", err)
	}

	writeThrottler := util.NewWriteThrottler(opts.MaxBytesPerSecond)
	err = oldNm.AscendingVisit(func(value needle_map.NeedleValue) error {

		offset, size := value.Offset, value.Size

		if !isLiveIndexEntry(offset, size) {
			return nil
		}

		if opts.ProgressCallback != nil {
			if !opts.ProgressCallback(offset.ToActualOffset()) {
				return fmt.Errorf("interrupted")
			}
		}

		n := new(needle.Needle)
		if err := n.ReadData(srcDatBackend, offset.ToActualOffset(), size, opts.version); err != nil {
			v.checkReadWriteError(err)
			// Only drop the entry when the failure is one of the well-known
			// permanent-corruption shapes. A transient disk fault, a tiered
			// read timeout, or a Windows hardware error (ERROR_CRC etc.) must
			// abort so an operator notices, rather than silently compacting
			// away data that might come back on retry. See issue #8928.
			if !isSkippableNeedleReadError(err) {
				return fmt.Errorf("cannot hydrate needle from file: %w", err)
			}
			unreadable.add(size)
			glog.Errorf("vacuum volume %d: dropping unreadable needle key=%d offset=%d size=%d: %v",
				v.Id, value.Key, offset.ToActualOffset(), size, err)
			return nil
		}

		if isVacuumTtlExpired(n, opts.superBlock.Ttl, now) {
			expired.add(size)
			glog.V(1).Infof("vacuum volume %d: dropping TTL-expired needle key=%d offset=%d size=%d",
				v.Id, value.Key, offset.ToActualOffset(), size)
			return nil
		}
		copied.add(size)

		if err = newNm.Set(n.Id, ToOffset(newOffset), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}
		if _, _, _, err = n.Append(dstDatBackend, opts.superBlock.Version); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}
		delta := n.DiskSize(opts.version)
		newOffset += delta
		writeThrottler.MaybeSlowdown(delta)
		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", newOffset, "data_size", n.Size)

		return nil
	})
	if err != nil {
		return err
	}
	if unreadable.count > 0 {
		glog.Warningf("vacuum volume %d: dropped %d unreadable index entries (%d data bytes) during compaction",
			v.Id, unreadable.count, unreadable.bytes)
	}
	if expired.count > 0 {
		glog.V(1).Infof("vacuum volume %d: dropped %d TTL-expired needles (%d data bytes)", v.Id, expired.count, expired.bytes)
	}
	dstDatSize, _, err := dstDatBackend.GetStat()
	if err != nil {
		return err
	}
	if err = checkCompactedAgainstSnapshot(v.Id, snapshot, copied, expired, unreadable, dstDatSize); err != nil {
		return err
	}
	err = newNm.SaveToIdx(opts.destIdxPath)
	if err != nil {
		return err
	}
	return v.loadTmpNeedleMap(opts.destIdxPath)
}

// loadTmpNeedleMap loads the compacted index into v.tmpNm. CommitCompact's
// makeupDiff applies the writes and deletes that landed during the copy on top
// of it, so every compaction path must build it.
func (v *Volume) loadTmpNeedleMap(idxPath string) (err error) {
	indexFile, err := os.OpenFile(idxPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Errorf("cannot open Volume Index %s: %v", idxPath, err)
		return err
	}
	defer func() {
		if syncErr := indexFile.Sync(); syncErr != nil && err == nil {
			err = fmt.Errorf("sync compacted idx %s: %v", idxPath, syncErr)
		}
		indexFile.Close()
	}()
	if v.tmpNm != nil {
		v.tmpNm.Close()
		v.tmpNm = nil
	}
	if v.needleMapKind == NeedleMapInMemory {

		nm := &NeedleMap{
			m: needle_map.NewCompactMap(),
		}
		v.tmpNm = nm
		//can be optimized, filling nm in oldNm.AscendingVisit
		err = v.tmpNm.DoOffsetLoading(nil, indexFile, 0)
		return err
	} else {
		dbFileName := v.FileName(".ldb")
		m := &LevelDbNeedleMap{dbFileName: dbFileName}
		m.dbFileName = dbFileName
		mm := &mapMetric{}
		m.mapMetric = *mm
		v.tmpNm = m
		err = v.tmpNm.DoOffsetLoading(v, indexFile, 0)
		if err != nil {
			return err
		}
	}
	return
}
