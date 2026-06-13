package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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
	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")
	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		glog.V(0).Infof("Volume %d is already compacting...", v.Id)
		return nil
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

	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		glog.V(0).Infof("Volume %d is already compacting2 ...", v.Id)
		return nil
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
		glog.V(0).Infof("Volume %d is already compacting ...", v.Id)
		return nil
	}
	defer v.isCompactionInProgress.Store(false)

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	glog.V(3).Infof("Got volume %d committing lock...", v.Id)
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

	var e error
	if e = v.makeupDiff(v.FileName(".cpd"), v.FileName(".cpx"), v.FileName(".dat"), v.FileName(".idx")); e != nil {
		glog.V(0).Infof("makeupDiff in CommitCompact volume %d failed %v", v.Id, e)
		e = os.Remove(v.FileName(".cpd"))
		if e != nil {
			return e
		}
		e = os.Remove(v.FileName(".cpx"))
		if e != nil {
			return e
		}
	} else {
		// makeupDiff has fsynced the .cpd/.cpx contents. Persist a durable .cpc
		// commit marker BEFORE renaming so the two renames are atomic across a
		// crash: a marker on disk means the swap is decided and reconcile rolls
		// forward; no marker means roll back. Without it, a crash between the
		// two renames leaves a stale .idx that a later vacuum compacts to empty.
		if e = v.writeCompactCommitMarker(); e != nil {
			return e
		}
		if e = v.applyCompactSwap(); e != nil {
			return e
		}
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)

	glog.V(3).Infof("Loading volume %d commit file...", v.Id)
	if e = v.load(true, false, v.needleMapKind, 0, v.Version()); e != nil {
		return e
	}
	glog.V(3).Infof("Finish committing volume %d", v.Id)
	return nil
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
	cpdPath := v.FileName(".cpd")
	cpxPath := v.FileName(".cpx")
	if !util.FileExists(cpdPath) || !util.FileExists(cpxPath) {
		return fmt.Errorf("volume %d compact swap aborted: missing .cpd/.cpx", v.Id)
	}

	if runtime.GOOS == "windows" {
		// Windows cannot rename over an existing file; remove the targets only
		// after the guard above confirms the replacements are present.
		if e := os.RemoveAll(v.FileName(".dat")); e != nil {
			return e
		}
		if e := os.RemoveAll(v.FileName(".idx")); e != nil {
			return e
		}
	}
	if e := os.Rename(cpdPath, v.FileName(".dat")); e != nil {
		return fmt.Errorf("rename %s: %v", cpdPath, e)
	}
	if e := os.Rename(cpxPath, v.FileName(".idx")); e != nil {
		return fmt.Errorf("rename %s: %v", cpxPath, e)
	}
	if e := fsyncDir(filepath.Dir(v.FileName(".dat"))); e != nil {
		return e
	}
	if v.dir != v.dirIdx {
		if e := fsyncDir(filepath.Dir(v.FileName(".idx"))); e != nil {
			return e
		}
	}

	// A stale .ldb/.rdb mirrors the old .idx; remove it as part of the swap so
	// it can never poison the needle map built from the freshly renamed .idx.
	os.RemoveAll(v.FileName(".ldb"))
	os.Remove(v.FileName(".rdb"))

	// The swap is now durable; clear the marker last and fsync the dir so a
	// later restart does not re-run a completed swap.
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
		if util.FileExists(v.FileName(".cpd")) && util.FileExists(v.FileName(".cpx")) {
			glog.V(0).Infof("volume %d: rolling forward interrupted compaction commit", v.Id)
			return v.applyCompactSwap()
		}
		// The marker outlived its .cpd/.cpx: the swap already finished but the
		// final marker removal did not. Clear it so we do not loop on reload.
		glog.V(0).Infof("volume %d: clearing orphan commit marker (swap already applied)", v.Id)
		if e := os.Remove(cpcPath); e != nil && !os.IsNotExist(e) {
			return e
		}
		return nil
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

	// Serialize with CommitCompact's swap and refuse to unlink .cpd/.cpx while a
	// .cpc marker exists: those temp files are the only inputs reconcile can roll
	// forward to, so removing them mid-commit would strand a decided swap.
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
// A failure to open the directory for sync is non-fatal on platforms that do
// not support it.
func fsyncDir(dir string) error {
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
		// marker and swaps the files.
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
		if !increIdxEntry.offset.IsZero() && increIdxEntry.size != 0 && increIdxEntry.size.IsValid() {
			//even the needle cache in memory is hit, the need_bytes is correct
			glog.V(4).Infof("file %d offset %d size %d", key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size)
			var needleBytes []byte
			needleBytes, err = needle.ReadNeedleBlob(oldDatBackend, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, v.Version())
			if err != nil {
				v.checkReadWriteError(err)
				return fmt.Errorf("ReadNeedleBlob %s key %d offset %d size %d failed: %w", oldDatFile.Name(), key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, err)
			}
			dstDatBackend.Write(needleBytes)
			if err := dstDatBackend.Sync(); err != nil {
				return fmt.Errorf("cannot sync needle %s: %v", dstDatBackend.File.Name(), err)
			}
			util.Uint32toBytes(idxEntryBytes[8:12], uint32(offset/NeedlePaddingSize))
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
}

func (scanner *VolumeFileScanner4Vacuum) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	superBlock.CompactionRevision++
	_, err := scanner.dstBackend.WriteAt(superBlock.Bytes(), 0)
	scanner.newOffset = int64(superBlock.BlockSize())
	return err

}
func (scanner *VolumeFileScanner4Vacuum) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Vacuum) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	if n.HasTtl() && scanner.now >= n.LastModified+uint64(scanner.v.Ttl.Minutes()*60) {
		return nil
	}
	nv, ok := scanner.v.nm.Get(n.Id)
	glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
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
		dstBackend:     dst,
		writeThrottler: util.NewWriteThrottler(opts.MaxBytesPerSecond),
	}
	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind, scanner)
	if err != nil {
		v.checkReadWriteError(err)
		return err
	}

	return nm.SaveToIdx(opts.destIdxPath)
}

func (v *Volume) copyDataBasedOnIndexFile(opts *CompactOptions) (err error) {
	var (
		srcDatBackend, dstDatBackend backend.BackendStorageFile
		dataFile                     *os.File
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
	}()

	oldNm := needle_map.NewMemDb()
	defer oldNm.Close()
	newNm := needle_map.NewMemDb()
	defer newNm.Close()
	if err = oldNm.LoadFromIdx(opts.srcIdxPath); err != nil {
		return err
	}
	if dataFile, err = os.Open(opts.srcDatPath); err != nil {
		return err
	}
	srcDatBackend = backend.NewDiskFile(dataFile)
	defer srcDatBackend.Close()

	now := uint64(time.Now().Unix())

	opts.superBlock.CompactionRevision++
	newOffset := int64(opts.superBlock.BlockSize())
	if _, err := dstDatBackend.WriteAt(opts.superBlock.Bytes(), 0); err != nil {
		return fmt.Errorf("failed to write superblock: %v", err)
	}

	writeThrottler := util.NewWriteThrottler(opts.MaxBytesPerSecond)
	var (
		skippedNeedles   int
		skippedDataBytes uint64
	)
	err = oldNm.AscendingVisit(func(value needle_map.NeedleValue) error {

		offset, size := value.Offset, value.Size

		if offset.IsZero() || size.IsDeleted() {
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
			skippedNeedles++
			if size.IsValid() {
				skippedDataBytes += uint64(size)
			}
			glog.Warningf("vacuum volume %d: dropping unreadable needle key=%d offset=%d size=%d: %v",
				v.Id, value.Key, offset.ToActualOffset(), size, err)
			return nil
		}

		if n.HasTtl() && now >= n.LastModified+uint64(opts.superBlock.Ttl.Minutes()*60) {
			return nil
		}

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
	if skippedNeedles > 0 {
		glog.Warningf("vacuum volume %d: dropped %d unreadable index entries (%d data bytes) during compaction",
			v.Id, skippedNeedles, skippedDataBytes)
	}
	if v.Ttl.String() == "" && v.nm != nil {
		dstDatSize, _, err := dstDatBackend.GetStat()
		if err != nil {
			return err
		}
		if v.nm.ContentSize() > v.nm.DeletedSize() {
			expectedContentSize := v.nm.ContentSize() - v.nm.DeletedSize()
			// Skipped needles still contribute to the source-side ContentSize but
			// were not written to the destination, so subtract them before the
			// safety check to avoid a false positive.
			if skippedDataBytes >= expectedContentSize {
				expectedContentSize = 0
			} else {
				expectedContentSize -= skippedDataBytes
			}
			if expectedContentSize > uint64(dstDatSize) {
				return fmt.Errorf("volume %s unexpected new data size: %d does not match size of content minus deleted: %d",
					v.Id.String(), dstDatSize, expectedContentSize)
			}
		} else if v.nm.DeletedSize() > v.nm.ContentSize() {
			glog.Warningf("volume %s content size: %d less deleted size: %d, new size: %d",
				v.Id.String(), v.nm.ContentSize(), v.nm.DeletedSize(), dstDatSize)
		}
	}
	err = newNm.SaveToIdx(opts.destIdxPath)
	if err != nil {
		return err
	}

	indexFile, err := os.OpenFile(opts.destIdxPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Errorf("cannot open Volume Index %s: %v", opts.destIdxPath, err)
		return err
	}
	defer func() {
		if syncErr := indexFile.Sync(); syncErr != nil && err == nil {
			err = fmt.Errorf("sync compacted idx %s: %v", opts.destIdxPath, syncErr)
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
