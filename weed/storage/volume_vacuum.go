package storage

import (
	"fmt"
	"os"
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

// compact a volume based on deletions in .dat files
func (v *Volume) Compact(preallocate int64, compactionBytePerSecond int64) error {

	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(3).Infof("Compacting volume %d ...", v.Id)
	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")
	if v.isCompacting || v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already compacting...", v.Id)
		return nil
	}
	v.isCompacting = true
	defer func() {
		v.isCompacting = false
	}()

	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision
	glog.V(3).Infof("creating copies for volume %d ,last offset %d...", v.Id, v.lastCompactIndexOffset)
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume %d", v.Id)
	}
	if err := v.nm.Sync(); err != nil {
		glog.V(0).Infof("compact failed to sync volume idx %d", v.Id)
	}
	return v.copyDataAndGenerateIndexFile(v.FileName(".cpd"), v.FileName(".cpx"), preallocate, compactionBytePerSecond)
}

// compact a volume based on deletions in .idx files
func (v *Volume) Compact2(preallocate int64, compactionBytePerSecond int64, progressFn ProgressFunc) error {

	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(3).Infof("Compact2 volume %d ...", v.Id)

	if v.isCompacting || v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already compacting2 ...", v.Id)
		return nil
	}
	v.isCompacting = true
	defer func() {
		v.isCompacting = false
	}()

	v.lastCompactIndexOffset = v.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactionRevision
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	if v.DataBackend == nil {
		return fmt.Errorf("volume %d backend is empty remote:%v", v.Id, v.HasRemoteFile())
	}
	if err := v.DataBackend.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume dat %d: %v", v.Id, err)
	}
	if err := v.nm.Sync(); err != nil {
		glog.V(0).Infof("compact2 failed to sync volume idx %d: %v", v.Id, err)
	}
	return v.copyDataBasedOnIndexFile(
		v.FileName(".dat"), v.FileName(".idx"),
		v.FileName(".cpd"), v.FileName(".cpx"),
		v.SuperBlock,
		v.Version(),
		preallocate,
		compactionBytePerSecond,
		progressFn,
	)
}

func (v *Volume) CommitCompact() error {
	if v.MemoryMapMaxSizeMb != 0 { //it makes no sense to compact in memory
		return nil
	}
	glog.V(0).Infof("Committing volume %d vacuuming...", v.Id)

	if v.isCommitCompacting {
		glog.V(0).Infof("Volume %d is already commit compacting ...", v.Id)
		return nil
	}
	v.isCommitCompacting = true
	defer func() {
		v.isCommitCompacting = false
	}()

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
		if runtime.GOOS == "windows" {
			e = os.RemoveAll(v.FileName(".dat"))
			if e != nil {
				return e
			}
			e = os.RemoveAll(v.FileName(".idx"))
			if e != nil {
				return e
			}
		}
		var e error
		if e = os.Rename(v.FileName(".cpd"), v.FileName(".dat")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpd"), e)
		}
		if e = os.Rename(v.FileName(".cpx"), v.FileName(".idx")); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName(".cpx"), e)
		}
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)

	os.RemoveAll(v.FileName(".ldb"))

	glog.V(3).Infof("Loading volume %d commit file...", v.Id)
	if e = v.load(true, false, v.needleMapKind, 0); e != nil {
		return e
	}
	glog.V(3).Infof("Finish committing volume %d", v.Id)
	return nil
}

func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up volume %d vacuuming...", v.Id)

	e1 := os.Remove(v.FileName(".cpd"))
	e2 := os.Remove(v.FileName(".cpx"))
	e3 := os.RemoveAll(v.FileName(".cpldb"))
	if e1 != nil && !os.IsNotExist(e1) {
		return e1
	}
	if e2 != nil && !os.IsNotExist(e2) {
		return e2
	}
	if e3 != nil && !os.IsNotExist(e3) {
		return e3
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
		idx.Sync()
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
				return fmt.Errorf("ReadNeedleBlob %s key %d offset %d size %d failed: %v", oldDatFile.Name(), key, increIdxEntry.offset.ToActualOffset(), increIdxEntry.size, err)
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
			_, _, _, err = fakeDelNeedle.Append(dstDatBackend, v.Version())
			if err != nil {
				return fmt.Errorf("append deleted %d failed: %v", key, err)
			}
			util.Uint32toBytes(idxEntryBytes[8:12], uint32(0))
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

func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName string, preallocate int64, compactionBytePerSecond int64) (err error) {
	var dst backend.BackendStorageFile
	if dst, err = backend.CreateVolumeFile(dstName, preallocate, 0); err != nil {
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
		writeThrottler: util.NewWriteThrottler(compactionBytePerSecond),
	}
	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind, scanner)
	if err != nil {
		return err
	}

	return nm.SaveToIdx(idxName)
}

func (v *Volume) copyDataBasedOnIndexFile(srcDatName, srcIdxName, dstDatName, datIdxName string, sb super_block.SuperBlock, version needle.Version, preallocate, compactionBytePerSecond int64, progressFn ProgressFunc) (err error) {
	var (
		srcDatBackend, dstDatBackend backend.BackendStorageFile
		dataFile                     *os.File
	)
	if dstDatBackend, err = backend.CreateVolumeFile(dstDatName, preallocate, 0); err != nil {
		return err
	}
	defer func() {
		dstDatBackend.Sync()
		dstDatBackend.Close()
	}()

	oldNm := needle_map.NewMemDb()
	defer oldNm.Close()
	newNm := needle_map.NewMemDb()
	defer newNm.Close()
	if err = oldNm.LoadFromIdx(srcIdxName); err != nil {
		return err
	}
	if dataFile, err = os.Open(srcDatName); err != nil {
		return err
	}
	srcDatBackend = backend.NewDiskFile(dataFile)
	defer srcDatBackend.Close()

	now := uint64(time.Now().Unix())

	sb.CompactionRevision++
	dstDatBackend.WriteAt(sb.Bytes(), 0)
	newOffset := int64(sb.BlockSize())

	writeThrottler := util.NewWriteThrottler(compactionBytePerSecond)
	err = oldNm.AscendingVisit(func(value needle_map.NeedleValue) error {

		offset, size := value.Offset, value.Size

		if offset.IsZero() || size.IsDeleted() {
			return nil
		}

		if progressFn != nil {
			if !progressFn(offset.ToActualOffset()) {
				return fmt.Errorf("interrupted")
			}
		}

		n := new(needle.Needle)
		if err := n.ReadData(srcDatBackend, offset.ToActualOffset(), size, version); err != nil {
			return fmt.Errorf("cannot hydrate needle from file: %s", err)
		}

		if n.HasTtl() && now >= n.LastModified+uint64(sb.Ttl.Minutes()*60) {
			return nil
		}

		if err = newNm.Set(n.Id, ToOffset(newOffset), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}
		if _, _, _, err = n.Append(dstDatBackend, sb.Version); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}
		delta := n.DiskSize(version)
		newOffset += delta
		writeThrottler.MaybeSlowdown(delta)
		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", newOffset, "data_size", n.Size)

		return nil
	})
	if err != nil {
		return err
	}
	if v.Ttl.String() == "" {
		dstDatSize, _, err := dstDatBackend.GetStat()
		if err != nil {
			return err
		}
		if v.nm.ContentSize() > v.nm.DeletedSize() {
			expectedContentSize := v.nm.ContentSize() - v.nm.DeletedSize()
			if expectedContentSize > uint64(dstDatSize) {
				return fmt.Errorf("volume %s unexpected new data size: %d does not match size of content minus deleted: %d",
					v.Id.String(), dstDatSize, expectedContentSize)
			}
		} else {
			glog.Warningf("volume %s content size: %d less deleted size: %d, new size: %d",
				v.Id.String(), v.nm.ContentSize(), v.nm.DeletedSize(), dstDatSize)
		}
	}
	err = newNm.SaveToIdx(datIdxName)
	if err != nil {
		return err
	}

	indexFile, err := os.OpenFile(datIdxName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		glog.Errorf("cannot open Volume Index %s: %v", datIdxName, err)
		return err
	}
	defer func() {
		indexFile.Sync()
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
