package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (v *Volume) garbageLevel() float64 {
	if v.ContentSize() == 0 {
		return 0
	}
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact(preallocate int64) error {
	glog.V(3).Infof("Compacting volume %d ...", v.Id)
	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")

	filePath := v.FileName()
	v.lastCompactIndexOffset = v.nm.IndexFileSize()
	v.lastCompactRevision = v.SuperBlock.CompactRevision
	glog.V(3).Infof("creating copies for volume %d ,last offset %d...", v.Id, v.lastCompactIndexOffset)
	return v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx", preallocate)
}

func (v *Volume) Compact2() error {
	glog.V(3).Infof("Compact2 volume %d ...", v.Id)
	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	return v.copyDataBasedOnIndexFile(filePath+".cpd", filePath+".cpx")
}

func (v *Volume) commitCompact() error {
	glog.V(0).Infof("Committing volume %d vacuuming...", v.Id)
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	glog.V(3).Infof("Got volume %d committing lock...", v.Id)
	v.compactingWg.Add(1)
	defer v.compactingWg.Done()
	v.nm.Close()
	if err := v.dataFile.Close(); err != nil {
		glog.V(0).Infof("fail to close volume %d", v.Id)
	}
	v.dataFile = nil

	var e error
	if e = v.makeupDiff(v.FileName()+".cpd", v.FileName()+".cpx", v.FileName()+".dat", v.FileName()+".idx"); e != nil {
		glog.V(0).Infof("makeupDiff in commitCompact volume %d failed %v", v.Id, e)
		e = os.Remove(v.FileName() + ".cpd")
		if e != nil {
			return e
		}
		e = os.Remove(v.FileName() + ".cpx")
		if e != nil {
			return e
		}
	} else {
		var e error
		if e = os.Rename(v.FileName()+".cpd", v.FileName()+".dat"); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName()+".cpd", e)
		}
		if e = os.Rename(v.FileName()+".cpx", v.FileName()+".idx"); e != nil {
			return fmt.Errorf("rename %s: %v", v.FileName()+".cpx", e)
		}
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)

	os.RemoveAll(v.FileName() + ".ldb")
	os.RemoveAll(v.FileName() + ".bdb")

	glog.V(3).Infof("Loading volume %d commit file...", v.Id)
	if e = v.load(true, false, v.needleMapKind, 0); e != nil {
		return e
	}
	return nil
}

func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up volume %d vacuuming...", v.Id)

	e1 := os.Remove(v.FileName() + ".cpd")
	e2 := os.Remove(v.FileName() + ".cpx")
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}

func fetchCompactRevisionFromDatFile(file *os.File) (compactRevision uint16, err error) {
	superBlock, err := ReadSuperBlock(file)
	if err != nil {
		return 0, err
	}
	return superBlock.CompactRevision, nil
}

func (v *Volume) makeupDiff(newDatFileName, newIdxFileName, oldDatFileName, oldIdxFileName string) (err error) {
	var indexSize int64

	oldIdxFile, err := os.Open(oldIdxFileName)
	defer oldIdxFile.Close()

	oldDatFile, err := os.Open(oldDatFileName)
	defer oldDatFile.Close()

	if indexSize, err = verifyIndexFileIntegrity(oldIdxFile); err != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", oldIdxFileName, err)
	}
	if indexSize == 0 || uint64(indexSize) <= v.lastCompactIndexOffset {
		return nil
	}

	oldDatCompactRevision, err := fetchCompactRevisionFromDatFile(oldDatFile)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile src %s failed: %v", oldDatFile.Name(), err)
	}
	if oldDatCompactRevision != v.lastCompactRevision {
		return fmt.Errorf("current old dat file's compact revision %d is not the expected one %d", oldDatCompactRevision, v.lastCompactRevision)
	}

	type keyField struct {
		offset Offset
		size   uint32
	}
	incrementedHasUpdatedIndexEntry := make(map[NeedleId]keyField)

	for idxOffset := indexSize - NeedleEntrySize; uint64(idxOffset) >= v.lastCompactIndexOffset; idxOffset -= NeedleEntrySize {
		var IdxEntry []byte
		if IdxEntry, err = readIndexEntryAtOffset(oldIdxFile, idxOffset); err != nil {
			return fmt.Errorf("readIndexEntry %s at offset %d failed: %v", oldIdxFileName, idxOffset, err)
		}
		key, offset, size := IdxFileEntry(IdxEntry)
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
	defer dst.Close()

	if idx, err = os.OpenFile(newIdxFileName, os.O_RDWR, 0644); err != nil {
		return fmt.Errorf("open idx file %s failed: %v", newIdxFileName, err)
	}
	defer idx.Close()

	var newDatCompactRevision uint16
	newDatCompactRevision, err = fetchCompactRevisionFromDatFile(dst)
	if err != nil {
		return fmt.Errorf("fetchCompactRevisionFromDatFile dst %s failed: %v", dst.Name(), err)
	}
	if oldDatCompactRevision+1 != newDatCompactRevision {
		return fmt.Errorf("oldDatFile %s 's compact revision is %d while newDatFile %s 's compact revision is %d", oldDatFileName, oldDatCompactRevision, newDatFileName, newDatCompactRevision)
	}

	idxEntryBytes := make([]byte, NeedleIdSize+OffsetSize+SizeSize)
	for key, increIdxEntry := range incrementedHasUpdatedIndexEntry {
		NeedleIdToBytes(idxEntryBytes[0:NeedleIdSize], key)
		OffsetToBytes(idxEntryBytes[NeedleIdSize:NeedleIdSize+OffsetSize], increIdxEntry.offset)
		util.Uint32toBytes(idxEntryBytes[NeedleIdSize+OffsetSize:NeedleIdSize+OffsetSize+SizeSize], increIdxEntry.size)

		var offset int64
		if offset, err = dst.Seek(0, 2); err != nil {
			glog.V(0).Infof("failed to seek the end of file: %v", err)
			return
		}
		//ensure file writing starting from aligned positions
		if offset%NeedlePaddingSize != 0 {
			offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
			if offset, err = v.dataFile.Seek(offset, 0); err != nil {
				glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
				return
			}
		}

		//updated needle
		if increIdxEntry.offset != 0 && increIdxEntry.size != 0 && increIdxEntry.size != TombstoneFileSize {
			//even the needle cache in memory is hit, the need_bytes is correct
			glog.V(4).Infof("file %d offset %d size %d", key, int64(increIdxEntry.offset)*NeedlePaddingSize, increIdxEntry.size)
			var needleBytes []byte
			needleBytes, err = ReadNeedleBlob(oldDatFile, int64(increIdxEntry.offset)*NeedlePaddingSize, increIdxEntry.size, v.Version())
			if err != nil {
				return fmt.Errorf("ReadNeedleBlob %s key %d offset %d size %d failed: %v", oldDatFile.Name(), key, int64(increIdxEntry.offset)*NeedlePaddingSize, increIdxEntry.size, err)
			}
			dst.Write(needleBytes)
			util.Uint32toBytes(idxEntryBytes[8:12], uint32(offset/NeedlePaddingSize))
		} else { //deleted needle
			//fakeDelNeedle 's default Data field is nil
			fakeDelNeedle := new(Needle)
			fakeDelNeedle.Id = key
			fakeDelNeedle.Cookie = 0x12345678
			fakeDelNeedle.AppendAtNs = uint64(time.Now().UnixNano())
			_, _, _, err = fakeDelNeedle.Append(dst, v.Version())
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
	}

	return nil
}

type VolumeFileScanner4Vacuum struct {
	version   Version
	v         *Volume
	dst       *os.File
	nm        *NeedleMap
	newOffset int64
	now       uint64
}

func (scanner *VolumeFileScanner4Vacuum) VisitSuperBlock(superBlock SuperBlock) error {
	scanner.version = superBlock.Version()
	superBlock.CompactRevision++
	_, err := scanner.dst.Write(superBlock.Bytes())
	scanner.newOffset = int64(superBlock.BlockSize())
	return err

}
func (scanner *VolumeFileScanner4Vacuum) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Vacuum) VisitNeedle(n *Needle, offset int64) error {
	if n.HasTtl() && scanner.now >= n.LastModified+uint64(scanner.v.Ttl.Minutes()*60) {
		return nil
	}
	nv, ok := scanner.v.nm.Get(n.Id)
	glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
	if ok && int64(nv.Offset)*NeedlePaddingSize == offset && nv.Size > 0 {
		if err := scanner.nm.Put(n.Id, Offset(scanner.newOffset/NeedlePaddingSize), n.Size); err != nil {
			return fmt.Errorf("cannot put needle: %s", err)
		}
		if _, _, _, err := n.Append(scanner.dst, scanner.v.Version()); err != nil {
			return fmt.Errorf("cannot append needle: %s", err)
		}
		scanner.newOffset += n.DiskSize(scanner.version)
		glog.V(4).Infoln("saving key", n.Id, "volume offset", offset, "=>", scanner.newOffset, "data_size", n.Size)
	}
	return nil
}

func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName string, preallocate int64) (err error) {
	var (
		dst, idx *os.File
	)
	if dst, err = createVolumeFile(dstName, preallocate); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer idx.Close()

	scanner := &VolumeFileScanner4Vacuum{
		v:   v,
		now: uint64(time.Now().Unix()),
		nm:  NewBtreeNeedleMap(idx),
		dst: dst,
	}
	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind, scanner)
	return
}

func (v *Volume) copyDataBasedOnIndexFile(dstName, idxName string) (err error) {
	var (
		dst, idx, oldIndexFile *os.File
	)
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer idx.Close()

	if oldIndexFile, err = os.OpenFile(v.FileName()+".idx", os.O_RDONLY, 0644); err != nil {
		return
	}
	defer oldIndexFile.Close()

	nm := NewBtreeNeedleMap(idx)
	now := uint64(time.Now().Unix())

	v.SuperBlock.CompactRevision++
	dst.Write(v.SuperBlock.Bytes())
	newOffset := int64(v.SuperBlock.BlockSize())

	WalkIndexFile(oldIndexFile, func(key NeedleId, offset Offset, size uint32) error {
		if offset == 0 || size == TombstoneFileSize {
			return nil
		}

		nv, ok := v.nm.Get(key)
		if !ok {
			return nil
		}

		n := new(Needle)
		n.ReadData(v.dataFile, int64(offset)*NeedlePaddingSize, size, v.Version())

		if n.HasTtl() && now >= n.LastModified+uint64(v.Ttl.Minutes()*60) {
			return nil
		}

		glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
		if nv.Offset == offset && nv.Size > 0 {
			if err = nm.Put(n.Id, Offset(newOffset/NeedlePaddingSize), n.Size); err != nil {
				return fmt.Errorf("cannot put needle: %s", err)
			}
			if _, _, _, err = n.Append(dst, v.Version()); err != nil {
				return fmt.Errorf("cannot append needle: %s", err)
			}
			newOffset += n.DiskSize(v.Version())
			glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", newOffset, "data_size", n.Size)
		}
		return nil
	})

	return
}
