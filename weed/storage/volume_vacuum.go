package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (v *Volume) garbageLevel() float64 {
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact(preallocate int64) error {
	glog.V(3).Infof("Compacting ...")
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
	glog.V(3).Infof("Compact2 ...")
	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	return v.copyDataBasedOnIndexFile(filePath+".cpd", filePath+".cpx")
}

func (v *Volume) commitCompact() error {
	glog.V(0).Infof("Committing vacuuming...")
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	glog.V(3).Infof("Got Committing lock...")
	v.nm.Close()
	_ = v.dataFile.Close()

	var e error
	if e = v.makeupDiff(v.FileName()+".cpd", v.FileName()+".cpx", v.FileName()+".dat", v.FileName()+".idx"); e != nil {
		glog.V(0).Infof("makeupDiff in commitCompact failed %v", e)
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
			return e
		}
		if e = os.Rename(v.FileName()+".cpx", v.FileName()+".idx"); e != nil {
			return e
		}
	}

	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)
	glog.V(3).Infof("Loading Commit file...")
	if e = v.load(true, false, v.needleMapKind, 0); e != nil {
		return e
	}
	return nil
}

func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up vacuuming...")

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
	if _, err = file.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("cannot seek to the beginning of %s: %v", file.Name(), err)
	}
	header := make([]byte, SuperBlockSize)
	if _, e := file.Read(header); e != nil {
		return 0, fmt.Errorf("cannot read file %s 's super block: %v", file.Name(), e)
	}
	superBlock, err := ParseSuperBlock(header)
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
		return
	}
	if oldDatCompactRevision != v.lastCompactRevision {
		return fmt.Errorf("current old dat file's compact revision %d is not the expected one %d", oldDatCompactRevision, v.lastCompactRevision)
	}

	type keyField struct {
		offset uint32
		size   uint32
	}
	incrementedHasUpdatedIndexEntry := make(map[uint64]keyField)

	for idx_offset := indexSize - NeedleIndexSize; uint64(idx_offset) >= v.lastCompactIndexOffset; idx_offset -= NeedleIndexSize {
		var IdxEntry []byte
		if IdxEntry, err = readIndexEntryAtOffset(oldIdxFile, idx_offset); err != nil {
			return fmt.Errorf("readIndexEntry %s at offset %d failed: %v", oldIdxFileName, idx_offset, err)
		}
		key, offset, size := idxFileEntry(IdxEntry)
		if _, found := incrementedHasUpdatedIndexEntry[key]; !found {
			incrementedHasUpdatedIndexEntry[key] = keyField{
				offset: offset,
				size:   size,
			}
		}
	}

	if len(incrementedHasUpdatedIndexEntry) > 0 {
		var (
			dst, idx *os.File
		)
		if dst, err = os.OpenFile(newDatFileName, os.O_RDWR, 0644); err != nil {
			return
		}
		defer dst.Close()

		if idx, err = os.OpenFile(newIdxFileName, os.O_RDWR, 0644); err != nil {
			return
		}
		defer idx.Close()

		var newDatCompactRevision uint16
		newDatCompactRevision, err = fetchCompactRevisionFromDatFile(dst)
		if err != nil {
			return
		}
		if oldDatCompactRevision+1 != newDatCompactRevision {
			return fmt.Errorf("oldDatFile %s 's compact revision is %d while newDatFile %s 's compact revision is %d", oldDatFileName, oldDatCompactRevision, newDatFileName, newDatCompactRevision)
		}

		idx_entry_bytes := make([]byte, 16)
		for key, incre_idx_entry := range incrementedHasUpdatedIndexEntry {
			util.Uint64toBytes(idx_entry_bytes[0:8], key)
			util.Uint32toBytes(idx_entry_bytes[8:12], incre_idx_entry.offset)
			util.Uint32toBytes(idx_entry_bytes[12:16], incre_idx_entry.size)

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
			if incre_idx_entry.offset != 0 && incre_idx_entry.size != 0 {
				//even the needle cache in memory is hit, the need_bytes is correct
				var needle_bytes []byte
				needle_bytes, err = ReadNeedleBlob(oldDatFile, int64(incre_idx_entry.offset)*NeedlePaddingSize, incre_idx_entry.size)
				if err != nil {
					return
				}
				dst.Write(needle_bytes)
				util.Uint32toBytes(idx_entry_bytes[8:12], uint32(offset/NeedlePaddingSize))
			} else { //deleted needle
				//fakeDelNeedle 's default Data field is nil
				fakeDelNeedle := new(Needle)
				fakeDelNeedle.Id = key
				fakeDelNeedle.Cookie = 0x12345678
				_, _, err = fakeDelNeedle.Append(dst, v.Version())
				if err != nil {
					return
				}
				util.Uint32toBytes(idx_entry_bytes[8:12], uint32(0))
			}

			if _, err := idx.Seek(0, 2); err != nil {
				return fmt.Errorf("cannot seek end of indexfile %s: %v",
					newIdxFileName, err)
			}
			_, err = idx.Write(idx_entry_bytes)
		}
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

	nm := NewBtreeNeedleMap(idx)
	new_offset := int64(SuperBlockSize)

	now := uint64(time.Now().Unix())

	err = ScanVolumeFile(v.dir, v.Collection, v.Id, v.needleMapKind,
		func(superBlock SuperBlock) error {
			superBlock.CompactRevision++
			_, err = dst.Write(superBlock.Bytes())
			return err
		}, true, func(n *Needle, offset int64) error {
			if n.HasTtl() && now >= n.LastModified+uint64(v.Ttl.Minutes()*60) {
				return nil
			}
			nv, ok := v.nm.Get(n.Id)
			glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
			if ok && int64(nv.Offset)*NeedlePaddingSize == offset && nv.Size > 0 {
				if err = nm.Put(n.Id, uint32(new_offset/NeedlePaddingSize), n.Size); err != nil {
					return fmt.Errorf("cannot put needle: %s", err)
				}
				if _, _, err := n.Append(dst, v.Version()); err != nil {
					return fmt.Errorf("cannot append needle: %s", err)
				}
				new_offset += n.DiskSize()
				glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", new_offset, "data_size", n.Size)
			}
			return nil
		})

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
	new_offset := int64(SuperBlockSize)

	WalkIndexFile(oldIndexFile, func(key uint64, offset, size uint32) error {
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
			if err = nm.Put(n.Id, uint32(new_offset/NeedlePaddingSize), n.Size); err != nil {
				return fmt.Errorf("cannot put needle: %s", err)
			}
			if _, _, err = n.Append(dst, v.Version()); err != nil {
				return fmt.Errorf("cannot append needle: %s", err)
			}
			new_offset += n.DiskSize()
			glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", new_offset, "data_size", n.Size)
		}
		return nil
	})

	return
}
