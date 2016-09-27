package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (v *Volume) garbageLevel() float64 {
	return float64(v.nm.DeletedSize()) / float64(v.ContentSize())
}

func (v *Volume) Compact() error {
	glog.V(3).Infof("Compacting ...")
	//no need to lock for copy on write
	//v.accessLock.Lock()
	//defer v.accessLock.Unlock()
	//glog.V(3).Infof("Got Compaction lock...")

	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	return v.copyDataAndGenerateIndexFile(filePath+".cpd", filePath+".cpx")
}

func (v *Volume) Compact2() error {
	glog.V(3).Infof("Compact2 ...")
	filePath := v.FileName()
	glog.V(3).Infof("creating copies for volume %d ...", v.Id)
	return v.copyDataBasedOnIndexFile(filePath+".cpd", filePath+".cpx")
}

func (v *Volume) commitCompact() error {
	glog.V(3).Infof("Committing vacuuming...")
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	glog.V(3).Infof("Got Committing lock...")
	v.nm.Close()
	_ = v.dataFile.Close()
	makeupDiff(v.FileName()+".cpd", v.FileName()+".cpx", v.FileName()+".dat", v.FileName()+".idx")
	var e error
	if e = os.Rename(v.FileName()+".cpd", v.FileName()+".dat"); e != nil {
		return e
	}
	if e = os.Rename(v.FileName()+".cpx", v.FileName()+".idx"); e != nil {
		return e
	}
	//glog.V(3).Infof("Pretending to be vacuuming...")
	//time.Sleep(20 * time.Second)
	glog.V(3).Infof("Loading Commit file...")
	if e = v.load(true, false, v.needleMapKind); e != nil {
		return e
	}
	return nil
}

func makeupDiff(newDatFile, newIdxFile, oldDatFile, oldIdxFile string) (err error) {
	return nil
}

func (v *Volume) copyDataAndGenerateIndexFile(dstName, idxName string) (err error) {
	var (
		dst, idx *os.File
	)
	if dst, err = os.OpenFile(dstName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer dst.Close()

	if idx, err = os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return
	}
	defer idx.Close()

	nm := NewNeedleMap(idx)
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
				if _, err = n.Append(dst, v.Version()); err != nil {
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

	nm := NewNeedleMap(idx)
	now := uint64(time.Now().Unix())

	v.SuperBlock.CompactRevision++
	dst.Write(v.SuperBlock.Bytes())
	new_offset := int64(SuperBlockSize)

	WalkIndexFile(oldIndexFile, func(key uint64, offset, size uint32) error {
		if size <= 0 {
			return nil
		}

		nv, ok := v.nm.Get(key)
		if !ok {
			return nil
		}

		n := new(Needle)
		n.ReadData(v.dataFile, int64(offset)*NeedlePaddingSize, size, v.Version())
		defer n.ReleaseMemory()

		if n.HasTtl() && now >= n.LastModified+uint64(v.Ttl.Minutes()*60) {
			return nil
		}

		glog.V(4).Infoln("needle expected offset ", offset, "ok", ok, "nv", nv)
		if nv.Offset == offset && nv.Size > 0 {
			if err = nm.Put(n.Id, uint32(new_offset/NeedlePaddingSize), n.Size); err != nil {
				return fmt.Errorf("cannot put needle: %s", err)
			}
			if _, err = n.Append(dst, v.Version()); err != nil {
				return fmt.Errorf("cannot append needle: %s", err)
			}
			new_offset += n.DiskSize()
			glog.V(3).Infoln("saving key", n.Id, "volume offset", offset, "=>", new_offset, "data_size", n.Size)
		}
		return nil
	})

	return
}
