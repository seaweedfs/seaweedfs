package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

var ErrorNotFound = errors.New("not found")

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
func (v *Volume) isFileUnchanged(n *needle.Needle) bool {
	if v.Ttl.String() != "" {
		return false
	}

	nv, ok := v.nm.Get(n.Id)
	if ok && !nv.Offset.IsZero() && nv.Size != TombstoneFileSize {
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToAcutalOffset(), nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file at offset %d size %d: %v", nv.Offset.ToAcutalOffset(), nv.Size, err)
			return false
		}
		if oldNeedle.Cookie == n.Cookie && oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.isCompacting {
		err = fmt.Errorf("volume %d is compacting", v.Id)
		return
	}
	v.Close()
	os.Remove(v.FileName() + ".dat")
	os.Remove(v.FileName() + ".idx")
	os.Remove(v.FileName() + ".tier")
	os.Remove(v.FileName() + ".sdx")
	os.Remove(v.FileName() + ".cpd")
	os.Remove(v.FileName() + ".cpx")
	os.RemoveAll(v.FileName() + ".ldb")
	return
}

func (v *Volume) writeNeedle(n *needle.Needle) (offset uint64, size uint32, isUnchanged bool, err error) {
	glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.isFileUnchanged(n) {
		size = n.DataSize
		isUnchanged = true
		return
	}

	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}

	// check whether existing needle cookie matches
	nv, ok := v.nm.Get(n.Id)
	if ok {
		existingNeedle, _, _, existingNeedleReadErr := needle.ReadNeedleHeader(v.DataBackend, v.Version(), nv.Offset.ToAcutalOffset())
		if existingNeedleReadErr != nil {
			err = fmt.Errorf("reading existing needle: %v", existingNeedleReadErr)
			return
		}
		if existingNeedle.Cookie != n.Cookie {
			glog.V(0).Infof("write cookie mismatch: existing %x, new %x", existingNeedle.Cookie, n.Cookie)
			err = fmt.Errorf("mismatching cookie %x", n.Cookie)
			return
		}
	}

	// append to dat file
	n.AppendAtNs = uint64(time.Now().UnixNano())
	if offset, size, _, err = n.Append(v.DataBackend, v.Version()); err != nil {
		return
	}
	v.lastAppendAtNs = n.AppendAtNs

	// add to needle map
	if !ok || uint64(nv.Offset.ToAcutalOffset()) < offset {
		if err = v.nm.Put(n.Id, ToOffset(int64(offset)), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}
	if v.lastModifiedTsSeconds < n.LastModified {
		v.lastModifiedTsSeconds = n.LastModified
	}
	return
}

func (v *Volume) deleteNeedle(n *needle.Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	nv, ok := v.nm.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && nv.Size != TombstoneFileSize {
		size := nv.Size
		n.Data = nil
		n.AppendAtNs = uint64(time.Now().UnixNano())
		offset, _, _, err := n.Append(v.DataBackend, v.Version())
		if err != nil {
			return size, err
		}
		v.lastAppendAtNs = n.AppendAtNs
		if err = v.nm.Delete(n.Id, ToOffset(int64(offset))); err != nil {
			return size, err
		}
		return size, err
	}
	return 0, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *needle.Needle) (int, error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return -1, ErrorNotFound
	}
	if nv.Size == TombstoneFileSize {
		return -1, errors.New("already deleted")
	}
	if nv.Size == 0 {
		return 0, nil
	}
	err := n.ReadData(v.DataBackend, nv.Offset.ToAcutalOffset(), nv.Size, v.Version())
	if err != nil {
		return 0, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	return -1, ErrorNotFound
}

type VolumeFileScanner interface {
	VisitSuperBlock(super_block.SuperBlock) error
	ReadNeedleBody() bool
	VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error
}

func ScanVolumeFile(dirname string, collection string, id needle.VolumeId,
	needleMapKind NeedleMapType,
	volumeFileScanner VolumeFileScanner) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("failed to load volume %d: %v", id, err)
	}
	if err = volumeFileScanner.VisitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("failed to process volume %d super block: %v", id, err)
	}
	defer v.Close()

	version := v.Version()

	offset := int64(v.SuperBlock.BlockSize())

	return ScanVolumeFileFrom(version, v.DataBackend, offset, volumeFileScanner)
}

func ScanVolumeFileFrom(version needle.Version, datBackend backend.BackendStorageFile, offset int64, volumeFileScanner VolumeFileScanner) (err error) {
	n, nh, rest, e := needle.ReadNeedleHeader(datBackend, version, offset)
	if e != nil {
		if e == io.EOF {
			return nil
		}
		return fmt.Errorf("cannot read %s at offset %d: %v", datBackend.Name(), offset, e)
	}
	for n != nil {
		var needleBody []byte
		if volumeFileScanner.ReadNeedleBody() {
			if needleBody, err = n.ReadNeedleBody(datBackend, version, offset+NeedleHeaderSize, rest); err != nil {
				glog.V(0).Infof("cannot read needle body: %v", err)
				//err = fmt.Errorf("cannot read needle body: %v", err)
				//return
			}
		}
		err := volumeFileScanner.VisitNeedle(n, offset, nh, needleBody)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
			return fmt.Errorf("visit needle error: %v", err)
		}
		offset += NeedleHeaderSize + rest
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, nh, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header at offset %d: %v", offset, err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}
	return nil
}
