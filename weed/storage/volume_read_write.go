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
var ErrorDeleted = errors.New("already deleted")
var ErrorSizeMismatch = errors.New("size mismatch")

func (v *Volume) checkReadWriteError(err error) {
	if err == nil {
		if v.lastIoError != nil {
			v.lastIoError = nil
		}
		return
	}
	if err.Error() == "input/output error" {
		v.lastIoError = err
	}
}

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
func (v *Volume) isFileUnchanged(n *needle.Needle) bool {
	if v.Ttl.String() != "" {
		return false
	}

	nv, ok := v.nm.Get(n.Id)
	if ok && !nv.Offset.IsZero() && nv.Size.IsValid() {
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
	close(v.asyncRequestsChan)
	storageName, storageKey := v.RemoteStorageNameKey()
	if v.HasRemoteFile() && storageName != "" && storageKey != "" {
		if backendStorage, found := backend.BackendStorages[storageName]; found {
			backendStorage.DeleteFile(storageKey)
		}
	}
	v.Close()
	removeVolumeFiles(v.DataFileName())
	removeVolumeFiles(v.IndexFileName())
	return
}

func removeVolumeFiles(filename string) {
	// basic
	os.Remove(filename + ".dat")
	os.Remove(filename + ".idx")
	os.Remove(filename + ".vif")
	// sorted index file
	os.Remove(filename + ".sdx")
	// compaction
	os.Remove(filename + ".cpd")
	os.Remove(filename + ".cpx")
	// level db indx file
	os.RemoveAll(filename + ".ldb")
	// marker for damaged or incomplete volume
	os.Remove(filename + ".note")
}

func (v *Volume) asyncRequestAppend(request *needle.AsyncRequest) {
	v.asyncRequestsChan <- request
}

func (v *Volume) syncWrite(n *needle.Needle) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	actualSize := needle.GetActualSize(Size(len(n.Data)), v.Version())

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if MaxPossibleVolumeSize < v.nm.ContentSize()+uint64(actualSize) {
		err = fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.nm.ContentSize())
		return
	}
	if v.isFileUnchanged(n) {
		size = Size(n.DataSize)
		isUnchanged = true
		return
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
	offset, size, _, err = n.Append(v.DataBackend, v.Version())
	v.checkReadWriteError(err)
	if err != nil {
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

func (v *Volume) writeNeedle2(n *needle.Needle, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}

	if !fsync {
		return v.syncWrite(n)
	} else {
		asyncRequest := needle.NewAsyncRequest(n, true)
		// using len(n.Data) here instead of n.Size before n.Size is populated in n.Append()
		asyncRequest.ActualSize = needle.GetActualSize(Size(len(n.Data)), v.Version())

		v.asyncRequestAppend(asyncRequest)
		offset, _, isUnchanged, err = asyncRequest.WaitComplete()

		return
	}
}

func (v *Volume) doWriteRequest(n *needle.Needle) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if v.isFileUnchanged(n) {
		size = Size(n.DataSize)
		isUnchanged = true
		return
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
	offset, size, _, err = n.Append(v.DataBackend, v.Version())
	v.checkReadWriteError(err)
	if err != nil {
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

func (v *Volume) syncDelete(n *needle.Needle) (Size, error) {
	// glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	actualSize := needle.GetActualSize(0, v.Version())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if MaxPossibleVolumeSize < v.nm.ContentSize()+uint64(actualSize) {
		err := fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.nm.ContentSize())
		return 0, err
	}

	nv, ok := v.nm.Get(n.Id)
	// fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && nv.Size.IsValid() {
		size := nv.Size
		n.Data = nil
		n.AppendAtNs = uint64(time.Now().UnixNano())
		offset, _, _, err := n.Append(v.DataBackend, v.Version())
		v.checkReadWriteError(err)
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

func (v *Volume) deleteNeedle2(n *needle.Needle) (Size, error) {
	// todo: delete info is always appended no fsync, it may need fsync in future
	fsync := false

	if !fsync {
		return v.syncDelete(n)
	} else {
		asyncRequest := needle.NewAsyncRequest(n, false)
		asyncRequest.ActualSize = needle.GetActualSize(0, v.Version())

		v.asyncRequestAppend(asyncRequest)
		_, size, _, err := asyncRequest.WaitComplete()

		return Size(size), err
	}
}

func (v *Volume) doDeleteRequest(n *needle.Needle) (Size, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	nv, ok := v.nm.Get(n.Id)
	// fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && nv.Size.IsValid() {
		size := nv.Size
		n.Data = nil
		n.AppendAtNs = uint64(time.Now().UnixNano())
		offset, _, _, err := n.Append(v.DataBackend, v.Version())
		v.checkReadWriteError(err)
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
func (v *Volume) readNeedle(n *needle.Needle, readOption *ReadOption) (int, error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return -1, ErrorNotFound
	}
	readSize := nv.Size
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return -1, ErrorDeleted
		}
	}
	if readSize == 0 {
		return 0, nil
	}
	err := n.ReadData(v.DataBackend, nv.Offset.ToAcutalOffset(), readSize, v.Version())
	if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
		err = n.ReadData(v.DataBackend, nv.Offset.ToAcutalOffset()+int64(MaxPossibleVolumeSize), readSize, v.Version())
	}
	v.checkReadWriteError(err)
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

func (v *Volume) startWorker() {
	go func() {
		chanClosed := false
		for {
			// chan closed. go thread will exit
			if chanClosed {
				break
			}
			currentRequests := make([]*needle.AsyncRequest, 0, 128)
			currentBytesToWrite := int64(0)
			for {
				request, ok := <-v.asyncRequestsChan
				// volume may be closed
				if !ok {
					chanClosed = true
					break
				}
				if MaxPossibleVolumeSize < v.ContentSize()+uint64(currentBytesToWrite+request.ActualSize) {
					request.Complete(0, 0, false,
						fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.ContentSize()))
					break
				}
				currentRequests = append(currentRequests, request)
				currentBytesToWrite += request.ActualSize
				// submit at most 4M bytes or 128 requests at one time to decrease request delay.
				// it also need to break if there is no data in channel to avoid io hang.
				if currentBytesToWrite >= 4*1024*1024 || len(currentRequests) >= 128 || len(v.asyncRequestsChan) == 0 {
					break
				}
			}
			if len(currentRequests) == 0 {
				continue
			}
			v.dataFileAccessLock.Lock()
			end, _, e := v.DataBackend.GetStat()
			if e != nil {
				for i := 0; i < len(currentRequests); i++ {
					currentRequests[i].Complete(0, 0, false,
						fmt.Errorf("cannot read current volume position: %v", e))
				}
				v.dataFileAccessLock.Unlock()
				continue
			}

			for i := 0; i < len(currentRequests); i++ {
				if currentRequests[i].IsWriteRequest {
					offset, size, isUnchanged, err := v.doWriteRequest(currentRequests[i].N)
					currentRequests[i].UpdateResult(offset, uint64(size), isUnchanged, err)
				} else {
					size, err := v.doDeleteRequest(currentRequests[i].N)
					currentRequests[i].UpdateResult(0, uint64(size), false, err)
				}
			}

			// if sync error, data is not reliable, we should mark the completed request as fail and rollback
			if err := v.DataBackend.Sync(); err != nil {
				// todo: this may generate dirty data or cause data inconsistent, may be weed need to panic?
				if te := v.DataBackend.Truncate(end); te != nil {
					glog.V(0).Infof("Failed to truncate %s back to %d with error: %v", v.DataBackend.Name(), end, te)
				}
				for i := 0; i < len(currentRequests); i++ {
					if currentRequests[i].IsSucceed() {
						currentRequests[i].UpdateResult(0, 0, false, err)
					}
				}
			}

			for i := 0; i < len(currentRequests); i++ {
				currentRequests[i].Submit()
			}
			v.dataFileAccessLock.Unlock()
		}
	}()
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
			// println("needle", n.Id.String(), "offset", offset, "size", n.Size, "rest", rest)
			if needleBody, err = n.ReadNeedleBody(datBackend, version, offset+NeedleHeaderSize, rest); err != nil {
				glog.V(0).Infof("cannot read needle head [%d, %d) body [%d, %d) body length %d: %v", offset, offset+NeedleHeaderSize, offset+NeedleHeaderSize, offset+NeedleHeaderSize+rest, rest, err)
				// err = fmt.Errorf("cannot read needle body: %v", err)
				// return
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
