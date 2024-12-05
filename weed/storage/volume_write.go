package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
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
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file at offset %d size %d: %v", nv.Offset.ToActualOffset(), nv.Size, err)
			return false
		}
		if oldNeedle.Cookie == n.Cookie && oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

var ErrVolumeNotEmpty = fmt.Errorf("volume not empty")

// Destroy removes everything related to this volume
func (v *Volume) Destroy(onlyEmpty bool) (err error) {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if onlyEmpty {
		isEmpty, e := v.doIsEmpty()
		if e != nil {
			err = fmt.Errorf("failed to read isEmpty %v", e)
			return
		}
		if !isEmpty {
			err = ErrVolumeNotEmpty
			return
		}
	}
	if v.isCompacting || v.isCommitCompacting {
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
	v.doClose()
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
	// level db index file
	os.RemoveAll(filename + ".ldb")
	// marker for damaged or incomplete volume
	os.Remove(filename + ".note")
}

func (v *Volume) asyncRequestAppend(request *needle.AsyncRequest) {
	v.asyncRequestsChan <- request
}

func (v *Volume) syncWrite(n *needle.Needle, checkCookie bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	return v.doWriteRequest(n, checkCookie)
}

func (v *Volume) writeNeedle2(n *needle.Needle, checkCookie bool, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}

	if !fsync {
		return v.syncWrite(n, checkCookie)
	} else {
		asyncRequest := needle.NewAsyncRequest(n, true)
		// using len(n.Data) here instead of n.Size before n.Size is populated in n.Append()
		asyncRequest.ActualSize = needle.GetActualSize(Size(len(n.Data)), v.Version())

		v.asyncRequestAppend(asyncRequest)
		offset, _, isUnchanged, err = asyncRequest.WaitComplete()

		return
	}
}

func (v *Volume) doWriteRequest(n *needle.Needle, checkCookie bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if v.isFileUnchanged(n) {
		size = Size(n.DataSize)
		isUnchanged = true
		return
	}

	// check whether existing needle cookie matches
	nv, ok := v.nm.Get(n.Id)
	if ok {
		existingNeedle, _, _, existingNeedleReadErr := needle.ReadNeedleHeader(v.DataBackend, v.Version(), nv.Offset.ToActualOffset())
		if existingNeedleReadErr != nil {
			err = fmt.Errorf("reading existing needle: %v", existingNeedleReadErr)
			return
		}
		if n.Cookie == 0 && !checkCookie {
			// this is from batch deletion, and read back again when tailing a remote volume
			// which only happens when checkCookie == false and fsync == false
			n.Cookie = existingNeedle.Cookie
		}
		if existingNeedle.Cookie != n.Cookie {
			glog.V(0).Infof("write cookie mismatch: existing %s, new %s",
				needle.NewFileIdFromNeedle(v.Id, existingNeedle), needle.NewFileIdFromNeedle(v.Id, n))
			err = fmt.Errorf("mismatching cookie %x", n.Cookie)
			return
		}
	}

	// append to dat file
	n.UpdateAppendAtNs(v.lastAppendAtNs)
	var actualSize int64
	offset, size, actualSize, err = n.Append(v.DataBackend, v.Version())
	v.checkReadWriteError(err)
	if err != nil {
		err = fmt.Errorf("append to volume %d size %d actualSize %d: %v", v.Id, size, actualSize, err)
		return
	}
	v.lastAppendAtNs = n.AppendAtNs

	// add to needle map
	if !ok || uint64(nv.Offset.ToActualOffset()) < offset {
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
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.nm == nil {
		return 0, nil
	}

	return v.doDeleteRequest(n)
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
		var offset uint64
		var err error
		size := nv.Size
		if !v.hasRemoteFile {
			n.Data = nil
			n.UpdateAppendAtNs(v.lastAppendAtNs)
			offset, _, _, err = n.Append(v.DataBackend, v.Version())
			v.checkReadWriteError(err)
			if err != nil {
				return size, err
			}
		}
		v.lastAppendAtNs = n.AppendAtNs
		if err = v.nm.Delete(n.Id, ToOffset(int64(offset))); err != nil {
			return size, err
		}
		return size, err
	}
	return 0, nil
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
					offset, size, isUnchanged, err := v.doWriteRequest(currentRequests[i].N, true)
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

func (v *Volume) WriteNeedleBlob(needleId NeedleId, needleBlob []byte, size Size) error {

	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if MaxPossibleVolumeSize < v.nm.ContentSize()+uint64(len(needleBlob)) {
		return fmt.Errorf("volume size limit %d exceeded! current size is %d", MaxPossibleVolumeSize, v.nm.ContentSize())
	}

	nv, ok := v.nm.Get(needleId)
	if ok && nv.Size == size {
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), nv.Size, v.Version())
		if err == nil {
			newNeedle := new(needle.Needle)
			err = newNeedle.ReadBytes(needleBlob, nv.Offset.ToActualOffset(), size, v.Version())
			if err == nil && oldNeedle.Cookie == newNeedle.Cookie && oldNeedle.Checksum == newNeedle.Checksum && bytes.Equal(oldNeedle.Data, newNeedle.Data) {
				glog.V(0).Infof("needle %v already exists", needleId)
				return nil
			}
		}
	}
	appendAtNs := needle.GetAppendAtNs(v.lastAppendAtNs)
	offset, err := needle.WriteNeedleBlob(v.DataBackend, needleBlob, size, appendAtNs, v.Version())

	v.checkReadWriteError(err)
	if err != nil {
		return err
	}
	v.lastAppendAtNs = appendAtNs

	// add to needle map
	if err = v.nm.Put(needleId, ToOffset(int64(offset)), size); err != nil {
		glog.V(4).Infof("failed to put in needle map %d: %v", needleId, err)
	}

	return err
}
