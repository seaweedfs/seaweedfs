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

// Destroy removes everything related to this volume. When keepRemoteData is
// true the cloud-tier object backing the volume is left intact — used by
// moves where another server is taking over the same .vif.
func (v *Volume) Destroy(onlyEmpty bool, onlyGarbage bool, keepRemoteData bool) (err error) {
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	return v.destroyLocked(onlyEmpty, onlyGarbage, keepRemoteData)
}

// destroyLocked is Destroy for callers already holding dataFileAccessLock,
// e.g. a guarded multi-copy delete that pins every copy under one lock span
// so validation and removal cannot be split by a write.
func (v *Volume) destroyLocked(onlyEmpty bool, onlyGarbage bool, keepRemoteData bool) (err error) {
	if err = v.checkDeletableLocked(onlyEmpty, onlyGarbage); err != nil {
		return
	}
	if !v.isCompactionInProgress.CompareAndSwap(false, true) {
		err = fmt.Errorf("volume %d is compacting", v.Id)
		return
	}
	storageName, storageKey := v.RemoteStorageNameKey()
	if !keepRemoteData && v.HasRemoteFile() && storageName != "" && storageKey != "" {
		if backendStorage, found := backend.BackendStorages[storageName]; found {
			if remoteErr := backendStorage.DeleteFile(storageKey); remoteErr != nil {
				// Stop short of local cleanup: a failed remote delete must
				// not strand a registered replica with no local files.
				v.isCompactionInProgress.Store(false)
				err = fmt.Errorf("volume %d remote object %s/%s delete failed: %w", v.Id, storageName, storageKey, remoteErr)
				return
			}
		}
	}
	v.stopWorker()
	// A regular volume and an EC volume for the same id share <base>.vif. When
	// EC artefacts coexist on this disk (e.g. shards distributed onto a source
	// replica before it is deleted), keep the .vif so removing the regular
	// volume does not strip the EC volume's info file.
	keepVif := v.sharesVifWithEcVolume()
	v.doClose()
	removeVolumeFiles(v.DataFileName(), keepVif)
	removeVolumeFiles(v.IndexFileName(), keepVif)
	return
}

// checkDeletableLocked enforces the guards Destroy applies before removing
// any file: either enabled check may pass, since a volume with no live data
// qualifies whether it reads empty or as all garbage.
func (v *Volume) checkDeletableLocked(onlyEmpty bool, onlyGarbage bool) (err error) {
	if !onlyEmpty && !onlyGarbage {
		return nil
	}
	emptyOk := false
	if onlyEmpty {
		isEmpty, e := v.doIsEmpty()
		if e != nil {
			return fmt.Errorf("failed to read isEmpty %v", e)
		}
		emptyOk = isEmpty
	}
	if !emptyOk && !(onlyGarbage && v.doIsGarbage()) {
		return ErrVolumeNotEmpty
	}
	return nil
}

// sharesVifWithEcVolume reports whether an EC volume for this volume id lives
// on the same disk, in which case its .vif is the same file as the regular
// volume's and must outlive the regular volume's deletion.
func (v *Volume) sharesVifWithEcVolume() bool {
	if v.location == nil {
		return false
	}
	if _, found := v.location.FindEcVolume(v.Id); found {
		return true
	}
	return v.location.HasEcxFileOnDisk(v.Collection, v.Id)
}

func removeVolumeFiles(filename string, keepVif bool) {
	// .dat/.idx removals log at V(0) so destructive calls are traceable.
	deleteAndLog := func(ext string) {
		fullFilename := filename + "." + ext
		st, statErr := os.Stat(fullFilename)
		err := os.RemoveAll(fullFilename)
		if err != nil {
			glog.V(0).Infof("failed to remove volume file %s: %s", fullFilename, err)
			return
		}
		if statErr == nil && (ext == "dat" || ext == "idx") {
			glog.Infof("removed volume file %s (size=%d)", fullFilename, st.Size())
		}
	}
	deleteAndLog("dat")
	deleteAndLog("idx")
	if !keepVif {
		deleteAndLog("vif")
	}
	// sorted index file
	deleteAndLog("sdx")
	// compaction
	deleteAndLog("cpd")
	deleteAndLog("cpx")
	// compaction commit marker
	deleteAndLog("cpc")
	// level db index file
	deleteAndLog("ldb")
	// redb index file (Rust volume server)
	deleteAndLog("rdb")
	// marker for damaged or incomplete volume
	deleteAndLog("note")
	// marker for a volume whose failed-batch recovery could not be verified
	deleteAndLog("unavailable")
}

// asyncRequestAppend queues a request for the batch worker, starting it on the
// first one. It reports false for a destroyed volume, so the caller writes
// inline rather than wait on a worker that will never answer.
func (v *Volume) asyncRequestAppend(request *needle.AsyncRequest) bool {
	requests := v.startWorker()
	if requests == nil {
		return false
	}
	requests <- request
	return true
}

func (v *Volume) syncWrite(n *needle.Needle, checkCookie bool, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if err := v.UnavailableError(); err != nil {
		return 0, 0, false, err
	}
	if v.IsReadOnly() {
		return 0, 0, false, fmt.Errorf("volume %d is read only", v.Id)
	}

	// A caller can still hold the volume after it was closed or destroyed, which
	// leaves both of these nil. Refuse the write rather than dereference them.
	if v.nm == nil || v.DataBackend == nil {
		return 0, 0, false, fmt.Errorf("volume %d is closed", v.Id)
	}

	if !fsync {
		return v.doWriteRequest(n, checkCookie, fsync)
	}

	end, _, statErr := v.DataBackend.GetStat()
	if statErr != nil {
		return 0, 0, false, fmt.Errorf("cannot read current volume position: %v", statErr)
	}

	lastAppendAtNs := v.lastAppendAtNs
	lastWriteAppendAtNs := v.lastWriteAppendAtNs
	lastWriteNeedleKey := v.lastWriteNeedleKey
	lastWriteDeleted := v.lastWriteDeleted
	lastModifiedTsSeconds := v.lastModifiedTsSeconds

	offset, size, isUnchanged, err = v.appendWriteRequest(n, checkCookie, fsync)
	if err != nil {
		return
	}
	// fsync with the access lock dropped so same-volume reads do not wait on
	// it; writers and compaction stay excluded by dataFileSyncLock. Nothing
	// has been published yet, so no reader can capture the appended offset.
	backend := v.DataBackend
	v.dataFileAccessLock.Unlock()
	syncErr := backend.Sync()
	v.dataFileAccessLock.Lock()
	if syncErr != nil {
		v.checkReadWriteError(syncErr)
		v.lastAppendAtNs = lastAppendAtNs
		v.lastWriteAppendAtNs = lastWriteAppendAtNs
		v.lastWriteNeedleKey = lastWriteNeedleKey
		v.lastWriteDeleted = lastWriteDeleted
		v.lastModifiedTsSeconds = lastModifiedTsSeconds
		if !isUnchanged {
			// Nothing was published, so the whole rollback is taking the
			// unflushed append back off the .dat.
			if te := v.DataBackend.Truncate(end); te != nil {
				v.markIoUnavailable(fmt.Errorf("truncate %s back to %d after a failed fsync: %w",
					v.DataBackend.Name(), end, te))
			}
		}
		return 0, 0, false, syncErr
	}
	if !isUnchanged {
		err = v.publishWriteRequest(n, offset, fsync)
	}
	return
}

// writeNeedle2 appends a needle. A durable write normally goes through the
// async batch worker, which fsyncs once for the whole batch; while the server
// is stopping the worker is winding down, so it is flushed inline instead. Both
// paths only return once the .dat is on disk.
func (v *Volume) writeNeedle2(n *needle.Needle, checkCookie bool, fsync bool, isStopping bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if err := v.UnavailableError(); err != nil {
		return 0, 0, false, err
	}

	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}

	if !fsync || isStopping {
		return v.syncWrite(n, checkCookie, fsync)
	} else {
		asyncRequest := needle.NewAsyncRequest(n, true)
		// using len(n.Data) here instead of n.Size before n.Size is populated in n.Append()
		asyncRequest.ActualSize = needle.GetActualSize(Size(len(n.Data)), v.Version())

		if !v.asyncRequestAppend(asyncRequest) {
			return v.syncWrite(n, checkCookie, fsync)
		}
		offset, _, isUnchanged, err = asyncRequest.WaitComplete()

		return
	}
}

func (v *Volume) doWriteRequest(n *needle.Needle, checkCookie bool, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	offset, size, isUnchanged, err = v.appendWriteRequest(n, checkCookie, fsync)
	if err != nil || isUnchanged {
		return
	}
	err = v.publishWriteRequest(n, offset, fsync)
	return
}

// appendWriteRequest is doWriteRequest through the .dat append, leaving the
// needle-map update to publishWriteRequest.
func (v *Volume) appendWriteRequest(n *needle.Needle, checkCookie bool, fsync bool) (offset uint64, size Size, isUnchanged bool, err error) {
	// glog.V(4).Infof("writing needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	// Re-check under the write lock: the outer checks run before the needle
	// is assembled, and VolumeMarkReadonly can land in between.
	if v.IsReadOnly() {
		return 0, 0, false, fmt.Errorf("volume %d is read only", v.Id)
	}
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
			err = fmt.Errorf("reading existing needle: %w", existingNeedleReadErr)
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
	v.lastWriteAppendAtNs = n.AppendAtNs
	v.lastWriteNeedleKey = n.Id
	v.lastWriteDeleted = false

	return
}

// publishWriteRequest indexes an already-appended needle. It is split from
// appendWriteRequest so a durable write can fsync before the mapping exists:
// an index entry that outlives the bytes it points at loads read-only, and
// an unpublished offset can never be captured by an in-flight read before a
// failed sync truncates it.
func (v *Volume) publishWriteRequest(n *needle.Needle, offset uint64, fsync bool) (err error) {
	nv, ok := v.nm.Get(n.Id)
	if !ok || uint64(nv.Offset.ToActualOffset()) < offset {
		if err = v.nm.Put(n.Id, ToOffset(int64(offset)), n.Size); err != nil {
			err = fmt.Errorf("index needle %d of volume %d at offset %d: %w", n.Id, v.Id, offset, err)
			glog.V(0).Info(err)
			if fsync {
				// The record is down but nothing indexes it. Stop taking
				// writes rather than append past it, same as the Rust
				// volume server.
				v.noWriteLock.Lock()
				v.noWriteOrDelete = true
				v.noWriteLock.Unlock()
			}
		}
	}
	if v.lastModifiedTsSeconds < n.LastModified {
		v.lastModifiedTsSeconds = n.LastModified
	}
	return
}

func (v *Volume) syncDelete(n *needle.Needle) (Size, error) {
	// glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if err := v.UnavailableError(); err != nil {
		return 0, err
	}
	if _, noWriteOrDelete, _, _ := v.ReadOnlyReasons(); noWriteOrDelete {
		return 0, fmt.Errorf("volume %d is read only", v.Id)
	}

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

		if !v.asyncRequestAppend(asyncRequest) {
			return v.syncDelete(n)
		}
		_, size, _, err := asyncRequest.WaitComplete()

		return Size(size), err
	}
}

func (v *Volume) doDeleteRequest(n *needle.Needle) (Size, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	size, offset, tombstoned, err := v.appendDeleteRequest(n)
	if err != nil || !tombstoned {
		return size, err
	}
	if err = v.publishDeleteRequest(n, offset); err != nil {
		return size, err
	}
	return size, nil
}

// appendDeleteRequest writes the tombstone record but leaves the map update
// to publishDeleteRequest, so a batch can fsync before any deletion is
// visible — same publication-after-sync ordering as writes.
func (v *Volume) appendDeleteRequest(n *needle.Needle) (size Size, offset uint64, tombstoned bool, err error) {
	nv, ok := v.nm.Get(n.Id)
	// fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if !ok || nv.Size.IsDeleted() {
		return 0, 0, false, nil
	}
	size = nv.Size
	if !v.HasRemoteFile() {
		n.Data = nil
		n.UpdateAppendAtNs(v.lastAppendAtNs)
		offset, _, _, err = n.Append(v.DataBackend, v.Version())
		v.checkReadWriteError(err)
		if err != nil {
			return size, 0, false, err
		}
	}
	v.lastAppendAtNs = n.AppendAtNs
	return size, offset, true, nil
}

func (v *Volume) publishDeleteRequest(n *needle.Needle, offset uint64) error {
	if err := v.nm.Delete(n.Id, ToOffset(int64(offset))); err != nil {
		return err
	}
	if n.Id == v.lastWriteNeedleKey {
		v.lastWriteAppendAtNs, v.lastWriteNeedleKey = 0, 0
		v.lastWriteDeleted = true
	}
	return nil
}

func (v *Volume) processBatch(currentRequests []*needle.AsyncRequest) {
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()

	// The batch is applied in runs of distinct needle ids: a repeated id
	// starts a new run so its dedup, cookie and delete checks see the earlier
	// operation's published result — the same rule as Rust's
	// write_needles_grouped.
	seen := make(map[NeedleId]bool, len(currentRequests))
	start := 0
	for i := 0; i < len(currentRequests); i++ {
		if seen[currentRequests[i].N.Id] {
			v.processBatchRun(currentRequests[start:i])
			seen = make(map[NeedleId]bool, len(currentRequests)-i)
			start = i
		}
		seen[currentRequests[i].N.Id] = true
	}
	if start < len(currentRequests) {
		v.processBatchRun(currentRequests[start:])
	}
}

// processBatchRun applies one run of distinct-id requests: every .dat
// mutation is staged, the run is synced once with the access lock dropped,
// and only then are the map changes published — in request order, and only
// over an older offset — so a failed sync rolls back bytes no reader ever
// resolved. A durable entry that fails to index stops the volume taking
// writes, and the entries after it in the run are refused read only.
func (v *Volume) processBatchRun(run []*needle.AsyncRequest) {
	v.dataFileAccessLock.Lock()

	end, e := int64(0), error(nil)
	if unavailableErr := v.UnavailableError(); unavailableErr != nil {
		e = unavailableErr
	} else if v.nm == nil || v.DataBackend == nil {
		e = fmt.Errorf("volume %d is closed", v.Id)
	} else {
		end, _, e = v.DataBackend.GetStat()
	}
	if e != nil {
		v.dataFileAccessLock.Unlock()
		for i := 0; i < len(run); i++ {
			run[i].Complete(0, 0, false,
				fmt.Errorf("cannot read current volume position: %v", e))
		}
		return
	}

	batchLastAppendAtNs := v.lastAppendAtNs
	batchLastWriteAppendAtNs := v.lastWriteAppendAtNs
	batchLastWriteNeedleKey := v.lastWriteNeedleKey
	batchLastWriteDeleted := v.lastWriteDeleted
	batchLastModifiedTsSeconds := v.lastModifiedTsSeconds

	stagedWriteOffsets := make([]uint64, len(run))
	stagedWriteOk := make([]bool, len(run))
	stagedDeleteOffsets := make([]uint64, len(run))
	stagedDeleteOk := make([]bool, len(run))
	for i := 0; i < len(run); i++ {
		// Every queued request meets the same refusal it would get sent on
		// its own: once a durable index update fails the volume stops taking
		// writes, and a lone write is turned away before it appends.
		if run[i].IsWriteRequest {
			if unavailableErr := v.UnavailableError(); unavailableErr != nil {
				run[i].UpdateResult(0, 0, false, unavailableErr)
			} else if v.IsReadOnly() {
				run[i].UpdateResult(0, 0, false, fmt.Errorf("volume %d is read only", v.Id))
			} else {
				offset, size, isUnchanged, err := v.appendWriteRequest(run[i].N, true, true)
				if err == nil && !isUnchanged {
					stagedWriteOffsets[i] = offset
					stagedWriteOk[i] = true
				}
				run[i].UpdateResult(offset, uint64(size), isUnchanged, err)
			}
		} else {
			if unavailableErr := v.UnavailableError(); unavailableErr != nil {
				run[i].UpdateResult(0, 0, false, unavailableErr)
			} else if _, noWriteOrDelete, _, _ := v.ReadOnlyReasons(); noWriteOrDelete {
				run[i].UpdateResult(0, 0, false, fmt.Errorf("volume %d is read only", v.Id))
			} else {
				size, offset, tombstoned, err := v.appendDeleteRequest(run[i].N)
				if err == nil && tombstoned {
					stagedDeleteOffsets[i] = offset
					stagedDeleteOk[i] = true
				}
				run[i].UpdateResult(0, uint64(size), false, err)
			}
		}
	}

	// The sync excludes writers and compaction via dataFileSyncLock but not
	// readers: nothing is published yet, so dropping the access lock here
	// lets same-volume reads proceed during the fsync.
	backend := v.DataBackend
	v.dataFileAccessLock.Unlock()
	syncErr := backend.Sync()
	v.dataFileAccessLock.Lock()
	if syncErr != nil {
		v.checkReadWriteError(syncErr)
		v.lastAppendAtNs = batchLastAppendAtNs
		v.lastWriteAppendAtNs = batchLastWriteAppendAtNs
		v.lastWriteNeedleKey = batchLastWriteNeedleKey
		v.lastWriteDeleted = batchLastWriteDeleted
		v.lastModifiedTsSeconds = batchLastModifiedTsSeconds
		batchErr := syncErr
		// Nothing was published: truncating the run's bytes is the whole
		// rollback, and no reader ever resolved them.
		if te := v.DataBackend.Truncate(end); te != nil {
			batchErr = errors.Join(syncErr,
				fmt.Errorf("truncate %s to %d: %w", v.DataBackend.Name(), end, te))
			v.markIoUnavailable(batchErr)
		}
		for i := 0; i < len(run); i++ {
			run[i].UpdateResult(0, 0, false, batchErr)
		}
		v.dataFileAccessLock.Unlock()
		for i := 0; i < len(run); i++ {
			run[i].Submit()
		}
		return
	}

	refused := false
	for i := 0; i < len(run); i++ {
		if refused {
			run[i].UpdateResult(0, 0, false, fmt.Errorf("volume %d is read only", v.Id))
			continue
		}
		if run[i].IsWriteRequest {
			if stagedWriteOk[i] {
				if perr := v.publishWriteRequest(run[i].N, stagedWriteOffsets[i], true); perr != nil {
					run[i].UpdateResult(0, 0, false, perr)
					refused = true
				}
			}
		} else if stagedDeleteOk[i] {
			if perr := v.publishDeleteRequest(run[i].N, stagedDeleteOffsets[i]); perr != nil {
				run[i].UpdateResult(0, 0, false, perr)
			}
		}
	}
	v.dataFileAccessLock.Unlock()
	for i := 0; i < len(run); i++ {
		run[i].Submit()
	}
}

// startWorker returns the volume's batch-write channel, creating it and its
// goroutine on first use, and nil once stopWorker has run.
func (v *Volume) startWorker() chan *needle.AsyncRequest {
	v.asyncWorkerLock.Lock()
	defer v.asyncWorkerLock.Unlock()
	if v.asyncWorkerClosed {
		return nil
	}
	if v.asyncRequestsChan != nil {
		return v.asyncRequestsChan
	}
	requests := make(chan *needle.AsyncRequest, 128)
	v.asyncRequestsChan = requests
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
				request, ok := <-requests
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
				if currentBytesToWrite >= 4*1024*1024 || len(currentRequests) >= 128 || len(requests) == 0 {
					break
				}
			}
			if len(currentRequests) == 0 {
				continue
			}
			v.processBatch(currentRequests)
		}
	}()
	return requests
}

// stopWorker closes the batch-write channel so the worker drains what is queued
// and exits. It stays closed: a destroyed volume takes no more writes.
func (v *Volume) stopWorker() {
	v.asyncWorkerLock.Lock()
	defer v.asyncWorkerLock.Unlock()
	if v.asyncWorkerClosed {
		return
	}
	v.asyncWorkerClosed = true
	if v.asyncRequestsChan != nil {
		close(v.asyncRequestsChan)
		v.asyncRequestsChan = nil
	}
}

func (v *Volume) WriteNeedleBlob(needleId NeedleId, needleBlob []byte, size Size) error {

	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if err := v.UnavailableError(); err != nil {
		return err
	}

	// nm.Put on a read-only volume fails only after the blob is appended to .dat.
	if v.IsReadOnly() {
		return fmt.Errorf("volume %d is read only", v.Id)
	}
	if size.IsDeleted() {
		return fmt.Errorf("needle %d has invalid size %d", needleId, size)
	}

	// size indexes the needle and places the v3 append timestamp, so a caller using
	// the payload-only DataSize corrupts both, silently until the needle is read back.
	if len(needleBlob) < NeedleHeaderSize {
		return fmt.Errorf("needle %d blob of %d bytes is shorter than a needle header", needleId, len(needleBlob))
	}
	var blobHeader needle.Needle
	blobHeader.ParseNeedleHeader(needleBlob)
	if blobHeader.Size != size {
		return fmt.Errorf("needle %d size %d does not match its blob header size %d", needleId, size, blobHeader.Size)
	}
	// The blob is appended as is: its length must be the record this size takes in this volume's version.
	if actualSize := needle.GetActualSize(size, v.Version()); int64(len(needleBlob)) != actualSize {
		return fmt.Errorf("needle %d blob of %d bytes does not match the %d bytes size %d takes in a version %d volume", needleId, len(needleBlob), actualSize, size, v.Version())
	}

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
	if v.IsReadOnly() {
		return fmt.Errorf("volume %d is read only", v.Id)
	}
	appendAtNs := needle.GetAppendAtNs(v.lastAppendAtNs)
	offset, err := needle.WriteNeedleBlob(v.DataBackend, needleBlob, size, appendAtNs, v.Version())

	v.checkReadWriteError(err)
	if err != nil {
		return err
	}
	v.lastAppendAtNs = appendAtNs
	v.lastWriteAppendAtNs = appendAtNs
	v.lastWriteNeedleKey = needleId
	v.lastWriteDeleted = false

	// add to needle map
	if err = v.nm.Put(needleId, ToOffset(int64(offset)), size); err != nil {
		err = fmt.Errorf("index needle %d of volume %d at offset %d: %w", needleId, v.Id, offset, err)
		glog.V(0).Info(err)
	}

	return err
}
