package mount

// evictOneWritableChunk is the callback the WriteBufferAccountant invokes
// when Reserve would otherwise block. It walks open file handles and asks
// the first one that has a writable chunk to force-seal it. Sealing moves
// the chunk into the async upload path, whose completion Releases a slot
// on the accountant and unblocks the waiter.
//
// Without this hook, workloads that open many files for write but never
// fill or close any single chunk (e.g. fio 4k randwrite across nrfiles
// where nrfiles * chunkSizeLimit > writeBufferSizeMB) deadlock
// permanently: every writable chunk reserves a full chunkSize slot on
// creation, and the pipeline only releases the slot when the chunk
// finishes uploading, which only happens after the chunk fills or the
// file closes.
//
// Called from WriteBufferAccountant.Reserve with accountant.mu dropped.
// Must not call anything that takes accountant.mu back; see the caller's
// single-flight `evicting` flag for safety against concurrent invocation.
func (wfs *WFS) evictOneWritableChunk(needBytes int64) bool {
	wfs.fhMap.RLock()
	defer wfs.fhMap.RUnlock()
	for _, fh := range wfs.fhMap.inode2fh {
		if fh == nil || fh.dirtyPages == nil {
			continue
		}
		if fh.dirtyPages.EvictOneWritableChunk() {
			return true
		}
	}
	return false
}
