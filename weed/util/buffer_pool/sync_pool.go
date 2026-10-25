package buffer_pool

import (
	"bytes"
	"sync"
)

// maxRetainedBufferCap caps the capacity of buffers we hand back to the
// sync.Pool. Buffers grown past this (e.g. by a 64 MiB chunk upload through
// volume.PostHandler -> needle.ParseUpload -> bytes.Buffer.ReadFrom) are
// dropped instead of pooled, so the underlying byte array becomes garbage
// and is collected. Without this cap the pool effectively hoards every
// high-water buffer for the process's lifetime — see #6541, where Harbor's
// concurrent UploadPartCopy filled the pool with 64 MiB buffers and RSS
// never receded.
const maxRetainedBufferCap = 4 * 1024 * 1024

var syncPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func SyncPoolGetBuffer() *bytes.Buffer {
	return syncPool.Get().(*bytes.Buffer)
}

func SyncPoolPutBuffer(buffer *bytes.Buffer) {
	if buffer == nil {
		return
	}
	if buffer.Cap() > maxRetainedBufferCap {
		// Drop the buffer; let GC reclaim the oversized backing array.
		return
	}
	buffer.Reset()
	syncPool.Put(buffer)
}
