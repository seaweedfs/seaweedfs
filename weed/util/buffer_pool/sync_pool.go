package buffer_pool

import (
	"bytes"
	"sync"
)

var syncPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func SyncPoolGetBuffer() *bytes.Buffer {
	return syncPool.Get().(*bytes.Buffer)
}

func SyncPoolPutBuffer(buffer *bytes.Buffer) {
	syncPool.Put(buffer)
}
