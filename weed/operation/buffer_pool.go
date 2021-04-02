package operation

import (
	"github.com/valyala/bytebufferpool"
	"sync/atomic"
)

var bufferCounter int64

func GetBuffer() *bytebufferpool.ByteBuffer {
	defer func() {
		atomic.AddInt64(&bufferCounter, 1)
		// println("+", bufferCounter)
	}()
	return bytebufferpool.Get()
}

func PutBuffer(buf *bytebufferpool.ByteBuffer) {
	defer func() {
		atomic.AddInt64(&bufferCounter, -1)
		// println("-", bufferCounter)
	}()
	bytebufferpool.Put(buf)
}
