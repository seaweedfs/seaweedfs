package buffer_pool

import (
	"bytes"
	"sync"
)

var SyncPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
