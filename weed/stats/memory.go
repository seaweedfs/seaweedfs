package stats

import (
	"runtime"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func MemStat() *volume_server_pb.MemStatus {
	mem := &volume_server_pb.MemStatus{}
	mem.Goroutines = int32(runtime.NumGoroutine())
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	mem.Self = memStat.Alloc
	mem.Heap = memStat.HeapAlloc
	mem.Stack = memStat.StackInuse

	fillInMemStatus(mem)
	return mem
}
