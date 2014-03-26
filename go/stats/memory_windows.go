// +build windows

package stats

import (
	"runtime"
)

type MemStatus struct {
	Goroutines uint32
	All        uint32
	Used       uint32
	Free       uint32
	Self       uint64
	Heap       uint64
	Stack      uint64
}

func MemStat() MemStatus {
	memStat := new(runtime.MemStats)
	mem.Goroutines = runtime.NumGoroutine()
	runtime.ReadMemStats(memStat)
	mem := MemStatus{}
	mem.Self = memStat.Alloc
	mem.Heap = memStat.HeapAlloc
	mem.Stack = memStat.StackInuse

	return mem
}
