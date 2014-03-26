// +build !windows

package stats

import (
	"runtime"
	"syscall"
)

type MemStatus struct {
	Goroutines int
	All        uint64
	Used       uint64
	Free       uint64
	Self       uint64
	Heap       uint64
	Stack      uint64
}

func MemStat() MemStatus {
	mem := MemStatus{}
	mem.Goroutines = runtime.NumGoroutine()
	memStat := new(runtime.MemStats)
	runtime.ReadMemStats(memStat)
	mem.Self = memStat.Alloc
	mem.Heap = memStat.HeapAlloc
	mem.Stack = memStat.StackInuse

	//system memory usage
	sysInfo := new(syscall.Sysinfo_t)
	err := syscall.Sysinfo(sysInfo)
	if err == nil {
		mem.All = sysInfo.Totalram //* uint64(syscall.Getpagesize())
		mem.Free = sysInfo.Freeram //* uint64(syscall.Getpagesize())
		mem.Used = mem.All - mem.Free
	}
	return mem
}
