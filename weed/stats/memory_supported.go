//go:build linux
// +build linux

package stats

import (
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func fillInMemStatus(mem *volume_server_pb.MemStatus) {
	//system memory usage
	sysInfo := new(syscall.Sysinfo_t)
	err := syscall.Sysinfo(sysInfo)
	if err == nil {
		mem.All = uint64(sysInfo.Totalram) //* uint64(syscall.Getpagesize())
		mem.Free = uint64(sysInfo.Freeram) //* uint64(syscall.Getpagesize())
		mem.Used = mem.All - mem.Free
	}
}
