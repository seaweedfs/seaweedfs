//go:build !windows && !openbsd && !netbsd && !plan9 && !solaris
// +build !windows,!openbsd,!netbsd,!plan9,!solaris

package main

import (
	"log"
	"syscall"
)

// go run unmaintained/disk/disk_status.go

type DiskStatus struct {
	Dir         string  `protobuf:"bytes,1,opt,name=dir,proto3" json:"dir,omitempty"`
	All         uint64  `protobuf:"varint,2,opt,name=all,proto3" json:"all,omitempty"`
	Used        uint64  `protobuf:"varint,3,opt,name=used,proto3" json:"used,omitempty"`
	Free        uint64  `protobuf:"varint,4,opt,name=free,proto3" json:"free,omitempty"`
	PercentFree float32 `protobuf:"fixed32,5,opt,name=percent_free,json=percentFree,proto3" json:"percent_free,omitempty"`
	PercentUsed float32 `protobuf:"fixed32,6,opt,name=percent_used,json=percentUsed,proto3" json:"percent_used,omitempty"`
	DiskType    string  `protobuf:"bytes,7,opt,name=disk_type,json=diskType,proto3" json:"disk_type,omitempty"`

	// new fields about avail blocks
	Avail        uint64  `protobuf:"varint,4,opt,name=avail,proto3" json:"avail,omitempty"`
	PercentAvail float32 `protobuf:"fixed32,5,opt,name=percent_avail,json=percentAvail,proto3" json:"percent_avail,omitempty"`
}

func main() {
	dirs := []string{"/mnt/sdb", "/mnt/sdc", "/mnt/sdd", "/mnt/sde", "/mnt/sdf", "/mnt/sdg", "/mnt/sdh", "/mnt/sdi", "/mnt/sdj"}
	// dirs := []string{"/mnt/sdb"}
	for _, dir := range dirs {
		disk := &DiskStatus{Dir: dir}
		fillInDiskStatus(disk)

		// bytes, _ := json.Marshal(disk)
		// log.Printf("disk status %s", bytes)
		log.Printf("disk: %s avail: %f free: %f", disk.Dir, disk.PercentAvail, disk.PercentFree)
	}
}

func fillInDiskStatus(disk *DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(disk.Dir, &fs)
	if err != nil {
		return
	}

	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	disk.PercentFree = float32((float64(disk.Free) / float64(disk.All)) * 100)
	disk.PercentUsed = float32((float64(disk.Used) / float64(disk.All)) * 100)

	// avail blocks
	disk.Avail = fs.Bavail * uint64(fs.Bsize)
	disk.PercentAvail = float32((float64(disk.Avail) / float64(disk.All)) * 100)
	return
}
