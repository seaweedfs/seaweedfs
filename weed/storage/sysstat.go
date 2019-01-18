package storage

import (
	"fmt"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"os"
	"path/filepath"
	"syscall"
)

// MemoryStat stats the current total and free memory of the host
func MemoryStat() (total, free uint64, err error) {
	stat, err := mem.VirtualMemory()
	if err != nil {
		return 0, 0, err
	}

	return stat.Total, stat.Free, nil
}

// DiskStat stats the current total and free of specified dir
func DiskStat(dir string) (total, free uint64, device, mountPoint string, err error) {
	absPath, _ := filepath.Abs(dir)
	stat, err := disk.Usage(absPath)
	if err != nil {
		return 0, 0, "", "", err
	}

	point, err := MountPoint(absPath)
	if err != nil {
		return 0, 0, "", "", err
	}

	partitions, _ := disk.Partitions(false)
	for _, p := range partitions {
		if p.Mountpoint == point {
			device = p.Device
			break
		}
		fmt.Println(p.Device, p.Fstype, p.Mountpoint)
	}

	return stat.Total, stat.Free, device, point, nil
}

// LoadStat return average load1, load5 and load15 of the host
func LoadStat() (load1, load5, load15 float64, err error) {
	stat, e := load.Avg()
	if e != nil {
		return 0, 0, 0, e
	}

	return stat.Load1, stat.Load5, stat.Load15, nil
}

// ProcessStat return cpu usage and RSS of the current process
func ProcessStat() (name string, cpuUsage float64, rss uint64, err error) {
	p, e := process.NewProcess(int32(os.Getpid()))
	if e != nil {
		return "", 0, 0, e
	}

	stat, e := p.MemoryInfo()
	if e != nil {
		return "", 0, 0, e
	}

	cpuUsage, e = p.CPUPercent()
	if e != nil {
		return "", 0, 0, e
	}

	name, e = p.Name()
	if e != nil {
		return "", 0, 0, e
	}

	return name, cpuUsage, stat.RSS, nil
}

func MountPoint(absPath string) (string, error) {
	pi, err := os.Stat(absPath)
	if err != nil {
		return "", err
	}

	odev := pi.Sys().(*syscall.Stat_t).Dev

	for absPath != "/" {
		_path := filepath.Dir(absPath)

		in, err := os.Stat(_path)
		if err != nil {
			return "", err
		}

		if odev != in.Sys().(*syscall.Stat_t).Dev {
			break
		}

		absPath = _path
	}

	return absPath, nil
}
