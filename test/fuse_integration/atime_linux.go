//go:build linux

package fuse

import (
	"syscall"
)

// getAtimeNano returns the access time in nanoseconds for Linux systems
func getAtimeNano(stat *syscall.Stat_t) int64 {
	return stat.Atim.Sec*1e9 + stat.Atim.Nsec
}
