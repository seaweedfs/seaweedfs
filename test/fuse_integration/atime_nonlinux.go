//go:build !linux && !windows

package fuse

import (
	"syscall"
)

// getAtimeNano returns the access time in nanoseconds for non-Linux systems (macOS, BSD, etc.)
func getAtimeNano(stat *syscall.Stat_t) int64 {
	return stat.Atimespec.Sec*1e9 + stat.Atimespec.Nsec
}
