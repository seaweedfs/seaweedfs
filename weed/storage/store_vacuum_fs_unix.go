//go:build !windows

package storage

import (
	"os"
	"syscall"
)

// sameFilesystem reports whether two directories share one free-space pool.
// When in doubt it says yes, which makes the space check ask for the sum.
func sameFilesystem(a, b string) bool {
	if a == b {
		return true
	}
	sa, errA := os.Stat(a)
	sb, errB := os.Stat(b)
	if errA != nil || errB != nil {
		return true
	}
	sta, okA := sa.Sys().(*syscall.Stat_t)
	stb, okB := sb.Sys().(*syscall.Stat_t)
	if !okA || !okB {
		return true
	}
	return sta.Dev == stb.Dev
}
