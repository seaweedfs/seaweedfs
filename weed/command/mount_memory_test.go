//go:build linux || darwin || freebsd || windows

package command

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestConfigureMountMemory(t *testing.T) {
	for _, size := range []int64{-1, 0, 1, 256, math.MaxInt64 >> 20, math.MaxInt64} {
		err := configureMountMemory(&MountOptions{readerCacheSizeMB: &size})
		valid := size > 0 && size <= math.MaxInt64>>20
		if (err == nil) != valid {
			t.Errorf("size=%d: err=%v", size, err)
		}
	}
}

func TestConfigureMountMemoryRuntimeLimit(t *testing.T) {
	previous := debug.SetMemoryLimit(512 << 20)
	defer debug.SetMemoryLimit(previous)
	for _, size := range []int64{0, -1, math.MaxInt64, 768, 0} {
		before := debug.SetMemoryLimit(-1)
		err := configureMountMemory(&MountOptions{memoryLimitMB: &size})
		valid := size >= 0 && size <= math.MaxInt64>>20
		if (err == nil) != valid {
			t.Errorf("size=%d: err=%v", size, err)
		}
		want := before
		if valid && size > 0 {
			want = size << 20
		}
		if got := debug.SetMemoryLimit(-1); got != want {
			t.Errorf("size=%d: runtime limit=%d, want %d", size, got, want)
		}
	}
}
