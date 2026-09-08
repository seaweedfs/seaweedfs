//go:build linux || darwin || freebsd || windows

package command

import (
	"math"
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
