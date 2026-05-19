//go:build !windows && !openbsd && !netbsd && !plan9 && !solaris

package stats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type diskState struct {
	mu           sync.Mutex
	isChecking   bool
	failureCount int
	lastErr      error
}

var diskRegistry sync.Map

const (
	maxFailuresBeforeAlert = 3
	diskTimeout            = 500 * time.Millisecond
)

func setDiskStatus(disk *volume_server_pb.DiskStatus) {
	actual, _ := diskRegistry.LoadOrStore(disk.Dir, &diskState{})
	state := actual.(*diskState)

	state.mu.Lock()

	if state.isChecking {
		state.failureCount++

		if state.failureCount > maxFailuresBeforeAlert {
			state.writeErrorLocked(disk)
		}

		state.mu.Unlock()
		return
	}

	state.isChecking = true
	state.mu.Unlock()

	dir := disk.Dir

	ctx, cancel := context.WithTimeout(context.Background(), diskTimeout)
	defer cancel()

	type result struct {
		fs  syscall.Statfs_t
		err error
	}

	ch := make(chan result, 1)

	go func() {
		var fs syscall.Statfs_t
		err := syscall.Statfs(dir, &fs)
		ch <- result{fs: fs, err: err}
	}()

	var (
		currentErr error
		fs         syscall.Statfs_t
	)

	select {
	case res := <-ch:
		currentErr = res.err
		fs = res.fs

	case <-ctx.Done():
		currentErr = errors.New("statfs timeout")
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	state.isChecking = false

	if currentErr != nil {
		state.failureCount++
		state.lastErr = currentErr

		if state.failureCount >= maxFailuresBeforeAlert {
			state.writeErrorLocked(disk)
		}
		return
	}

	state.failureCount = 0
	state.lastErr = nil
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bavail * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	disk.PercentFree = float32(float64(disk.Free) / float64(disk.All) * 100)
	disk.PercentUsed = float32(float64(disk.Used) / float64(disk.All) * 100)
}

func (s *diskState) writeErrorLocked(disk *volume_server_pb.DiskStatus) {
	if s.lastErr == nil {
		disk.Error = "disk health check failed"
		return
	}

	disk.Error = fmt.Sprintf("disk health check failed: %v", s.lastErr)
}
