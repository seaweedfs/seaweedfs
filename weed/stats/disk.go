package stats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	setDiskStatus(disk)
	if disk.PercentUsed > 95 {
		glog.V(0).Infof("disk status: %v", disk)
	}
	return
}

// setDiskStatus fills disk by calling the platform fillInDiskStatus under a
// timeout, so a stuck filesystem cannot block the caller. Repeated failures are
// surfaced through disk.Error once they cross maxFailuresBeforeAlert.
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

	ctx, cancel := context.WithTimeout(context.Background(), diskTimeout)
	defer cancel()

	probe := &volume_server_pb.DiskStatus{Dir: disk.Dir}
	ch := make(chan error, 1)
	go func() {
		ch <- fillInDiskStatus(probe)
	}()

	var probeErr error
	select {
	case probeErr = <-ch:
	case <-ctx.Done():
		probeErr = errors.New("statfs timeout")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	state.isChecking = false

	if probeErr != nil {
		state.failureCount++
		state.lastErr = probeErr
		if state.failureCount >= maxFailuresBeforeAlert {
			state.writeErrorLocked(disk)
		}
		return
	}

	state.failureCount = 0
	state.lastErr = nil
	disk.All = probe.All
	disk.Free = probe.Free
	disk.Used = probe.Used
	disk.PercentFree = probe.PercentFree
	disk.PercentUsed = probe.PercentUsed
}

func (s *diskState) writeErrorLocked(disk *volume_server_pb.DiskStatus) {
	if s.lastErr == nil {
		disk.Error = "disk health check failed"
		return
	}
	disk.Error = fmt.Sprintf("disk health check failed: %v", s.lastErr)
}
