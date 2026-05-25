package stats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type DiskIOProbeConfig struct {
	// Enable/disable disk IO latency probing
	Enabled bool

	// EWMA smoothing factor for latency calculation (0-1), lower = smoother
	LatencyAlpha float64

	Timeout time.Duration

	// Minimum latency threshold to consider as potential slow operation
	MinSlowLatency time.Duration

	// Multiplier above average latency to flag as slow (current > avg * factor)
	SlowLatencyFactor float64

	// Multiplier above standard deviation to flag as slow (current > avg + stddev * factor)
	SlowStddevFactor float64

	// Number of consecutive slow operations required before reporting disk failure
	MaxConsecutiveSlow int

	// Number of IO failures tolerated before setting disk.Error alert
	MaxFailuresBeforeAlert int
}

func DefaultDiskIOProbeConfig() DiskIOProbeConfig {
	return DiskIOProbeConfig{
		Enabled:                false,
		Timeout:                2 * time.Second,
		LatencyAlpha:           0.2,
		MinSlowLatency:         50 * time.Millisecond,
		SlowLatencyFactor:      3.0,
		SlowStddevFactor:       2.0,
		MaxConsecutiveSlow:     3,
		MaxFailuresBeforeAlert: 3,
	}
}

type diskState struct {
	mu sync.Mutex

	// statfs probe
	isChecking       bool
	statFailureCount int
	lastStatErr      error

	// latency tracking
	initialized     bool
	avgLatency      float64
	variance        float64
	consecutiveSlow int

	// IO probe
	isIOChecking   bool
	ioCheckID      uint64
	lastIOCheck    time.Time
	ioFile         *os.File
	ioBuf          []byte
	ioFailureCount int
	lastIOErr      error
}

var diskRegistry sync.Map

const (
	// statfs timeout
	diskTimeout = 500 * time.Millisecond

	// IO probe interval
	ioCheckInterval = 30 * time.Second
)

func NewDiskStatus(path string) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	fillInDiskStatus(disk)
	if disk.PercentUsed > 95 {
		glog.V(0).Infof("disk status: %v", disk)
	}
	return
}

func NewDiskStatusOnStart(path string, config DiskIOProbeConfig) (disk *volume_server_pb.DiskStatus) {
	disk = &volume_server_pb.DiskStatus{Dir: path}
	diskProbe(disk, config)
	if disk.PercentUsed > 95 {
		glog.V(0).Infof("disk status: %v", disk)
	}
	return
}

func diskProbe(disk *volume_server_pb.DiskStatus, config DiskIOProbeConfig) {
	actual, _ := diskRegistry.LoadOrStore(disk.Dir, &diskState{})
	state := actual.(*diskState)

	state.mu.Lock()
	if state.isChecking {
		state.statFailureCount++
		state.lastStatErr = errors.New("statfs still in progress")
		state.updateErrorLocked(disk, config)
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
		err := fillInDiskStatus(probe)
		// Clear the flag only once statfs has actually returned, so a stuck disk
		// keeps a single outstanding probe instead of spawning a new one each tick.
		state.mu.Lock()
		state.isChecking = false
		state.mu.Unlock()
		ch <- err
	}()

	var probeErr error
	select {
	case probeErr = <-ch:
	case <-ctx.Done():
		// Leave isChecking set; the probe goroutine clears it when statfs returns.
		state.mu.Lock()
		state.statFailureCount++
		state.lastStatErr = errors.New("statfs timeout")
		state.updateErrorLocked(disk, config)
		state.mu.Unlock()
		return
	}

	state.mu.Lock()

	if probeErr != nil {
		state.statFailureCount++
		state.lastStatErr = probeErr
		state.updateErrorLocked(disk, config)
		state.mu.Unlock()
		return
	}

	state.statFailureCount = 0
	state.lastStatErr = nil
	disk.All = probe.All
	disk.Free = probe.Free
	disk.Used = probe.Used
	disk.PercentFree = probe.PercentFree
	disk.PercentUsed = probe.PercentUsed

	shouldCheckIO := false
	ioCheckID := uint64(0)

	if config.Enabled && time.Since(state.lastIOCheck) >= ioCheckInterval {
		if state.isIOChecking {
			state.ioFailureCount++
			state.lastIOErr = errors.New("disk io still in progress")
			state.lastIOCheck = time.Now()
		} else {
			state.isIOChecking = true
			state.ioCheckID++
			ioCheckID = state.ioCheckID
			state.lastIOCheck = time.Now()
			shouldCheckIO = true
		}
	}

	state.updateErrorLocked(disk, config)
	state.mu.Unlock()

	if shouldCheckIO {
		checkDiskIOLatency(state, disk, ioCheckID, config)
	}
}

type ioProbeResult struct {
	err     error
	latency time.Duration
}

func checkDiskIOLatency(state *diskState, disk *volume_server_pb.DiskStatus, checkID uint64, config DiskIOProbeConfig) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	ch := make(chan ioProbeResult, 1)

	go func() {
		start := time.Now()
		err := runIOProbe(state, disk.Dir)
		ch <- ioProbeResult{
			err:     err,
			latency: time.Since(start),
		}
	}()

	select {
	case result := <-ch:
		state.mu.Lock()
		defer state.mu.Unlock()

		if state.ioCheckID != checkID {
			return
		}

		state.isIOChecking = false
		state.lastIOCheck = time.Now()

		if result.err != nil {
			state.ioFailureCount++
			state.lastIOErr = result.err
			state.updateErrorLocked(disk, config)
			return
		}

		if state.observeLatencyLocked(result.latency, config) {
			state.ioFailureCount++
			state.lastIOErr = fmt.Errorf(
				"disk io degradation detected latency=%v avg=%v",
				result.latency,
				time.Duration(state.avgLatency),
			)
		} else {
			state.ioFailureCount = 0
			state.lastIOErr = nil
		}

		state.updateErrorLocked(disk, config)

		glog.V(0).Infof(
			"disk io latency dir=%s latency=%v avg=%v stddev=%v",
			disk.Dir,
			result.latency,
			time.Duration(state.avgLatency),
			time.Duration(math.Sqrt(state.variance)),
		)

	case <-ctx.Done():
		state.mu.Lock()

		if state.ioCheckID == checkID {
			state.ioFailureCount++
			state.lastIOErr = errors.New("disk io timeout")
			state.lastIOCheck = time.Now()
			state.updateErrorLocked(disk, config)
		}

		state.mu.Unlock()

		go func() {
			<-ch

			state.mu.Lock()
			if state.ioCheckID == checkID {
				state.isIOChecking = false
				state.lastIOCheck = time.Now()
			}
			state.mu.Unlock()
		}()
	}
}

func runIOProbe(state *diskState, dir string) error {
	state.mu.Lock()
	f := state.ioFile
	buf := state.ioBuf
	state.mu.Unlock()

	if f == nil {
		path := filepath.Join(dir, ".disk-health-check")

		newFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}

		state.mu.Lock()
		if state.ioFile == nil {
			state.ioFile = newFile
			state.ioBuf = make([]byte, 4096)
			f = state.ioFile
			buf = state.ioBuf
			newFile = nil
		} else {
			f = state.ioFile
			buf = state.ioBuf
		}
		state.mu.Unlock()

		if newFile != nil {
			_ = newFile.Close()
		}
	}

	if _, err := f.WriteAt(buf, 0); err != nil {
		return err
	}

	return f.Sync()
}

func (s *diskState) observeLatencyLocked(d time.Duration, c DiskIOProbeConfig) bool {
	x := float64(d)

	if !s.initialized {
		s.avgLatency = x
		s.initialized = true
		return false
	}

	avg := s.avgLatency
	stddev := math.Sqrt(s.variance)

	isSlow := d > c.MinSlowLatency &&
		x > avg*c.SlowLatencyFactor &&
		x > avg+c.SlowStddevFactor*stddev

	if isSlow {
		s.consecutiveSlow++
		return s.consecutiveSlow >= c.MaxConsecutiveSlow
	}

	s.consecutiveSlow = 0

	diff := x - avg
	s.avgLatency = c.LatencyAlpha*x + (1-c.LatencyAlpha)*avg
	s.variance = c.LatencyAlpha*(diff*diff) + (1-c.LatencyAlpha)*s.variance

	return false
}

func (s *diskState) updateErrorLocked(disk *volume_server_pb.DiskStatus, config DiskIOProbeConfig) {
	var problems []string

	if s.statFailureCount >= config.MaxFailuresBeforeAlert && s.lastStatErr != nil {
		problems = append(problems, fmt.Sprintf("statfs: %v", s.lastStatErr))
	}

	if s.ioFailureCount >= config.MaxFailuresBeforeAlert && s.lastIOErr != nil {
		problems = append(problems, fmt.Sprintf("io: %v", s.lastIOErr))
	}

	if len(problems) == 0 {
		disk.Error = ""
		return
	}

	disk.Error = "disk health check failed: " + strings.Join(problems, "; ")
}
