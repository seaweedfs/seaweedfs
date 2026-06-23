package stats

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

type DiskIOProbeConfig struct {
	// enable/disable disk io probing
	Enabled bool

	// timeout for a single io probe
	Timeout time.Duration

	// probe interval
	Interval time.Duration

	// latency above this threshold is considered slow
	SlowLatency time.Duration

	// rolling observation window
	Window time.Duration

	// minimum number of samples required before evaluating health
	MinSamples int

	// percentage of slow operations required to mark disk degraded
	SlowPercent float64

	// percentage of failed operations required to mark disk degraded
	ErrorPercent float64

	// recovery coef defines the hysteresis threshold for recovering from degraded state.
	RecoveryCoef float64

	// statfs failures before alerting
	MaxStatFailures int
}

func DefaultDiskIOProbeConfig() DiskIOProbeConfig {
	return DiskIOProbeConfig{
		Enabled:     false,
		Timeout:     2 * time.Second,
		Interval:    30 * time.Second,
		SlowLatency: 100 * time.Millisecond,

		Window:     time.Minute,
		MinSamples: 10,

		SlowPercent:  20,
		ErrorPercent: 10,
		RecoveryCoef: 0.5,

		MaxStatFailures: 3,
	}
}

type ioSample struct {
	ts      time.Time
	latency time.Duration
	failed  bool
	slow    bool
}

type diskState struct {
	mu sync.Mutex

	// statfs probe
	isChecking       bool
	statFailureCount int
	statSuccessCount int
	lastStatErr      error
	lastGoodStatus   volume_server_pb.DiskStatus
	hasLastGood      bool

	samples []ioSample

	// IO probe
	isIOChecking   bool
	ioCheckID      uint64
	lastIOCheck    time.Time
	ioFailureCount int
	lastIOErr      error
	ioDegraded     bool
}

var diskRegistry sync.Map

const (
	// statfs timeout
	diskTimeout = 500 * time.Millisecond
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
		state.statSuccessCount = 0
		state.lastStatErr = errors.New("statfs still in progress")
		state.updateErrorLocked(disk, config)
		state.applyLastGoodStatusLocked(disk)
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
		state.statSuccessCount = 0
		state.lastStatErr = errors.New("statfs timeout")
		state.updateErrorLocked(disk, config)
		state.applyLastGoodStatusLocked(disk)
		state.mu.Unlock()
		return
	}

	state.mu.Lock()

	if probeErr != nil {
		state.statFailureCount++
		state.statSuccessCount = 0
		state.lastStatErr = probeErr
		state.updateErrorLocked(disk, config)
		state.applyLastGoodStatusLocked(disk)
		state.mu.Unlock()
		return
	}

	state.observeStatSuccessLocked(config)
	disk.All = probe.All
	disk.Free = probe.Free
	disk.Used = probe.Used
	disk.PercentFree = probe.PercentFree
	disk.PercentUsed = probe.PercentUsed
	state.rememberGoodStatusLocked(disk)

	if !config.Enabled {
		state.ioFailureCount = 0
		state.lastIOErr = nil
		state.ioDegraded = false
		state.updateErrorLocked(disk, config)
		state.mu.Unlock()
		return
	}

	shouldCheckIO := false
	ioCheckID := uint64(0)

	if time.Since(state.lastIOCheck) >= config.Interval {
		if state.isIOChecking {
			degraded := state.observeLatencyLocked(
				config.Timeout,
				true,
				config,
			)

			state.ioDegraded = degraded
			state.lastIOCheck = time.Now()

			if degraded {
				state.ioFailureCount++
				state.lastIOErr = errors.New("disk io still in progress")
			} else {
				state.ioFailureCount = 0
				state.lastIOErr = nil
			}
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
		err := runIOProbe(disk.Dir)
		ch <- ioProbeResult{
			err:     err,
			latency: time.Since(start),
		}

		state.mu.Lock()
		if state.ioCheckID == checkID {
			state.isIOChecking = false
			state.lastIOCheck = time.Now()
		}
		state.mu.Unlock()
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
			degraded := state.observeLatencyLocked(
				config.Timeout,
				true,
				config,
			)

			state.ioDegraded = degraded

			if degraded {
				state.ioFailureCount++
				state.lastIOErr = result.err
			} else {
				state.ioFailureCount = 0
				state.lastIOErr = nil
			}

			state.updateErrorLocked(disk, config)
			return
		}

		degraded := state.observeLatencyLocked(
			result.latency,
			false,
			config,
		)

		state.ioDegraded = degraded

		if degraded {
			state.ioFailureCount++
			state.lastIOErr = fmt.Errorf(
				"disk io degradation detected latency=%v",
				result.latency,
			)
		} else {
			state.ioFailureCount = 0
			state.lastIOErr = nil
		}

		state.updateErrorLocked(disk, config)

		glog.V(1).Infof(
			"disk io latency dir=%s latency=%v ioFailureCount=%d degraded=%v samples=%d error=%v",
			disk.Dir,
			result.latency,
			state.ioFailureCount,
			degraded,
			len(state.samples),
			state.lastIOErr,
		)

	case <-ctx.Done():
		state.mu.Lock()

		if state.ioCheckID == checkID {
			degraded := state.observeLatencyLocked(
				config.Timeout,
				true,
				config,
			)

			state.ioDegraded = degraded

			if degraded {
				state.ioFailureCount++
				state.lastIOErr = errors.New("disk io timeout")
			} else {
				state.ioFailureCount = 0
				state.lastIOErr = nil
			}

			state.lastIOCheck = time.Now()
			state.updateErrorLocked(disk, config)
		}

		state.mu.Unlock()
	}
}

func runIOProbe(dir string) error {
	path := filepath.Join(dir, ".disk-health-check")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteAt(make([]byte, 4096), 0); err != nil {
		return err
	}

	return f.Sync()
}

func (s *diskState) observeLatencyLocked(latency time.Duration, failed bool, config DiskIOProbeConfig) bool {
	now := time.Now()

	sample := ioSample{
		ts:      now,
		latency: latency,
		failed:  failed,
		slow:    latency >= config.SlowLatency,
	}

	s.samples = append(s.samples, sample)

	cutoff := now.Add(-config.Window)

	idx := 0
	for _, sample := range s.samples {
		if sample.ts.After(cutoff) {
			s.samples[idx] = sample
			idx++
		}
	}

	s.samples = s.samples[:idx]

	total := len(s.samples)

	if total < config.MinSamples {
		return s.ioDegraded
	}

	var slowCount int
	var failedCount int

	for _, sample := range s.samples {
		if sample.slow {
			slowCount++
		}

		if sample.failed {
			failedCount++
		}
	}

	slowPercent := float64(slowCount) / float64(total) * 100
	errorPercent := float64(failedCount) / float64(total) * 100

	if !s.ioDegraded {
		return slowPercent >= config.SlowPercent ||
			errorPercent >= config.ErrorPercent
	}

	recoverSlowPercent := config.SlowPercent * config.RecoveryCoef
	recoverErrorPercent := config.ErrorPercent * config.RecoveryCoef

	return slowPercent > recoverSlowPercent ||
		errorPercent > recoverErrorPercent
}

func (s *diskState) observeStatSuccessLocked(config DiskIOProbeConfig) {
	if s.statFailureCount < config.MaxStatFailures {
		s.statFailureCount = 0
		s.statSuccessCount = 0
		s.lastStatErr = nil
		return
	}

	s.statSuccessCount++
	if s.statSuccessCount >= statRecoverySuccesses(config) {
		s.statFailureCount = 0
		s.statSuccessCount = 0
		s.lastStatErr = nil
	}
}

func statRecoverySuccesses(config DiskIOProbeConfig) int {
	if config.MaxStatFailures <= 1 {
		return 1
	}
	if config.RecoveryCoef <= 0 {
		return config.MaxStatFailures * 2
	}

	successes := int(float64(config.MaxStatFailures) * (1/config.RecoveryCoef - 1))
	if successes < 1 {
		return 1
	}
	return successes
}

func (s *diskState) updateErrorLocked(disk *volume_server_pb.DiskStatus, config DiskIOProbeConfig) {
	var problems []string

	if s.statFailureCount >= config.MaxStatFailures &&
		s.lastStatErr != nil {
		problems = append(
			problems,
			fmt.Sprintf("statfs: %v", s.lastStatErr),
		)
	}

	if config.Enabled && s.ioDegraded && s.lastIOErr != nil {
		problems = append(
			problems,
			fmt.Sprintf("io: %v", s.lastIOErr),
		)
	}

	if len(problems) == 0 {
		disk.Error = ""
		return
	}

	disk.Error =
		"disk health check failed: " +
			strings.Join(problems, "; ")
}

func (s *diskState) rememberGoodStatusLocked(disk *volume_server_pb.DiskStatus) {
	s.lastGoodStatus = *disk
	s.hasLastGood = true
}

func (s *diskState) applyLastGoodStatusLocked(disk *volume_server_pb.DiskStatus) {
	if !s.hasLastGood || disk.Error != "" {
		return
	}

	dir := disk.Dir
	errorMessage := disk.Error
	*disk = s.lastGoodStatus
	disk.Dir = dir
	disk.Error = errorMessage
}
