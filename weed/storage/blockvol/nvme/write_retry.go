package nvme

import (
	"errors"
	"math/rand"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// WALPressureProvider extends BlockDevice with WAL pressure reporting.
type WALPressureProvider interface {
	WALPressure() float64 // 0.0 = empty, 1.0 = full
}

// isRetryableWALPressure returns true if the error represents transient
// WAL pressure that may clear with a short retry.
func isRetryableWALPressure(err error) bool {
	return err != nil && errors.Is(err, blockvol.ErrWALFull)
}

// writeRetryBackoffs defines the backoff schedule for writeWithRetry.
var writeRetryBackoffs = [3]time.Duration{
	50 * time.Millisecond,
	200 * time.Millisecond,
	800 * time.Millisecond,
}

// sleepFn is the sleep function used by retry/throttle helpers.
// Replaced in tests for deterministic behavior.
var sleepFn = time.Sleep

// jitterFn returns a jitter duration given a max value.
// Replaced in tests for deterministic behavior.
var jitterFn = func(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

// writeWithRetry wraps dev.WriteAt with target-side retry on WAL pressure.
// Non-WAL errors return immediately. On WAL pressure, retries with jittered
// backoff before giving up. Returns the last error unchanged so mapBlockError
// preserves DNR=0 semantics.
func writeWithRetry(dev BlockDevice, lba uint64, data []byte) error {
	err := dev.WriteAt(lba, data)
	if err == nil || !isRetryableWALPressure(err) {
		return err
	}

	for _, backoff := range writeRetryBackoffs {
		jitter := jitterFn(backoff / 4)
		sleepFn(backoff + jitter)
		err = dev.WriteAt(lba, data)
		if err == nil || !isRetryableWALPressure(err) {
			return err
		}
	}
	return err
}

// throttleOnWALPressure inserts a small delay when WAL pressure is high,
// desynchronizing concurrent writers to reduce thundering-herd retry storms.
// No-op if the device does not implement WALPressureProvider.
func throttleOnWALPressure(dev BlockDevice) {
	prov, ok := dev.(WALPressureProvider)
	if !ok {
		return
	}
	p := prov.WALPressure()
	if p < 0.9 {
		return
	}
	// Scale: 0.9→1ms, 0.95→3ms, 1.0→5ms
	ms := (p - 0.9) * 50
	if ms > 0 {
		sleepFn(time.Duration(ms * float64(time.Millisecond)))
	}
}
