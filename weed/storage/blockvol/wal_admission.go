package blockvol

import (
	"sync/atomic"
	"time"
)

// WALAdmission controls write admission based on WAL pressure watermarks.
// It limits concurrent writers via a counting semaphore and gates new
// admission when WAL usage exceeds configurable thresholds.
//
// Watermark behavior:
//   - below soft watermark: writes pass through immediately
//   - between soft and hard: writes are admitted with a small delay to
//     desynchronize concurrent writers and give the flusher time to drain
//   - above hard watermark: new writes are blocked until pressure drops
//     below the hard watermark or the timeout expires
//
// A single deadline governs the entire Acquire call. Time spent waiting
// for the hard watermark to clear reduces the budget available for
// semaphore acquisition.
type WALAdmission struct {
	sem      chan struct{}   // counting semaphore for concurrent WAL appenders
	walUsed  func() float64 // returns WAL used fraction 0.0–1.0
	notifyFn func()         // wakes flusher
	softMark float64        // begin throttling
	hardMark float64        // block admission
	closedFn func() bool    // returns true if volume is closed

	// sleepFn is the sleep function. Replaced in tests for determinism.
	sleepFn func(time.Duration)

	metrics *EngineMetrics // optional; if nil, no metrics recorded

	// Pressure wait tracking (CP11A-3): cumulative ns writers spent waiting.
	softPressureWaitNs atomic.Int64
	hardPressureWaitNs atomic.Int64
}

// WALAdmissionConfig holds parameters for WALAdmission construction.
type WALAdmissionConfig struct {
	MaxConcurrent int            // max concurrent writers (semaphore size)
	SoftWatermark float64        // WAL fraction above which writes throttle
	HardWatermark float64        // WAL fraction above which writes block
	WALUsedFn     func() float64 // returns WAL used fraction
	NotifyFn      func()         // wake flusher on pressure
	ClosedFn      func() bool    // check if volume is closed
	Metrics       *EngineMetrics // optional; if nil, no metrics recorded
}

// NewWALAdmission creates a WAL admission controller.
func NewWALAdmission(cfg WALAdmissionConfig) *WALAdmission {
	return &WALAdmission{
		sem:      make(chan struct{}, cfg.MaxConcurrent),
		walUsed:  cfg.WALUsedFn,
		notifyFn: cfg.NotifyFn,
		softMark: cfg.SoftWatermark,
		hardMark: cfg.HardWatermark,
		closedFn: cfg.ClosedFn,
		sleepFn:  time.Sleep,
		metrics:  cfg.Metrics,
	}
}

// PressureState returns the current WAL pressure state:
// "hard" if usage >= hard watermark, "soft" if >= soft watermark, "normal" otherwise.
func (a *WALAdmission) PressureState() string {
	used := a.walUsed()
	if used >= a.hardMark {
		return "hard"
	}
	if used >= a.softMark {
		return "soft"
	}
	return "normal"
}

// SoftPressureWaitNs returns cumulative nanoseconds writers spent waiting in the soft pressure zone.
func (a *WALAdmission) SoftPressureWaitNs() int64 { return a.softPressureWaitNs.Load() }

// HardPressureWaitNs returns cumulative nanoseconds writers spent waiting in the hard pressure zone.
func (a *WALAdmission) HardPressureWaitNs() int64 { return a.hardPressureWaitNs.Load() }

// SoftMark returns the configured soft watermark threshold.
func (a *WALAdmission) SoftMark() float64 { return a.softMark }

// HardMark returns the configured hard watermark threshold.
func (a *WALAdmission) HardMark() float64 { return a.hardMark }

// Acquire blocks until a write slot is available or the deadline expires.
// The timeout covers both the watermark wait and semaphore acquisition.
// Returns ErrWALFull on timeout, ErrVolumeClosed if the volume closes.
func (a *WALAdmission) Acquire(timeout time.Duration) error {
	start := time.Now()
	var hitSoft, hitHard bool

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	pressure := a.walUsed()

	// Hard watermark gate: wait for flusher to drain before competing for semaphore.
	if pressure >= a.hardMark {
		hitHard = true
		a.notifyFn()
		hardStart := time.Now()
		for a.walUsed() >= a.hardMark {
			if a.closedFn() {
				a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
				a.recordAdmit(start, hitSoft, hitHard, false)
				return ErrVolumeClosed
			}
			a.notifyFn()
			select {
			case <-deadline.C:
				a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
				a.recordAdmit(start, hitSoft, hitHard, true)
				return ErrWALFull
			default:
			}
			a.sleepFn(2 * time.Millisecond)
		}
		a.hardPressureWaitNs.Add(time.Since(hardStart).Nanoseconds())
		// Pressure dropped — fall through to semaphore acquisition.
	} else if pressure >= a.softMark {
		// Soft watermark: small delay to desynchronize herd.
		hitSoft = true
		a.notifyFn()
		scale := (pressure - a.softMark) / (a.hardMark - a.softMark)
		if scale > 1 {
			scale = 1
		}
		// Scale: softMark→0ms, hardMark→5ms.
		delay := time.Duration(scale * 5 * float64(time.Millisecond))
		if delay > 0 {
			softStart := time.Now()
			a.sleepFn(delay)
			a.softPressureWaitNs.Add(time.Since(softStart).Nanoseconds())
		}
	}

	// Acquire semaphore slot using the same deadline.
	select {
	case a.sem <- struct{}{}:
		a.recordAdmit(start, hitSoft, hitHard, false)
		return nil
	default:
	}
	// Semaphore full — wait with remaining budget, also check close.
	closeTick := time.NewTicker(5 * time.Millisecond)
	defer closeTick.Stop()
	for {
		select {
		case a.sem <- struct{}{}:
			a.recordAdmit(start, hitSoft, hitHard, false)
			return nil
		case <-deadline.C:
			a.recordAdmit(start, hitSoft, hitHard, true)
			return ErrWALFull
		case <-closeTick.C:
			if a.closedFn() {
				a.recordAdmit(start, hitSoft, hitHard, false)
				return ErrVolumeClosed
			}
		}
	}
}

func (a *WALAdmission) recordAdmit(start time.Time, soft, hard, timedOut bool) {
	if a.metrics != nil {
		a.metrics.RecordWALAdmit(time.Since(start), soft, hard, timedOut)
	}
}

// Release returns a write slot to the semaphore.
func (a *WALAdmission) Release() {
	<-a.sem
}
