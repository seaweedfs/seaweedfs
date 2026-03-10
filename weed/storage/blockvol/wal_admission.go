package blockvol

import (
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
}

// WALAdmissionConfig holds parameters for WALAdmission construction.
type WALAdmissionConfig struct {
	MaxConcurrent int            // max concurrent writers (semaphore size)
	SoftWatermark float64        // WAL fraction above which writes throttle
	HardWatermark float64        // WAL fraction above which writes block
	WALUsedFn     func() float64 // returns WAL used fraction
	NotifyFn      func()         // wake flusher on pressure
	ClosedFn      func() bool    // check if volume is closed
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
	}
}

// Acquire blocks until a write slot is available or the deadline expires.
// The timeout covers both the watermark wait and semaphore acquisition.
// Returns ErrWALFull on timeout, ErrVolumeClosed if the volume closes.
func (a *WALAdmission) Acquire(timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	pressure := a.walUsed()

	// Hard watermark gate: wait for flusher to drain before competing for semaphore.
	if pressure >= a.hardMark {
		a.notifyFn()
		for a.walUsed() >= a.hardMark {
			if a.closedFn() {
				return ErrVolumeClosed
			}
			a.notifyFn()
			select {
			case <-deadline.C:
				return ErrWALFull
			default:
			}
			a.sleepFn(2 * time.Millisecond)
		}
		// Pressure dropped — fall through to semaphore acquisition.
	} else if pressure >= a.softMark {
		// Soft watermark: small delay to desynchronize herd.
		a.notifyFn()
		scale := (pressure - a.softMark) / (a.hardMark - a.softMark)
		if scale > 1 {
			scale = 1
		}
		// Scale: softMark→0ms, hardMark→5ms.
		delay := time.Duration(scale * 5 * float64(time.Millisecond))
		if delay > 0 {
			a.sleepFn(delay)
		}
	}

	// Acquire semaphore slot using the same deadline.
	select {
	case a.sem <- struct{}{}:
		return nil
	default:
	}
	// Semaphore full — wait with remaining budget, also check close.
	closeTick := time.NewTicker(5 * time.Millisecond)
	defer closeTick.Stop()
	for {
		select {
		case a.sem <- struct{}{}:
			return nil
		case <-deadline.C:
			return ErrWALFull
		case <-closeTick.C:
			if a.closedFn() {
				return ErrVolumeClosed
			}
		}
	}
}

// Release returns a write slot to the semaphore.
func (a *WALAdmission) Release() {
	<-a.sem
}
