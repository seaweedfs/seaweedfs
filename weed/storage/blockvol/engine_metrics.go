package blockvol

import (
	"sync/atomic"
	"time"
)

// EngineMetrics collects subsystem-level counters for Prometheus export.
// All fields are safe for concurrent access.
type EngineMetrics struct {
	// Flusher
	FlusherBytesTotal   atomic.Uint64
	FlusherFlushesTotal atomic.Uint64
	FlusherErrorsTotal  atomic.Uint64
	flusherLatencyNs    atomicHistogram // flush cycle duration

	// Group Commit
	GroupCommitBatchTotal  atomic.Uint64 // alias for GroupCommitter.syncCount
	groupCommitBatchSize  atomicHistogram
	groupCommitWaitNs     atomicHistogram

	// WAL Shipper
	WALShippedEntriesTotal    atomic.Uint64
	WALBarrierRequestsTotal   atomic.Uint64
	WALFailedBarriersTotal    atomic.Uint64
	walBarrierLatencyNs       atomicHistogram

	// Scrub
	ScrubPassesTotal   atomic.Uint64
	ScrubErrorsTotal   atomic.Uint64
	scrubDurationNs    atomicHistogram

	// WAL Admission
	WALAdmitTotal         atomic.Uint64 // total Acquire calls
	WALAdmitSoftTotal     atomic.Uint64 // soft watermark throttles
	WALAdmitHardTotal     atomic.Uint64 // hard watermark blocks
	WALAdmitTimeoutTotal  atomic.Uint64 // ErrWALFull timeouts
	walAdmitWaitNs        atomicHistogram // wait time in Acquire

	// Durability (CP8-3-1)
	DurabilityBarrierFailedTotal atomic.Uint64 // sync_all barrier failures
	DurabilityQuorumLostTotal    atomic.Uint64 // sync_quorum quorum lost

	// WAL Admission Histogram Observer (CP11A-3)
	// Set by Prometheus layer to feed histogram buckets; nil = no-op.
	WALAdmitWaitObserver func(float64)
}

// NewEngineMetrics creates an EngineMetrics instance.
func NewEngineMetrics() *EngineMetrics {
	return &EngineMetrics{}
}

// RecordFlusherFlush records a completed flush cycle.
func (m *EngineMetrics) RecordFlusherFlush(bytesWritten uint64, dur time.Duration) {
	m.FlusherBytesTotal.Add(bytesWritten)
	m.FlusherFlushesTotal.Add(1)
	m.flusherLatencyNs.record(dur.Nanoseconds())
}

// RecordFlusherError records a flush error.
func (m *EngineMetrics) RecordFlusherError() {
	m.FlusherErrorsTotal.Add(1)
}

// RecordGroupCommitBatch records a group commit batch.
func (m *EngineMetrics) RecordGroupCommitBatch(batchSize int, waitDur time.Duration) {
	m.GroupCommitBatchTotal.Add(1)
	m.groupCommitBatchSize.record(int64(batchSize))
	m.groupCommitWaitNs.record(waitDur.Nanoseconds())
}

// RecordWALShipped records a shipped WAL entry.
func (m *EngineMetrics) RecordWALShipped() {
	m.WALShippedEntriesTotal.Add(1)
}

// RecordWALBarrier records a barrier request with its result.
func (m *EngineMetrics) RecordWALBarrier(dur time.Duration, failed bool) {
	m.WALBarrierRequestsTotal.Add(1)
	m.walBarrierLatencyNs.record(dur.Nanoseconds())
	if failed {
		m.WALFailedBarriersTotal.Add(1)
	}
}

// RecordWALAdmit records a WAL admission Acquire call.
func (m *EngineMetrics) RecordWALAdmit(waitDur time.Duration, soft, hard, timedOut bool) {
	m.WALAdmitTotal.Add(1)
	m.walAdmitWaitNs.record(waitDur.Nanoseconds())
	if m.WALAdmitWaitObserver != nil {
		m.WALAdmitWaitObserver(waitDur.Seconds())
	}
	if soft {
		m.WALAdmitSoftTotal.Add(1)
	}
	if hard {
		m.WALAdmitHardTotal.Add(1)
	}
	if timedOut {
		m.WALAdmitTimeoutTotal.Add(1)
	}
}

// WALAdmitWaitSnapshot returns WAL admission wait stats.
func (m *EngineMetrics) WALAdmitWaitSnapshot() (count uint64, sumNs int64) {
	return m.walAdmitWaitNs.snapshot()
}

// RecordScrubPass records a completed scrub pass.
func (m *EngineMetrics) RecordScrubPass(dur time.Duration, errors int64) {
	m.ScrubPassesTotal.Add(1)
	m.ScrubErrorsTotal.Add(uint64(errors))
	m.scrubDurationNs.record(dur.Nanoseconds())
}

// FlusherLatencySnapshot returns flusher latency stats (count, sum_ns).
func (m *EngineMetrics) FlusherLatencySnapshot() (count uint64, sumNs int64) {
	return m.flusherLatencyNs.snapshot()
}

// GroupCommitBatchSizeSnapshot returns batch size stats.
func (m *EngineMetrics) GroupCommitBatchSizeSnapshot() (count uint64, sum int64) {
	return m.groupCommitBatchSize.snapshot()
}

// GroupCommitWaitSnapshot returns group commit wait time stats.
func (m *EngineMetrics) GroupCommitWaitSnapshot() (count uint64, sumNs int64) {
	return m.groupCommitWaitNs.snapshot()
}

// WALBarrierLatencySnapshot returns barrier latency stats.
func (m *EngineMetrics) WALBarrierLatencySnapshot() (count uint64, sumNs int64) {
	return m.walBarrierLatencyNs.snapshot()
}

// ScrubDurationSnapshot returns scrub duration stats.
func (m *EngineMetrics) ScrubDurationSnapshot() (count uint64, sumNs int64) {
	return m.scrubDurationNs.snapshot()
}

// atomicHistogram is a minimal lock-free histogram that tracks count and sum.
// Prometheus histograms are registered in the iscsi-target metrics layer;
// this struct provides the raw observations for GaugeFunc export.
type atomicHistogram struct {
	count atomic.Uint64
	sum   atomic.Int64
}

func (h *atomicHistogram) record(val int64) {
	h.count.Add(1)
	h.sum.Add(val)
}

func (h *atomicHistogram) snapshot() (uint64, int64) {
	return h.count.Load(), h.sum.Load()
}
