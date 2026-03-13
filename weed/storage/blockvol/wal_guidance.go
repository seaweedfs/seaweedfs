package blockvol

import "fmt"

// Workload hint constants for WAL sizing guidance.
const (
	WorkloadGeneral    = "general"
	WorkloadDatabase   = "database"
	WorkloadThroughput = "throughput"
)

// WAL sizing thresholds. Each minimum is documented with the reasoning.
const (
	// minWALGeneral: 32MB ≈ 0.5s of sustained 64K writes at QD=16.
	// Adequate for mixed workloads with moderate write bursts.
	minWALGeneral = 32 << 20

	// minWALDatabase: 128MB ≈ 2s of sustained 64K writes at QD=32.
	// Databases issue many small fsyncs; WAL must absorb bursts
	// between flusher checkpoints without hitting hard watermark.
	minWALDatabase = 128 << 20

	// minWALThroughput: 128MB ≈ 2s of sustained 64K writes at QD=32.
	// Large sequential writes fill WAL fast; admission control still
	// applies but larger WAL reduces soft-zone throttle frequency.
	minWALThroughput = 128 << 20

	// minWALEntries: WAL must hold at least 64 max-size entries
	// to avoid immediate ErrWALFull under any access pattern.
	minWALEntries = 64
)

// WALGuidanceResult holds the result of a WAL sizing or preflight evaluation.
type WALGuidanceResult struct {
	Level    string   // "ok" or "warn"
	Warnings []string // empty when Level is "ok"
}

// WALSizingGuidance evaluates whether a WAL size is adequate for the given
// workload hint and block size. Pure function with no side effects.
func WALSizingGuidance(walSize, blockSize uint64, workloadHint string) WALGuidanceResult {
	r := WALGuidanceResult{Level: "ok"}

	// Check workload-specific minimum.
	var minWAL uint64
	switch workloadHint {
	case WorkloadGeneral:
		minWAL = minWALGeneral
	case WorkloadDatabase:
		minWAL = minWALDatabase
	case WorkloadThroughput:
		minWAL = minWALThroughput
	default:
		// Unknown hint: advisory note, not an error.
		r.Warnings = append(r.Warnings, fmt.Sprintf("unknown workload hint %q; using general minimum", workloadHint))
		minWAL = minWALGeneral
	}

	if walSize < minWAL {
		r.Level = "warn"
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"WAL size %d bytes is below recommended minimum %d for workload %q",
			walSize, minWAL, workloadHint))
	}

	// Check absolute minimum: WAL must hold at least minWALEntries blocks.
	absMin := blockSize * minWALEntries
	if walSize < absMin {
		r.Level = "warn"
		r.Warnings = append(r.Warnings, fmt.Sprintf(
			"WAL size %d bytes is below absolute minimum %d (%d × blockSize %d)",
			walSize, absMin, minWALEntries, blockSize))
	}

	return r
}

// EvaluateWALConfig runs preflight checks on WAL configuration parameters.
// Takes narrow inputs for minimal coupling. Returns aggregated guidance.
func EvaluateWALConfig(walSize, blockSize uint64, maxConcurrent int, workloadHint string) WALGuidanceResult {
	r := WALSizingGuidance(walSize, blockSize, workloadHint)

	// Check concurrency vs WAL size ratio.
	// Heuristic: each concurrent writer may produce up to one extent (blockSize)
	// in the WAL. If the WAL cannot hold 4× the concurrent capacity, warn.
	if maxConcurrent > 0 {
		concurrentCapacity := uint64(maxConcurrent) * blockSize * 4
		if walSize < concurrentCapacity {
			r.Level = "warn"
			r.Warnings = append(r.Warnings, fmt.Sprintf(
				"WAL size %d bytes is small relative to maxConcurrent=%d (need ≥ %d for headroom)",
				walSize, maxConcurrent, concurrentCapacity))
		}
	}

	return r
}
