package weed_server

import (
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 12 P4: Performance Floor — Bounded Measurement Package
//
// Workload envelope:
//   Topology: RF=2 sync_all accepted chosen path
//   Operations: 4K random write, 4K random read, 4K sequential write, 4K sequential read
//   Runtime: no failover, no disturbance, steady-state
//   Environment: unit test harness (single-process, local disk, engine-local I/O)
//
// What this measures:
//   Engine I/O floor for the accepted chosen path. WriteLBA/ReadLBA through
//   the full fencing path (epoch, role, lease, writeGate, WAL, dirtyMap).
//   No transport layer (iSCSI/NVMe). No cross-machine replication.
//
// What this does NOT measure:
//   Transport throughput, cross-machine replication tax, multi-client concurrency,
//   failover-under-load, degraded mode. Production floor with replication is
//   documented in baseline-roce-20260401.md.
//
// NOT performance tuning. NOT broad benchmark.
// ============================================================

const (
	perfBlockSize  = 4096
	perfVolumeSize = 64 * 1024 * 1024 // 64MB
	perfWALSize    = 16 * 1024 * 1024  // 16MB
	perfOps        = 1000              // ops per measurement run
	perfWarmupOps  = 200               // warmup ops (discarded from measurement)
	perfIterations = 3                 // run N times, report worst as floor
)

// Minimum acceptable floor thresholds (engine-local, single-writer).
//
// These are regression gates, not performance targets. Set conservatively
// so any reasonable hardware passes, but catastrophic regressions
// (accidental serialization, O(n^2) scan, broken WAL path) are caught.
//
// Rationale for values:
//   Measured on dev SSD: rand-write ~10K, rand-read ~80K, seq-write ~30K, seq-read ~180K.
//   Thresholds set at ~10% of measured to tolerate slow CI machines and VMs.
//   Write P99 ceiling at 100ms catches deadlocks/stalls without false-positiving
//   on slow storage.
var perfFloorGates = map[string]struct {
	MinIOPS     float64
	MaxWriteP99 time.Duration // 0 = no ceiling (reads)
}{
	"rand-write": {MinIOPS: 1000, MaxWriteP99: 100 * time.Millisecond},
	"rand-read":  {MinIOPS: 5000},
	"seq-write":  {MinIOPS: 2000, MaxWriteP99: 100 * time.Millisecond},
	"seq-read":   {MinIOPS: 10000},
}

// perfResult holds measurements for one workload run.
type perfResult struct {
	Workload   string
	Ops        int
	Elapsed    time.Duration
	IOPS       float64
	MBps       float64
	LatSamples []int64 // per-op latency in nanoseconds
}

func (r *perfResult) latPct(pct float64) time.Duration {
	if len(r.LatSamples) == 0 {
		return 0
	}
	sorted := make([]int64, len(r.LatSamples))
	copy(sorted, r.LatSamples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(pct/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return time.Duration(sorted[idx])
}

func (r *perfResult) latAvg() time.Duration {
	if len(r.LatSamples) == 0 {
		return 0
	}
	var sum int64
	for _, s := range r.LatSamples {
		sum += s
	}
	return time.Duration(sum / int64(len(r.LatSamples)))
}

// setupPerfVolume creates a BlockVol configured as Primary for perf measurement.
func setupPerfVolume(t *testing.T) *blockvol.BlockVol {
	t.Helper()
	dir := t.TempDir()
	volPath := filepath.Join(dir, "perf.blk")
	vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
		VolumeSize: perfVolumeSize,
		BlockSize:  perfBlockSize,
		WALSize:    perfWALSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Set up as Primary with long lease so writes are allowed.
	if err := vol.HandleAssignment(1, blockvol.RolePrimary, 10*time.Minute); err != nil {
		vol.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { vol.Close() })
	return vol
}

// maxLBAs returns the number of addressable 4K blocks in the extent area.
func maxLBAs() uint64 {
	// Volume size minus WAL, divided by block size, with safety margin.
	return (perfVolumeSize - perfWALSize) / perfBlockSize / 2
}

// runPerfWorkload executes one workload measurement and returns the result.
func runPerfWorkload(t *testing.T, vol *blockvol.BlockVol, workload string, ops int) perfResult {
	t.Helper()
	data := make([]byte, perfBlockSize)
	rand.Read(data)
	max := maxLBAs()

	samples := make([]int64, 0, ops)
	start := time.Now()

	for i := 0; i < ops; i++ {
		var lba uint64
		switch {
		case strings.HasPrefix(workload, "rand"):
			lba = uint64(mrand.Int63n(int64(max)))
		default: // sequential
			lba = uint64(i) % max
		}

		opStart := time.Now()
		switch {
		case strings.HasSuffix(workload, "write"):
			if err := vol.WriteLBA(lba, data); err != nil {
				t.Fatalf("%s op %d: WriteLBA(%d): %v", workload, i, lba, err)
			}
		case strings.HasSuffix(workload, "read"):
			if _, err := vol.ReadLBA(lba, perfBlockSize); err != nil {
				t.Fatalf("%s op %d: ReadLBA(%d): %v", workload, i, lba, err)
			}
		}
		samples = append(samples, time.Since(opStart).Nanoseconds())
	}

	elapsed := time.Since(start)
	iops := float64(ops) / elapsed.Seconds()
	mbps := iops * float64(perfBlockSize) / (1024 * 1024)

	return perfResult{
		Workload:   workload,
		Ops:        ops,
		Elapsed:    elapsed,
		IOPS:       iops,
		MBps:       mbps,
		LatSamples: samples,
	}
}

// floorOf returns the worst (lowest) IOPS and worst (highest) P99 across iterations.
type perfFloor struct {
	Workload string
	FloorIOPS float64
	FloorMBps float64
	WorstAvg  time.Duration
	WorstP50  time.Duration
	WorstP99  time.Duration
	WorstMax  time.Duration
}

func computeFloor(results []perfResult) perfFloor {
	f := perfFloor{
		Workload:  results[0].Workload,
		FloorIOPS: math.MaxFloat64,
		FloorMBps: math.MaxFloat64,
	}
	for _, r := range results {
		if r.IOPS < f.FloorIOPS {
			f.FloorIOPS = r.IOPS
		}
		if r.MBps < f.FloorMBps {
			f.FloorMBps = r.MBps
		}
		avg := r.latAvg()
		if avg > f.WorstAvg {
			f.WorstAvg = avg
		}
		p50 := r.latPct(50)
		if p50 > f.WorstP50 {
			f.WorstP50 = p50
		}
		p99 := r.latPct(99)
		if p99 > f.WorstP99 {
			f.WorstP99 = p99
		}
		pmax := r.latPct(100)
		if pmax > f.WorstMax {
			f.WorstMax = pmax
		}
	}
	return f
}

// --- Test 1: PerformanceFloor_Bounded ---

func TestP12P4_PerformanceFloor_Bounded(t *testing.T) {
	vol := setupPerfVolume(t)

	workloads := []string{"rand-write", "rand-read", "seq-write", "seq-read"}
	floors := make([]perfFloor, 0, len(workloads))

	for _, wl := range workloads {
		// Warmup: populate volume with data (needed for reads).
		if strings.HasSuffix(wl, "read") {
			warmupData := make([]byte, perfBlockSize)
			rand.Read(warmupData)
			for i := 0; i < int(maxLBAs()); i++ {
				if err := vol.WriteLBA(uint64(i), warmupData); err != nil {
					break // WAL full is acceptable during warmup
				}
			}
			time.Sleep(200 * time.Millisecond) // let flusher drain
		}

		// Warmup ops (discarded).
		runPerfWorkload(t, vol, wl, perfWarmupOps)

		// Measurement: N iterations, take floor.
		var results []perfResult
		for iter := 0; iter < perfIterations; iter++ {
			r := runPerfWorkload(t, vol, wl, perfOps)
			results = append(results, r)
		}

		floor := computeFloor(results)
		floors = append(floors, floor)
	}

	// Report structured floor table.
	t.Log("")
	t.Log("=== P12P4 Performance Floor (engine-local, single-writer) ===")
	t.Log("")
	t.Logf("%-12s  %10s  %8s  %10s  %10s  %10s  %10s",
		"Workload", "Floor IOPS", "MB/s", "Avg Lat", "P50 Lat", "P99 Lat", "Max Lat")
	t.Logf("%-12s  %10s  %8s  %10s  %10s  %10s  %10s",
		"--------", "----------", "------", "-------", "-------", "-------", "-------")
	for _, f := range floors {
		t.Logf("%-12s  %10.0f  %8.2f  %10s  %10s  %10s  %10s",
			f.Workload, f.FloorIOPS, f.FloorMBps, f.WorstAvg, f.WorstP50, f.WorstP99, f.WorstMax)
	}
	t.Log("")
	t.Logf("Config: volume=%dMB WAL=%dMB block=%dB ops=%d warmup=%d iterations=%d",
		perfVolumeSize/(1024*1024), perfWALSize/(1024*1024), perfBlockSize, perfOps, perfWarmupOps, perfIterations)
	t.Log("Method: worst of N iterations (floor, not peak)")
	t.Log("Scope: engine-local only; production RF=2 floor in baseline-roce-20260401.md")

	// Gate: floor values must meet minimum acceptable thresholds.
	// These are regression gates — if any floor drops below the gate,
	// the test fails, blocking rollout.
	t.Log("")
	t.Log("=== Floor Gate Validation ===")
	allGatesPassed := true
	for _, f := range floors {
		gate, ok := perfFloorGates[f.Workload]
		if !ok {
			t.Fatalf("no floor gate defined for workload %s", f.Workload)
		}
		passed := true
		if f.FloorIOPS < gate.MinIOPS {
			t.Errorf("GATE FAIL: %s floor IOPS %.0f < minimum %.0f", f.Workload, f.FloorIOPS, gate.MinIOPS)
			passed = false
		}
		if gate.MaxWriteP99 > 0 && f.WorstP99 > gate.MaxWriteP99 {
			t.Errorf("GATE FAIL: %s worst P99 %s > ceiling %s", f.Workload, f.WorstP99, gate.MaxWriteP99)
			passed = false
		}
		status := "PASS"
		if !passed {
			status = "FAIL"
			allGatesPassed = false
		}
		t.Logf("  %-12s  min=%6.0f IOPS  →  floor=%6.0f  [%s]", f.Workload, gate.MinIOPS, f.FloorIOPS, status)
	}

	if !allGatesPassed {
		t.Fatal("P12P4 PerformanceFloor: FAIL — one or more floor gates not met")
	}
	t.Log("P12P4 PerformanceFloor: PASS — all floor gates met")
}

// --- Test 2: CostCharacterization_Bounded ---

func TestP12P4_CostCharacterization_Bounded(t *testing.T) {
	vol := setupPerfVolume(t)

	// Measure write latency breakdown: WriteLBA includes WAL append + group commit.
	data := make([]byte, perfBlockSize)
	rand.Read(data)
	max := maxLBAs()

	const costOps = 500
	var writeLatSum int64
	for i := 0; i < costOps; i++ {
		lba := uint64(mrand.Int63n(int64(max)))
		start := time.Now()
		if err := vol.WriteLBA(lba, data); err != nil {
			t.Fatalf("write op %d: %v", i, err)
		}
		writeLatSum += time.Since(start).Nanoseconds()
	}
	avgWriteLat := time.Duration(writeLatSum / costOps)

	// Measure read latency for comparison.
	// Populate first.
	for i := 0; i < int(max/2); i++ {
		vol.WriteLBA(uint64(i), data)
	}
	time.Sleep(200 * time.Millisecond) // let flusher drain

	var readLatSum int64
	for i := 0; i < costOps; i++ {
		lba := uint64(mrand.Int63n(int64(max / 2)))
		start := time.Now()
		if _, err := vol.ReadLBA(lba, perfBlockSize); err != nil {
			t.Fatalf("read op %d: %v", i, err)
		}
		readLatSum += time.Since(start).Nanoseconds()
	}
	avgReadLat := time.Duration(readLatSum / costOps)

	// Cost statement.
	t.Log("")
	t.Log("=== P12P4 Cost Characterization (engine-local) ===")
	t.Log("")
	t.Logf("Average write latency: %s (includes WAL append + group commit sync)", avgWriteLat)
	t.Logf("Average read latency:  %s (dirtyMap lookup + WAL/extent read)", avgReadLat)
	t.Log("")
	t.Log("Bounded cost statement:")
	t.Log("  WAL write amplification: 2x minimum (WAL write + eventual extent flush)")
	t.Log("  Group commit: amortizes fdatasync across batched writers (1 sync per batch)")
	t.Log("  Replication tax (production RF=2 sync_all): -56% vs RF=1 (barrier round-trip)")
	t.Log("  Replication tax source: baseline-roce-20260401.md, measured on 25Gbps RoCE")
	t.Log("")
	t.Logf("Write/read ratio: %.1fx (write is %.1fx slower than read)",
		float64(avgWriteLat)/float64(avgReadLat),
		float64(avgWriteLat)/float64(avgReadLat))
	t.Log("")
	t.Logf("Config: volume=%dMB WAL=%dMB block=%dB ops=%d",
		perfVolumeSize/(1024*1024), perfWALSize/(1024*1024), perfBlockSize, costOps)

	// Proof: cost values are finite and positive.
	if avgWriteLat <= 0 || avgReadLat <= 0 {
		t.Fatal("latency values must be positive")
	}
	if avgWriteLat < avgReadLat {
		t.Log("Note: write faster than read in this run (possible due to WAL cache hits)")
	}

	t.Log("P12P4 CostCharacterization: PASS — bounded cost statement produced")
}

// --- Test 3: RolloutGate_Bounded ---

func TestP12P4_RolloutGate_Bounded(t *testing.T) {
	floorPath := "../../sw-block/.private/phase/phase-12-p4-floor.md"
	gatesPath := "../../sw-block/.private/phase/phase-12-p4-rollout-gates.md"
	baselinePath := "../../learn/projects/sw-block/test/results/baseline-roce-20260401.md"
	blockerPath := "../../sw-block/.private/phase/phase-12-p3-blockers.md"

	// --- Read all cited evidence sources ---

	floorData, err := os.ReadFile(floorPath)
	if err != nil {
		t.Fatalf("floor doc must exist at %s: %v", floorPath, err)
	}
	floorContent := string(floorData)

	gatesData, err := os.ReadFile(gatesPath)
	if err != nil {
		t.Fatalf("rollout-gates doc must exist at %s: %v", gatesPath, err)
	}
	gatesContent := string(gatesData)

	baselineData, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("cited baseline must exist at %s: %v", baselinePath, err)
	}
	baselineContent := string(baselineData)

	blockerData, err := os.ReadFile(blockerPath)
	if err != nil {
		t.Fatalf("cited blocker ledger must exist at %s: %v", blockerPath, err)
	}
	blockerContent := string(blockerData)

	// --- Structural validation (shape) ---

	// Floor doc: workload envelope, floor table, non-claims.
	for _, required := range []string{"RF=2", "sync_all", "4K random write", "4K random read", "sequential write", "sequential read"} {
		if !strings.Contains(floorContent, required) {
			t.Fatalf("floor doc missing required content: %q", required)
		}
	}
	if !strings.Contains(floorContent, "Floor") || !strings.Contains(floorContent, "IOPS") {
		t.Fatal("floor doc must contain floor table with IOPS")
	}
	if !strings.Contains(floorContent, "does NOT") {
		t.Fatal("floor doc must contain explicit non-claims")
	}

	// Gates doc: gates table, launch envelope, exclusions, non-claims.
	if !strings.Contains(gatesContent, "Gate") || !strings.Contains(gatesContent, "Status") {
		t.Fatal("rollout-gates doc must contain gates table")
	}
	if !strings.Contains(gatesContent, "Launch Envelope") {
		t.Fatal("rollout-gates doc must contain launch envelope")
	}
	if !strings.Contains(gatesContent, "Exclusion") {
		t.Fatal("rollout-gates doc must contain exclusions")
	}
	if !strings.Contains(gatesContent, "does NOT") {
		t.Fatal("rollout-gates doc must contain explicit non-claims")
	}

	// Count gates — must be finite.
	gateLines := 0
	for _, line := range strings.Split(gatesContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "| G") || strings.HasPrefix(trimmed, "| E") {
			gateLines++
		}
	}
	if gateLines == 0 {
		t.Fatal("rollout-gates doc has no gate items")
	}
	if gateLines > 20 {
		t.Fatalf("rollout-gates doc should be finite, got %d items", gateLines)
	}

	// --- Semantic cross-checks (evidence alignment) ---

	// 1. G6 cites "28.4K write IOPS" — baseline must contain this number.
	if strings.Contains(gatesContent, "28.4K write IOPS") || strings.Contains(gatesContent, "28,4") {
		// The gates doc cites write IOPS from baseline. Verify the baseline has it.
		if !strings.Contains(baselineContent, "28,") {
			t.Fatal("G6 cites write IOPS but baseline does not contain matching value")
		}
	}
	// More precise: baseline must contain the specific numbers cited in G6.
	if !strings.Contains(baselineContent, "28,347") && !strings.Contains(baselineContent, "28,429") &&
		!strings.Contains(baselineContent, "28,453") {
		t.Fatal("baseline must contain RF=2 sync_all write IOPS data (28,3xx-28,4xx range)")
	}
	if !strings.Contains(baselineContent, "136,648") {
		t.Fatal("baseline must contain RF=2 read IOPS data (136,648)")
	}

	// 2. G5 cites "-56% replication tax" — baseline must contain this.
	if strings.Contains(gatesContent, "-56%") {
		if !strings.Contains(baselineContent, "-56%") {
			t.Fatal("G5 cites -56% replication tax but baseline does not contain -56%")
		}
	}

	// 3. Launch envelope claims specific transport/network combos — verify against baseline.
	// Claimed: NVMe-TCP @ 25Gbps RoCE
	if strings.Contains(gatesContent, "NVMe-TCP @ 25Gbps RoCE") {
		if !strings.Contains(baselineContent, "NVMe-TCP") || !strings.Contains(baselineContent, "RoCE") {
			t.Fatal("launch envelope claims NVMe-TCP @ RoCE but baseline has no such data")
		}
	}
	// Claimed: iSCSI @ 25Gbps RoCE
	if strings.Contains(gatesContent, "iSCSI @ 25Gbps RoCE") {
		if !strings.Contains(baselineContent, "iSCSI") || !strings.Contains(baselineContent, "RoCE") {
			t.Fatal("launch envelope claims iSCSI @ RoCE but baseline has no such data")
		}
	}
	// Claimed: iSCSI @ 1Gbps
	if strings.Contains(gatesContent, "iSCSI @ 1Gbps") {
		if !strings.Contains(baselineContent, "iSCSI") || !strings.Contains(baselineContent, "1Gbps") {
			t.Fatal("launch envelope claims iSCSI @ 1Gbps but baseline has no such data")
		}
	}
	// Exclusion: NVMe-TCP @ 1Gbps must NOT be claimed as supported.
	if strings.Contains(gatesContent, "NOT included") {
		// Verify baseline indeed lacks NVMe-TCP @ 1Gbps.
		hasNvme1g := strings.Contains(baselineContent, "NVMe-TCP") && strings.Contains(baselineContent, "| NVMe-TCP | 1Gbps")
		if hasNvme1g {
			t.Fatal("baseline contains NVMe-TCP @ 1Gbps data but gates doc excludes it — resolve mismatch")
		}
	}

	// 4. G7 cites blocker ledger counts — verify against actual ledger.
	if strings.Contains(gatesContent, "3 diagnosed") {
		diagCount := 0
		for _, line := range strings.Split(blockerContent, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "| B") {
				diagCount++
			}
		}
		if diagCount != 3 {
			t.Fatalf("G7 claims 3 diagnosed blockers but ledger has %d", diagCount)
		}
	}
	if strings.Contains(gatesContent, "3 unresolved") {
		unresCount := 0
		for _, line := range strings.Split(blockerContent, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "| U") {
				unresCount++
			}
		}
		if unresCount != 3 {
			t.Fatalf("G7 claims 3 unresolved blockers but ledger has %d", unresCount)
		}
	}

	// 5. Floor doc gate thresholds must match code-defined gates.
	for workload, gate := range perfFloorGates {
		// The doc uses comma-formatted numbers (e.g., "1,000" or "5,000").
		minInt := int(gate.MinIOPS)
		// Check for both comma-formatted and plain forms.
		found := false
		for _, form := range []string{
			fmt.Sprintf("%d", minInt),                                      // "1000"
			fmt.Sprintf("%d,%03d", minInt/1000, minInt%1000),               // "1,000"
		} {
			if strings.Contains(floorContent, form) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("floor doc gate for %s should cite minimum %d IOPS but doesn't", workload, minInt)
		}
	}

	t.Logf("P12P4 RolloutGate: floor doc %d bytes, gates doc %d bytes, %d gate items",
		len(floorData), len(gatesData), gateLines)
	t.Log("P12P4 RolloutGate: semantic cross-checks passed (baseline, blocker ledger, gate thresholds)")
	t.Log("P12P4 RolloutGate: PASS — bounded launch envelope with verified evidence alignment")
}

// --- Helpers ---

func init() {
	// Seed random for reproducible LBA patterns within a test run.
	mrand.Seed(time.Now().UnixNano())
}

// formatDuration formats a duration for table display.
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1000.0)
	}
	return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
}
