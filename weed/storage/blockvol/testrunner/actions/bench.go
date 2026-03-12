package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterBenchActions registers benchmark-related actions.
func RegisterBenchActions(r *tr.Registry) {
	r.RegisterFunc("fio_json", tr.TierBlock, fioJSON)
	r.RegisterFunc("fio_parse", tr.TierCore, fioParse)
	r.RegisterFunc("bench_compare", tr.TierCore, benchCompare)
	r.RegisterFunc("bench_stats", tr.TierCore, benchStats)
}

// fioJSON runs fio with JSON output. Supports numjobs for multi-queue testing.
// Params:
//   - device (required): block device path
//   - rw: IO pattern (default: "randwrite")
//   - bs: block size (default: "4k")
//   - iodepth: queue depth per job (default: "32")
//   - numjobs: number of parallel jobs (default: "1")
//   - runtime: seconds (default: "60")
//   - size: file/device size (default: "256M")
//   - name: job name (default: "bench")
//   - rwmixread: read percentage for randrw (optional)
//
// Returns: value = fio JSON output string
func fioJSON(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("fio_json: device param required")
	}

	rw := paramDefault(act.Params, "rw", "randwrite")
	bs := paramDefault(act.Params, "bs", "4k")
	iodepth := paramDefault(act.Params, "iodepth", "32")
	numjobs := paramDefault(act.Params, "numjobs", "1")
	runtime := paramDefault(act.Params, "runtime", "60")
	size := paramDefault(act.Params, "size", "256M")
	name := paramDefault(act.Params, "name", "bench")

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("fio --name=%s --filename=%s --rw=%s --bs=%s --iodepth=%s --numjobs=%s --direct=1 --ioengine=libaio --runtime=%s --time_based --size=%s --group_reporting --output-format=json",
		name, device, rw, bs, iodepth, numjobs, runtime, size)

	if rwmixread := act.Params["rwmixread"]; rwmixread != "" {
		cmd += fmt.Sprintf(" --rwmixread=%s", rwmixread)
	}

	actx.Log("  fio %s bs=%s j=%s qd=%s %ss on %s", rw, bs, numjobs, iodepth, runtime, device)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("fio_json: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": stdout}, nil
}

// fioParse extracts a specific metric from fio JSON output.
// Params:
//   - json_var: name of var containing fio JSON (required)
//   - metric: one of "iops", "bw_bytes", "lat_mean_us", "lat_p50_us", "lat_p99_us", "lat_p999_us" (required)
//   - direction: "read" or "write" (default: auto-detect from rw type)
//
// Returns: value = numeric string
func fioParse(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	varName := act.Params["json_var"]
	if varName == "" {
		return nil, fmt.Errorf("fio_parse: json_var param required")
	}
	metric := act.Params["metric"]
	if metric == "" {
		return nil, fmt.Errorf("fio_parse: metric param required")
	}

	jsonStr := actx.Vars[varName]
	if jsonStr == "" {
		return nil, fmt.Errorf("fio_parse: var %q is empty", varName)
	}

	val, err := ParseFioMetric(jsonStr, metric, act.Params["direction"])
	if err != nil {
		return nil, fmt.Errorf("fio_parse: %w", err)
	}

	return map[string]string{"value": strconv.FormatFloat(val, 'f', 2, 64)}, nil
}

// benchCompare compares two fio results and asserts a performance gate.
// Params:
//   - a_var: var name for baseline (e.g. iSCSI) fio JSON (required)
//   - b_var: var name for candidate (e.g. NVMe) fio JSON (required)
//   - metric: metric to compare (required, same as fio_parse)
//   - gate: minimum ratio b/a (default: "1.0" = candidate >= baseline)
//   - warn_gate: soft threshold — ratio < gate but >= warn_gate returns success
//     with value prefixed "WARN:" instead of hard-failing (optional)
//   - direction: "read" or "write" (default: auto-detect)
//
// Returns: value = "delta_pct" (e.g. "+14.1%"), prefixed "WARN:" if in warn band.
// Fails only if candidate/baseline < warn_gate (or < gate when warn_gate is unset).
func benchCompare(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	aVar := act.Params["a_var"]
	bVar := act.Params["b_var"]
	metric := act.Params["metric"]
	if aVar == "" || bVar == "" || metric == "" {
		return nil, fmt.Errorf("bench_compare: a_var, b_var, metric params required")
	}

	gateStr := paramDefault(act.Params, "gate", "1.0")
	gate, err := strconv.ParseFloat(gateStr, 64)
	if err != nil {
		return nil, fmt.Errorf("bench_compare: invalid gate %q: %w", gateStr, err)
	}

	// warn_gate: soft threshold below gate. If ratio is between warn_gate and gate,
	// we return success with a "WARN:" prefix instead of hard-failing.
	warnGate := 0.0
	hasWarnGate := false
	if wg := act.Params["warn_gate"]; wg != "" {
		warnGate, err = strconv.ParseFloat(wg, 64)
		if err != nil {
			return nil, fmt.Errorf("bench_compare: invalid warn_gate %q: %w", wg, err)
		}
		hasWarnGate = true
	}

	direction := act.Params["direction"]

	aJSON := actx.Vars[aVar]
	bJSON := actx.Vars[bVar]
	if aJSON == "" {
		return nil, fmt.Errorf("bench_compare: var %q is empty", aVar)
	}
	if bJSON == "" {
		return nil, fmt.Errorf("bench_compare: var %q is empty", bVar)
	}

	aVal, err := ParseFioMetric(aJSON, metric, direction)
	if err != nil {
		return nil, fmt.Errorf("bench_compare baseline (%s): %w", aVar, err)
	}
	bVal, err := ParseFioMetric(bJSON, metric, direction)
	if err != nil {
		return nil, fmt.Errorf("bench_compare candidate (%s): %w", bVar, err)
	}

	// For latency metrics, lower is better — invert the comparison.
	isLatency := strings.HasPrefix(metric, "lat_")
	var ratio float64
	var deltaStr string

	if aVal == 0 {
		return nil, fmt.Errorf("bench_compare: baseline %s = 0, cannot compute ratio", metric)
	}

	if isLatency {
		// For latency: ratio = baseline/candidate (higher is better = candidate has lower latency)
		ratio = aVal / bVal
		deltaPct := (aVal - bVal) / aVal * 100
		if deltaPct >= 0 {
			deltaStr = fmt.Sprintf("-%.1f%%", deltaPct) // latency decreased = good
		} else {
			deltaStr = fmt.Sprintf("+%.1f%%", -deltaPct) // latency increased = bad
		}
	} else {
		// For throughput: ratio = candidate/baseline (higher is better)
		ratio = bVal / aVal
		deltaPct := (bVal - aVal) / aVal * 100
		if deltaPct >= 0 {
			deltaStr = fmt.Sprintf("+%.1f%%", deltaPct)
		} else {
			deltaStr = fmt.Sprintf("%.1f%%", deltaPct)
		}
	}

	actx.Log("  %s: baseline=%.1f candidate=%.1f delta=%s ratio=%.3f gate=%.2f",
		metric, aVal, bVal, deltaStr, ratio, gate)

	if ratio < gate {
		// If warn_gate is set and ratio >= warn_gate, return success with WARN prefix.
		if hasWarnGate && ratio >= warnGate {
			actx.Log("  WARN: ratio %.3f below gate %.2f but above warn_gate %.2f", ratio, gate, warnGate)
			return map[string]string{"value": "WARN:" + deltaStr}, nil
		}
		return nil, fmt.Errorf("bench_compare FAIL: %s ratio=%.3f < gate=%.2f (baseline=%.1f candidate=%.1f delta=%s)",
			metric, ratio, gate, aVal, bVal, deltaStr)
	}

	return map[string]string{"value": deltaStr}, nil
}

// --- fio JSON parsing ---

// fioOutput represents the top-level fio JSON output.
type fioOutput struct {
	Jobs []fioJob `json:"jobs"`
}

type fioJob struct {
	JobName string      `json:"jobname"`
	Read    fioJobStats `json:"read"`
	Write   fioJobStats `json:"write"`
}

type fioJobStats struct {
	IOPS    float64    `json:"iops"`
	BWBytes float64    `json:"bw_bytes"`
	LatNS   fioLatency `json:"lat_ns"`
	CLatNS  fioLatency `json:"clat_ns"`
}

type fioLatency struct {
	Mean       float64            `json:"mean"`
	Percentile map[string]float64 `json:"percentile"`
}

// ParseFioMetric extracts a named metric from fio JSON or returns a plain
// numeric value directly. This allows bench_compare to accept either raw fio
// JSON output or pre-aggregated scalar values (e.g. from bench_stats or
// phase repeat/aggregate).
//
// direction: "read", "write", or "" (auto-detect: use whichever has IOPS > 0).
// Supported metrics: "iops", "bw_bytes", "bw_mb", "lat_mean_us", "lat_p50_us", "lat_p99_us", "lat_p999_us"
func ParseFioMetric(input, metric, direction string) (float64, error) {
	// Try plain numeric value first (from aggregation or bench_stats).
	trimmed := strings.TrimSpace(input)
	if v, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return v, nil
	}

	// Try quoted numeric string (e.g. "15322.00").
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		if v, err := strconv.ParseFloat(trimmed[1:len(trimmed)-1], 64); err == nil {
			return v, nil
		}
	}

	// Parse as fio JSON.
	var output fioOutput
	if err := json.Unmarshal([]byte(input), &output); err != nil {
		return 0, fmt.Errorf("parse fio metric: input is neither a number nor valid fio JSON: %w", err)
	}
	if len(output.Jobs) == 0 {
		return 0, fmt.Errorf("fio JSON has no jobs")
	}

	// Use first job (group_reporting merges into one).
	job := output.Jobs[0]

	// Auto-detect direction.
	var stats fioJobStats
	switch direction {
	case "read":
		stats = job.Read
	case "write":
		stats = job.Write
	default:
		if job.Write.IOPS > 0 {
			stats = job.Write
		} else {
			stats = job.Read
		}
	}

	switch metric {
	case "iops":
		return stats.IOPS, nil
	case "bw_bytes":
		return stats.BWBytes, nil
	case "bw_mb":
		return stats.BWBytes / (1024 * 1024), nil
	case "lat_mean_us":
		return stats.LatNS.Mean / 1000, nil // ns → µs
	case "lat_p50_us":
		return getPercentileWithFallback(stats, "50.000000") / 1000, nil
	case "lat_p99_us":
		return getPercentileWithFallback(stats, "99.000000") / 1000, nil
	case "lat_p999_us":
		return getPercentileWithFallback(stats, "99.900000") / 1000, nil
	default:
		return 0, fmt.Errorf("unknown metric %q", metric)
	}
}

func getPercentile(lat fioLatency, key string) float64 {
	if lat.Percentile == nil {
		return 0
	}
	return lat.Percentile[key]
}

// getPercentileWithFallback tries clat_ns first (fio puts percentiles there),
// then falls back to lat_ns.
func getPercentileWithFallback(stats fioJobStats, key string) float64 {
	if v := getPercentile(stats.CLatNS, key); v != 0 {
		return v
	}
	return getPercentile(stats.LatNS, key)
}

// benchStats computes statistics from a comma-separated list of values.
// Useful for aggregating results from multiple runs outside the phase repeat system.
// Params:
//   - values_var: name of var containing comma-separated numeric values (required)
//   - trim_pct: percentage of outliers to trim from each end (default: "20")
//   - label: label for log output (default: "bench_stats")
//
// Returns: value = median. Also sets {save_as}_mean, _stddev, _min, _max, _n.
func benchStats(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	varName := act.Params["values_var"]
	if varName == "" {
		return nil, fmt.Errorf("bench_stats: values_var param required")
	}
	valStr := actx.Vars[varName]
	if valStr == "" {
		return nil, fmt.Errorf("bench_stats: var %q is empty", varName)
	}

	trimPct := 20
	if tp := act.Params["trim_pct"]; tp != "" {
		if v, err := strconv.Atoi(tp); err == nil {
			trimPct = v
		}
	}
	label := act.Params["label"]
	if label == "" {
		label = "bench_stats"
	}

	// Parse comma-separated values.
	parts := strings.Split(valStr, ",")
	var values []float64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, fmt.Errorf("bench_stats: invalid value %q in %s: %w", p, varName, err)
		}
		values = append(values, f)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("bench_stats: no numeric values in %s", varName)
	}

	// Trim outliers and compute stats.
	trimmed := trimValues(values, trimPct)
	stats := tr.ComputeStats(trimmed)

	actx.Log("  [%s] n=%d median=%.2f mean=%.2f stddev=%.2f min=%.2f max=%.2f (trimmed %d%% from %d)",
		label, stats.Count, stats.P50, stats.Mean, stats.StdDev, stats.Min, stats.Max, trimPct, len(values))

	result := map[string]string{
		"value": strconv.FormatFloat(stats.P50, 'f', 2, 64),
	}

	// Store detailed stats as __-prefixed vars for auto-propagation.
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_mean"] = strconv.FormatFloat(stats.Mean, 'f', 2, 64)
		actx.Vars[act.SaveAs+"_stddev"] = strconv.FormatFloat(stats.StdDev, 'f', 2, 64)
		actx.Vars[act.SaveAs+"_min"] = strconv.FormatFloat(stats.Min, 'f', 2, 64)
		actx.Vars[act.SaveAs+"_max"] = strconv.FormatFloat(stats.Max, 'f', 2, 64)
		actx.Vars[act.SaveAs+"_n"] = strconv.Itoa(stats.Count)
	}

	return result, nil
}

// trimValues removes the top and bottom pct% of values.
func trimValues(values []float64, pct int) []float64 {
	if len(values) <= 2 || pct <= 0 {
		return values
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	trim := int(math.Round(float64(len(sorted)) * float64(pct) / 100.0))
	if trim*2 >= len(sorted) {
		trim = (len(sorted) - 1) / 2
	}
	return sorted[trim : len(sorted)-trim]
}

func paramDefault(params map[string]string, key, def string) string {
	if v := params[key]; v != "" {
		return v
	}
	return def
}

// FormatBenchReport generates a human-readable A/B comparison table.
// results is a list of {workload, metric, baselineVal, candidateVal, deltaPct, gate, pass}.
func FormatBenchReport(results []BenchResult) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%-24s | %12s | %12s | %8s | %s\n", "Workload", "Baseline", "Candidate", "Delta", "Gate"))
	b.WriteString(strings.Repeat("-", 76) + "\n")
	for _, r := range results {
		status := "PASS"
		if !r.Pass {
			status = "FAIL"
			if r.Ratio >= 0.9 {
				status = "WARN"
			}
		}
		b.WriteString(fmt.Sprintf("%-24s | %12.1f | %12.1f | %7s | %s\n",
			r.Workload, r.Baseline, r.Candidate, r.Delta, status))
	}
	return b.String()
}

// BenchResult holds one row of A/B comparison.
type BenchResult struct {
	Workload  string
	Metric    string
	Baseline  float64
	Candidate float64
	Delta     string
	Ratio     float64
	Gate      float64
	Pass      bool
}

// ComputeBenchResult computes a single A/B comparison row.
func ComputeBenchResult(workload, metric string, baseline, candidate, gate float64) BenchResult {
	isLatency := strings.HasPrefix(metric, "lat_")
	var ratio float64
	var delta string

	if baseline == 0 {
		return BenchResult{Workload: workload, Metric: metric, Pass: false, Delta: "N/A"}
	}

	if isLatency {
		ratio = baseline / candidate
		deltaPct := (baseline - candidate) / baseline * 100
		if deltaPct >= 0 {
			delta = fmt.Sprintf("-%.1f%%", deltaPct)
		} else {
			delta = fmt.Sprintf("+%.1f%%", math.Abs(deltaPct))
		}
	} else {
		ratio = candidate / baseline
		deltaPct := (candidate - baseline) / baseline * 100
		if deltaPct >= 0 {
			delta = fmt.Sprintf("+%.1f%%", deltaPct)
		} else {
			delta = fmt.Sprintf("%.1f%%", deltaPct)
		}
	}

	return BenchResult{
		Workload:  workload,
		Metric:    metric,
		Baseline:  baseline,
		Candidate: candidate,
		Delta:     delta,
		Ratio:     ratio,
		Gate:      gate,
		Pass:      ratio >= gate,
	}
}
