package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterMetricsActions registers metrics/perf actions.
func RegisterMetricsActions(r *tr.Registry) {
	r.RegisterFunc("scrape_metrics", tr.TierBlock, scrapeMetrics)
	r.RegisterFunc("perf_summary", tr.TierBlock, perfSummary)
	r.RegisterFunc("collect_artifacts", tr.TierBlock, collectArtifactsAction)
	r.RegisterFunc("assert_metric_gt", tr.TierBlock, assertMetricGT)
	r.RegisterFunc("assert_metric_eq", tr.TierBlock, assertMetricEQ)
	r.RegisterFunc("assert_metric_lt", tr.TierBlock, assertMetricLT)
	r.RegisterFunc("pprof_capture", tr.TierBlock, pprofCapture)
	r.RegisterFunc("vmstat_capture", tr.TierBlock, vmstatCapture)
	r.RegisterFunc("iostat_capture", tr.TierBlock, iostatCapture)
}

// scrapeMetrics fetches /metrics from a target's admin port via SSH curl.
// Saves the parsed metrics as JSON string in save_as.
func scrapeMetrics(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("curl -s http://127.0.0.1:%d/metrics 2>&1", tgt.AdminPort)
	stdout, _, code, err := tgt.Node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("scrape_metrics %s: code=%d err=%v", act.Target, code, err)
	}

	sample := tr.MetricsSample{
		Target:  act.Target,
		Metrics: tr.ParsePrometheusText(stdout),
	}
	data, _ := json.Marshal(sample)
	return map[string]string{"value": string(data)}, nil
}

// perfSummary collects the target's log, parses PERF[5s] lines, and outputs stats.
func perfSummary(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	logContent, err := tgt.CollectLog()
	if err != nil {
		return nil, fmt.Errorf("perf_summary: collect log: %w", err)
	}

	perfData := tr.ParsePerfLogLines(logContent)
	if len(perfData) == 0 {
		return map[string]string{"value": "no PERF data found"}, nil
	}

	var lines []string
	for field, values := range perfData {
		stats := tr.ComputeStats(values)
		lines = append(lines, tr.FormatStats(field, stats))
	}

	result := ""
	for i, l := range lines {
		if i > 0 {
			result += "\n"
		}
		result += l
	}
	return map[string]string{"value": result}, nil
}

// parseMetricFromVar extracts a named metric value from a MetricsSample JSON var.
// Params: metrics_var (JSON of MetricsSample), metric (metric name)
func parseMetricFromVar(actx *tr.ActionContext, act tr.Action) (float64, error) {
	varName := act.Params["metrics_var"]
	if varName == "" {
		return 0, fmt.Errorf("metrics_var param required")
	}
	metricName := act.Params["metric"]
	if metricName == "" {
		return 0, fmt.Errorf("metric param required")
	}

	jsonStr := actx.Vars[varName]
	if jsonStr == "" {
		return 0, fmt.Errorf("var %q is empty", varName)
	}

	var sample tr.MetricsSample
	if err := json.Unmarshal([]byte(jsonStr), &sample); err != nil {
		return 0, fmt.Errorf("parse metrics JSON from %q: %w", varName, err)
	}

	val, ok := sample.Metrics[metricName]
	if !ok {
		return 0, fmt.Errorf("metric %q not found in %q", metricName, varName)
	}
	return val, nil
}

// assertMetricGT asserts metric > threshold.
func assertMetricGT(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	val, err := parseMetricFromVar(actx, act)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_gt: %w", err)
	}
	threshold, err := strconv.ParseFloat(act.Params["threshold"], 64)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_gt: invalid threshold: %w", err)
	}
	if val <= threshold {
		return nil, fmt.Errorf("assert_metric_gt: %s = %g <= %g", act.Params["metric"], val, threshold)
	}
	return map[string]string{"value": strconv.FormatFloat(val, 'g', -1, 64)}, nil
}

// assertMetricEQ asserts metric == threshold (within epsilon 1e-9).
func assertMetricEQ(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	val, err := parseMetricFromVar(actx, act)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_eq: %w", err)
	}
	threshold, err := strconv.ParseFloat(act.Params["threshold"], 64)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_eq: invalid threshold: %w", err)
	}
	if math.Abs(val-threshold) > 1e-9 {
		return nil, fmt.Errorf("assert_metric_eq: %s = %g != %g", act.Params["metric"], val, threshold)
	}
	return map[string]string{"value": strconv.FormatFloat(val, 'g', -1, 64)}, nil
}

// assertMetricLT asserts metric < threshold.
func assertMetricLT(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	val, err := parseMetricFromVar(actx, act)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_lt: %w", err)
	}
	threshold, err := strconv.ParseFloat(act.Params["threshold"], 64)
	if err != nil {
		return nil, fmt.Errorf("assert_metric_lt: invalid threshold: %w", err)
	}
	if val >= threshold {
		return nil, fmt.Errorf("assert_metric_lt: %s = %g >= %g", act.Params["metric"], val, threshold)
	}
	return map[string]string{"value": strconv.FormatFloat(val, 'g', -1, 64)}, nil
}

// pprofCapture fetches a Go pprof profile from a target's admin port and saves
// it to a file on the target node. The profile is fetched via SSH curl to the
// admin server's /debug/pprof/ endpoint.
//
// Params:
//   - target (required): target name
//   - profile: pprof profile type (default: "profile" = CPU)
//     Supported: "profile" (CPU), "heap", "allocs", "block", "mutex", "goroutine"
//   - seconds: duration for CPU profile (default: "30")
//   - output_dir: directory to save profile on target node (default: "/tmp/pprof")
//   - label: filename label (default: profile type)
//
// Returns: value = remote file path of saved profile
func pprofCapture(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	profile := paramDefault(act.Params, "profile", "profile")
	seconds := paramDefault(act.Params, "seconds", "30")
	outputDir := paramDefault(act.Params, "output_dir", "/tmp/pprof")
	label := paramDefault(act.Params, "label", profile)

	// Validate profile type.
	switch profile {
	case "profile", "heap", "allocs", "block", "mutex", "goroutine":
		// OK
	default:
		return nil, fmt.Errorf("pprof_capture: unsupported profile type %q", profile)
	}

	// Create output directory.
	if _, _, _, err := tgt.Node.RunRoot(ctx, "mkdir -p "+outputDir); err != nil {
		return nil, fmt.Errorf("pprof_capture: mkdir: %w", err)
	}

	outFile := fmt.Sprintf("%s/%s.pb.gz", outputDir, label)

	// Build URL. CPU profile uses ?seconds= parameter.
	url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/%s", tgt.AdminPort, profile)
	if profile == "profile" {
		url += "?seconds=" + seconds
	}

	actx.Log("  pprof %s → %s (%ss)", profile, outFile, seconds)

	cmd := fmt.Sprintf("curl -s -o %s '%s'", outFile, url)
	// CPU profile can take a while — extend timeout.
	_, stderr, code, err := tgt.Node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("pprof_capture: curl failed: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": outFile}, nil
}

// vmstatCapture runs vmstat on a node for a duration and saves the output.
// Params:
//   - node (required): node to run on
//   - seconds: duration (default: "30")
//   - interval: vmstat interval (default: "1")
//   - output_dir: directory for output (default: "/tmp/pprof")
//   - label: filename label (default: "vmstat")
//
// Returns: value = remote file path
func vmstatCapture(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	seconds := paramDefault(act.Params, "seconds", "30")
	interval := paramDefault(act.Params, "interval", "1")
	outputDir := paramDefault(act.Params, "output_dir", "/tmp/pprof")
	label := paramDefault(act.Params, "label", "vmstat")

	if _, _, _, err := node.RunRoot(ctx, "mkdir -p "+outputDir); err != nil {
		return nil, fmt.Errorf("vmstat_capture: mkdir: %w", err)
	}

	outFile := fmt.Sprintf("%s/%s.txt", outputDir, label)

	// vmstat <interval> <count>
	count, _ := strconv.Atoi(seconds)
	intv, _ := strconv.Atoi(interval)
	if intv < 1 {
		intv = 1
	}
	iterations := count / intv
	if iterations < 1 {
		iterations = 1
	}

	cmd := fmt.Sprintf("vmstat %s %d > %s 2>&1", interval, iterations, outFile)
	actx.Log("  vmstat %s×%d → %s", interval, iterations, outFile)

	_, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("vmstat_capture: code=%d err=%v", code, err)
	}

	return map[string]string{"value": outFile}, nil
}

// iostatCapture runs iostat -x on a node for a duration and saves the output.
// Params:
//   - node (required): node to run on
//   - seconds: duration (default: "30")
//   - interval: iostat interval (default: "1")
//   - output_dir: directory for output (default: "/tmp/pprof")
//   - label: filename label (default: "iostat")
//
// Returns: value = remote file path
func iostatCapture(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	seconds := paramDefault(act.Params, "seconds", "30")
	interval := paramDefault(act.Params, "interval", "1")
	outputDir := paramDefault(act.Params, "output_dir", "/tmp/pprof")
	label := paramDefault(act.Params, "label", "iostat")

	if _, _, _, err := node.RunRoot(ctx, "mkdir -p "+outputDir); err != nil {
		return nil, fmt.Errorf("iostat_capture: mkdir: %w", err)
	}

	outFile := fmt.Sprintf("%s/%s.txt", outputDir, label)

	count, _ := strconv.Atoi(seconds)
	intv, _ := strconv.Atoi(interval)
	if intv < 1 {
		intv = 1
	}
	iterations := count / intv
	if iterations < 1 {
		iterations = 1
	}

	cmd := fmt.Sprintf("iostat -x %s %d > %s 2>&1", interval, iterations, outFile)
	actx.Log("  iostat -x %s×%d → %s", interval, iterations, outFile)

	_, _, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("iostat_capture: code=%d err=%v", code, err)
	}

	return map[string]string{"value": outFile}, nil
}

// collectArtifactsAction explicitly collects artifacts from targets.
func collectArtifactsAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	dir := act.Params["dir"]
	if dir == "" {
		dir = tempPath(actx, "artifacts")
	}

	// Find client node for dmesg/lsblk.
	clientNodeName := act.Node
	if clientNodeName == "" {
		clientNodeName = "client_node"
	}
	node, _ := GetNode(actx, clientNodeName)
	if node == nil {
		// Use any available node.
		for _, n := range actx.Nodes {
			if nn, ok := n.(*infra.Node); ok {
				node = nn
				break
			}
		}
	}
	if node == nil {
		return nil, fmt.Errorf("collect_artifacts: no node available")
	}

	collector := infra.NewArtifactCollector(dir, node, nil)

	for name, tgt := range actx.Targets {
		if lc, ok := tgt.(infra.LogCollector); ok {
			collector.CollectLabeled(lc, name)
		}
	}

	return map[string]string{"value": fmt.Sprintf("artifacts saved to %s", dir)}, nil
}
