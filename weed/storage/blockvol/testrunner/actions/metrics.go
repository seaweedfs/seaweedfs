package actions

import (
	"context"
	"encoding/json"
	"fmt"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterMetricsActions registers metrics/perf actions.
func RegisterMetricsActions(r *tr.Registry) {
	r.RegisterFunc("scrape_metrics", tr.TierBlock, scrapeMetrics)
	r.RegisterFunc("perf_summary", tr.TierBlock, perfSummary)
	r.RegisterFunc("collect_artifacts", tr.TierBlock, collectArtifactsAction)
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

// collectArtifactsAction explicitly collects artifacts from targets.
func collectArtifactsAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	dir := act.Params["dir"]
	if dir == "" {
		dir = "/tmp/sw-test-runner-artifacts"
	}

	// Find client node for dmesg/lsblk.
	clientNodeName := act.Node
	if clientNodeName == "" {
		clientNodeName = "client_node"
	}
	node, _ := getNode(actx, clientNodeName)
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
