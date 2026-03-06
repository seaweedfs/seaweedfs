package testrunner

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"time"
)

// WriteHTMLReport generates a single-file HTML report from a ScenarioResult.
func WriteHTMLReport(result *ScenarioResult, path string) error {
	data := buildHTMLData(result)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()

	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		return fmt.Errorf("parse template: %w", err)
	}

	return tmpl.Execute(f, data)
}

// htmlData is the template context.
type htmlData struct {
	Name      string
	Status    string
	StatusCSS string
	Duration  string
	Timestamp string
	Phases    []htmlPhase
	Perf      []htmlPerfEntry
	Metrics   []htmlMetricsGroup
	Artifacts []ArtifactEntry
	Error     string
	HasPerf   bool
	HasMetrics bool
	HasArtifacts bool
}

type htmlPhase struct {
	Name     string
	Status   string
	StatusCSS string
	Duration string
	Actions  []htmlAction
}

type htmlAction struct {
	Name     string
	Status   string
	StatusCSS string
	Duration string
	Output   string
	Error    string
	YAML     string
}

type htmlPerfEntry struct {
	Name  string
	Count int
	Mean  string
	P50   string
	P90   string
	P99   string
	Min   string
	Max   string
}

type htmlMetricsGroup struct {
	Name    string
	Entries []htmlMetricEntry
}

type htmlMetricEntry struct {
	Name  string
	Value string
}

func buildHTMLData(result *ScenarioResult) htmlData {
	d := htmlData{
		Name:      result.Name,
		Status:    string(result.Status),
		StatusCSS: statusCSS(result.Status),
		Duration:  result.Duration.Round(time.Millisecond).String(),
		Timestamp: time.Now().Format(time.RFC3339),
		Error:     result.Error,
		Artifacts: result.Artifacts,
		HasArtifacts: len(result.Artifacts) > 0,
	}

	for _, pr := range result.Phases {
		hp := htmlPhase{
			Name:      pr.Name,
			Status:    string(pr.Status),
			StatusCSS: statusCSS(pr.Status),
			Duration:  pr.Duration.Round(time.Millisecond).String(),
		}
		for _, ar := range pr.Actions {
			hp.Actions = append(hp.Actions, htmlAction{
				Name:      ar.Action,
				Status:    string(ar.Status),
				StatusCSS: statusCSS(ar.Status),
				Duration:  ar.Duration.Round(time.Millisecond).String(),
				Output:    ar.Output,
				Error:     ar.Error,
				YAML:      ar.YAML,
			})
		}
		d.Phases = append(d.Phases, hp)
	}

	// Extract perf stats from vars.
	if len(result.Vars) > 0 {
		d.Perf = extractHTMLPerf(result.Vars)
		d.HasPerf = len(d.Perf) > 0
		d.Metrics = extractHTMLMetrics(result.Vars)
		d.HasMetrics = len(d.Metrics) > 0
	}

	return d
}

func statusCSS(s ResultStatus) string {
	switch s {
	case StatusPass:
		return "pass"
	case StatusFail:
		return "fail"
	case StatusSkip:
		return "skip"
	default:
		return ""
	}
}

func extractHTMLPerf(vars map[string]string) []htmlPerfEntry {
	var entries []htmlPerfEntry
	for key, val := range vars {
		var ps PerfStats
		if err := json.Unmarshal([]byte(val), &ps); err == nil && ps.Count > 0 {
			entries = append(entries, perfToHTML(key, ps))
			continue
		}
		if ps, ok := parsePerfStatsLine(val); ok {
			entries = append(entries, perfToHTML(key, ps))
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}

func perfToHTML(name string, ps PerfStats) htmlPerfEntry {
	return htmlPerfEntry{
		Name:  name,
		Count: ps.Count,
		Mean:  fmt.Sprintf("%.1f", ps.Mean),
		P50:   fmt.Sprintf("%.1f", ps.P50),
		P90:   fmt.Sprintf("%.1f", ps.P90),
		P99:   fmt.Sprintf("%.1f", ps.P99),
		Min:   fmt.Sprintf("%.1f", ps.Min),
		Max:   fmt.Sprintf("%.1f", ps.Max),
	}
}

func extractHTMLMetrics(vars map[string]string) []htmlMetricsGroup {
	var groups []htmlMetricsGroup
	for key, val := range vars {
		var ms MetricsSample
		if err := json.Unmarshal([]byte(val), &ms); err != nil || len(ms.Metrics) == 0 {
			continue
		}
		label := key
		if ms.Target != "" {
			label = ms.Target
		}
		names := make([]string, 0, len(ms.Metrics))
		for n := range ms.Metrics {
			names = append(names, n)
		}
		sort.Strings(names)
		g := htmlMetricsGroup{Name: label}
		for _, n := range names {
			g.Entries = append(g.Entries, htmlMetricEntry{
				Name:  n,
				Value: formatMetricValue(ms.Metrics[n]),
			})
		}
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	return groups
}

var htmlTemplate = strings.TrimSpace(`
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{{.Name}} — Test Report</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background: #1a1a2e; color: #e0e0e0; padding: 24px; }
  h1 { font-size: 1.4em; margin-bottom: 8px; }
  h2 { font-size: 1.1em; margin: 24px 0 8px; color: #a0a0c0; }
  .header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
  .badge { padding: 4px 12px; border-radius: 4px; font-weight: bold; font-size: 0.85em; }
  .badge.pass { background: #2d6a4f; color: #b7e4c7; }
  .badge.fail { background: #922b21; color: #f5b7b1; }
  .badge.skip { background: #7d6608; color: #f9e79f; }
  .meta { color: #888; font-size: 0.85em; }
  table { border-collapse: collapse; width: 100%; margin-top: 8px; }
  th, td { text-align: left; padding: 6px 12px; border-bottom: 1px solid #2a2a4a; }
  th { color: #a0a0c0; font-size: 0.8em; text-transform: uppercase; }
  tr:hover { background: #2a2a4a; }
  .phase-row { background: #16213e; font-weight: bold; }
  .action-row td:first-child { padding-left: 32px; }
  .error { color: #e74c3c; font-size: 0.85em; padding: 4px 12px 4px 32px; }
  .output { color: #888; font-size: 0.8em; padding: 2px 12px 2px 32px; white-space: pre-wrap; word-break: break-all; max-height: 200px; overflow-y: auto; }
  .perf-table th, .perf-table td { text-align: right; }
  .perf-table th:first-child, .perf-table td:first-child { text-align: left; }
  .artifact-list { list-style: none; }
  .artifact-list li { padding: 4px 0; font-size: 0.85em; color: #a0a0c0; }
  .artifact-list .size { color: #666; margin-left: 8px; }
</style>
</head>
<body>
  <div class="header">
    <h1>{{.Name}}</h1>
    <span class="badge {{.StatusCSS}}">{{.Status}}</span>
    <span class="meta">{{.Duration}}</span>
  </div>
  <div class="meta">Generated: {{.Timestamp}}</div>

  <h2>Phases &amp; Actions</h2>
  <table>
    <tr><th>Name</th><th>Status</th><th>Duration</th></tr>
    {{range .Phases}}
    <tr class="phase-row">
      <td>{{.Name}}</td>
      <td><span class="badge {{.StatusCSS}}">{{.Status}}</span></td>
      <td>{{.Duration}}</td>
    </tr>
    {{range .Actions}}
    <tr class="action-row">
      <td>{{.Name}}</td>
      <td><span class="badge {{.StatusCSS}}">{{.Status}}</span></td>
      <td>{{.Duration}}</td>
    </tr>
    {{if .Error}}<tr><td colspan="3" class="error">ERROR: {{.Error}}</td></tr>{{end}}
    {{if .Output}}<tr><td colspan="3" class="output">{{.Output}}</td></tr>{{end}}
    {{if .YAML}}<tr><td colspan="3" style="padding-left:32px"><details><summary style="cursor:pointer;color:#a0a0c0;font-size:0.8em">YAML definition</summary><pre class="output">{{.YAML}}</pre></details></td></tr>{{end}}
    {{end}}
    {{end}}
  </table>

  {{if .HasPerf}}
  <h2>Performance</h2>
  <table class="perf-table">
    <tr><th>Metric</th><th>Count</th><th>Mean</th><th>P50</th><th>P90</th><th>P99</th><th>Min</th><th>Max</th></tr>
    {{range .Perf}}
    <tr><td>{{.Name}}</td><td>{{.Count}}</td><td>{{.Mean}}</td><td>{{.P50}}</td><td>{{.P90}}</td><td>{{.P99}}</td><td>{{.Min}}</td><td>{{.Max}}</td></tr>
    {{end}}
  </table>
  {{end}}

  {{if .HasMetrics}}
  {{range .Metrics}}
  <h2>Metrics ({{.Name}})</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    {{range .Entries}}
    <tr><td>{{.Name}}</td><td>{{.Value}}</td></tr>
    {{end}}
  </table>
  {{end}}
  {{end}}

  {{if .HasArtifacts}}
  <h2>Artifacts</h2>
  <ul class="artifact-list">
    {{range .Artifacts}}
    <li>{{.Agent}}: {{.Path}} <span class="size">({{.Size}} bytes)</span></li>
    {{end}}
  </ul>
  {{end}}

  {{if .Error}}
  <h2>Error</h2>
  <pre class="error" style="padding:12px">{{.Error}}</pre>
  {{end}}
</body>
</html>
`)
