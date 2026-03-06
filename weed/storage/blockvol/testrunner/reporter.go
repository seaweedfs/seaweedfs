package testrunner

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// WriteJSON writes the scenario result as JSON to the given path.
func WriteJSON(result *ScenarioResult, path string) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

// PrintSummary writes a human-readable summary table to w.
func PrintSummary(w io.Writer, result *ScenarioResult) {
	fmt.Fprintf(w, "\n=== %s === %s (%s)\n\n",
		result.Name, result.Status, result.Duration.Round(time.Millisecond))

	passCount, failCount, totalActions := 0, 0, 0
	for _, pr := range result.Phases {
		status := string(pr.Status)
		fmt.Fprintf(w, "  %-20s %s  (%s)\n",
			pr.Name, status, pr.Duration.Round(time.Millisecond))
		for _, ar := range pr.Actions {
			totalActions++
			if ar.Status == StatusPass {
				passCount++
			} else {
				failCount++
			}
			marker := " "
			if ar.Status == StatusFail {
				marker = "X"
			}
			fmt.Fprintf(w, "    %s %-25s %s\n", marker, ar.Action, ar.Duration.Round(time.Millisecond))
			if ar.Error != "" {
				fmt.Fprintf(w, "      ERROR: %s\n", ar.Error)
			}
		}
	}

	fmt.Fprintf(w, "\n  %d actions: %d passed, %d failed\n", totalActions, passCount, failCount)

	// Render perf stats and metrics tables from result.Vars.
	if len(result.Vars) > 0 {
		printPerfTable(w, result.Vars)
		printMetricsTable(w, result.Vars)
	}

	fmt.Fprintln(w)

	if result.Error != "" {
		fmt.Fprintf(w, "  ERROR: %s\n\n", result.Error)
	}
}

// printPerfTable scans vars for PerfStats-format values (FormatStats output)
// and renders a formatted table. Matches lines like "name: n=100 mean=1.23 ...".
func printPerfTable(w io.Writer, vars map[string]string) {
	type perfEntry struct {
		name  string
		stats PerfStats
	}
	var entries []perfEntry

	for key, val := range vars {
		// Try parsing as PerfStats JSON first.
		var ps PerfStats
		if err := json.Unmarshal([]byte(val), &ps); err == nil && ps.Count > 0 {
			entries = append(entries, perfEntry{name: key, stats: ps})
			continue
		}
		// Try parsing FormatStats text: "name: n=100 mean=1.23 ..."
		if ps, ok := parsePerfStatsLine(val); ok {
			entries = append(entries, perfEntry{name: key, stats: ps})
		}
	}

	if len(entries) == 0 {
		return
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	fmt.Fprintf(w, "\n  === Performance ===\n")
	fmt.Fprintf(w, "  %-16s %6s %12s %12s %12s %12s %12s %12s\n",
		"Metric", "Count", "Mean", "P50", "P90", "P99", "Min", "Max")
	for _, e := range entries {
		fmt.Fprintf(w, "  %-16s %6d %12.1f %12.1f %12.1f %12.1f %12.1f %12.1f\n",
			e.name, e.stats.Count, e.stats.Mean, e.stats.P50, e.stats.P90, e.stats.P99, e.stats.Min, e.stats.Max)
	}
}

// parsePerfStatsLine parses a FormatStats-style line: "name: n=100 mean=1.23 stddev=0.5 p50=1.2 p90=1.5 p99=1.8 min=0.5 max=2.0"
func parsePerfStatsLine(s string) (PerfStats, bool) {
	if !strings.Contains(s, "n=") || !strings.Contains(s, "p99=") {
		return PerfStats{}, false
	}

	var ps PerfStats
	fields := strings.Fields(s)
	parsed := 0
	for _, f := range fields {
		parts := strings.SplitN(f, "=", 2)
		if len(parts) != 2 {
			continue
		}
		var val float64
		if _, err := fmt.Sscanf(parts[1], "%f", &val); err != nil {
			continue
		}
		switch parts[0] {
		case "n":
			ps.Count = int(val)
			parsed++
		case "mean":
			ps.Mean = val
			parsed++
		case "stddev":
			ps.StdDev = val
			parsed++
		case "p50":
			ps.P50 = val
			parsed++
		case "p90":
			ps.P90 = val
			parsed++
		case "p99":
			ps.P99 = val
			parsed++
		case "min":
			ps.Min = val
			parsed++
		case "max":
			ps.Max = val
			parsed++
		}
	}
	return ps, parsed >= 3 && ps.Count > 0
}

// printMetricsTable scans vars for MetricsSample JSON values and renders a table.
func printMetricsTable(w io.Writer, vars map[string]string) {
	type metricsGroup struct {
		name    string
		sample  MetricsSample
	}
	var groups []metricsGroup

	for key, val := range vars {
		var ms MetricsSample
		if err := json.Unmarshal([]byte(val), &ms); err != nil {
			continue
		}
		if len(ms.Metrics) == 0 {
			continue
		}
		label := key
		if ms.Target != "" {
			label = ms.Target
		}
		groups = append(groups, metricsGroup{name: label, sample: ms})
	}

	if len(groups) == 0 {
		return
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].name < groups[j].name })

	for _, g := range groups {
		fmt.Fprintf(w, "\n  === Metrics (%s) ===\n", g.name)
		// Sort metric names.
		names := make([]string, 0, len(g.sample.Metrics))
		for name := range g.sample.Metrics {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(w, "  %-40s %s\n", name, formatMetricValue(g.sample.Metrics[name]))
		}
	}
}

// formatMetricValue formats a float with commas for readability.
func formatMetricValue(v float64) string {
	if v == float64(int64(v)) {
		return formatInt(int64(v))
	}
	return fmt.Sprintf("%.2f", v)
}

// formatInt formats an integer with comma separators.
func formatInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	if n < 0 {
		return s
	}
	// Insert commas.
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// --- JUnit XML output ---

// JUnitTestSuites is the root of JUnit XML.
type JUnitTestSuites struct {
	XMLName xml.Name         `xml:"testsuites"`
	Suites  []JUnitTestSuite `xml:"testsuite"`
}

// JUnitTestSuite corresponds to one scenario.
type JUnitTestSuite struct {
	XMLName  xml.Name        `xml:"testsuite"`
	Name     string          `xml:"name,attr"`
	Tests    int             `xml:"tests,attr"`
	Failures int             `xml:"failures,attr"`
	Time     float64         `xml:"time,attr"`
	Cases    []JUnitTestCase `xml:"testcase"`
}

// JUnitTestCase corresponds to one action.
type JUnitTestCase struct {
	XMLName   xml.Name      `xml:"testcase"`
	Name      string        `xml:"name,attr"`
	ClassName string        `xml:"classname,attr"`
	Time      float64       `xml:"time,attr"`
	Failure   *JUnitFailure `xml:"failure,omitempty"`
}

// JUnitFailure describes a test failure.
type JUnitFailure struct {
	Message string `xml:"message,attr"`
	Content string `xml:",chardata"`
}

// WriteJUnitXML writes JUnit XML output to the given path.
func WriteJUnitXML(result *ScenarioResult, path string) error {
	suite := JUnitTestSuite{
		Name: result.Name,
		Time: result.Duration.Seconds(),
	}

	for _, pr := range result.Phases {
		for _, ar := range pr.Actions {
			suite.Tests++
			tc := JUnitTestCase{
				Name:      fmt.Sprintf("%s/%s", pr.Name, ar.Action),
				ClassName: result.Name,
				Time:      ar.Duration.Seconds(),
			}
			if ar.Status == StatusFail {
				suite.Failures++
				tc.Failure = &JUnitFailure{
					Message: ar.Error,
					Content: ar.Output,
				}
			}
			suite.Cases = append(suite.Cases, tc)
		}
	}

	suites := JUnitTestSuites{Suites: []JUnitTestSuite{suite}}
	data, err := xml.MarshalIndent(suites, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal JUnit XML: %w", err)
	}

	header := []byte(xml.Header)
	data = append(header, data...)
	return os.WriteFile(path, data, 0644)
}

// --- Baseline comparison ---

// BaselineCompare compares a result against a baseline JSON file.
// Returns a list of regressions (actions that passed in baseline but fail now).
func BaselineCompare(result *ScenarioResult, baselinePath string) ([]string, error) {
	data, err := os.ReadFile(baselinePath)
	if err != nil {
		return nil, fmt.Errorf("read baseline: %w", err)
	}
	var baseline ScenarioResult
	if err := json.Unmarshal(data, &baseline); err != nil {
		return nil, fmt.Errorf("parse baseline: %w", err)
	}

	// Build map of baseline action statuses.
	baseActions := make(map[string]ResultStatus)
	for _, pr := range baseline.Phases {
		for _, ar := range pr.Actions {
			key := fmt.Sprintf("%s/%s", pr.Name, ar.Action)
			baseActions[key] = ar.Status
		}
	}

	var regressions []string
	for _, pr := range result.Phases {
		for _, ar := range pr.Actions {
			key := fmt.Sprintf("%s/%s", pr.Name, ar.Action)
			if baseStatus, ok := baseActions[key]; ok {
				if baseStatus == StatusPass && ar.Status == StatusFail {
					regressions = append(regressions, fmt.Sprintf("%s: was PASS, now FAIL: %s", key, ar.Error))
				}
			}
		}
	}
	return regressions, nil
}
