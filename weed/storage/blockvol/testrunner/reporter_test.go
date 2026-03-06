package testrunner

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrintSummary(t *testing.T) {
	result := &ScenarioResult{
		Name:     "test-scenario",
		Status:   StatusPass,
		Duration: 1500 * time.Millisecond,
		Phases: []PhaseResult{
			{
				Name:     "setup",
				Status:   StatusPass,
				Duration: 800 * time.Millisecond,
				Actions: []ActionResult{
					{Action: "build_deploy", Status: StatusPass, Duration: 500 * time.Millisecond},
					{Action: "start_target", Status: StatusPass, Duration: 300 * time.Millisecond},
				},
			},
			{
				Name:     "verify",
				Status:   StatusPass,
				Duration: 700 * time.Millisecond,
				Actions: []ActionResult{
					{Action: "assert_equal", Status: StatusPass, Duration: 1 * time.Millisecond},
				},
			},
		},
	}

	var buf bytes.Buffer
	PrintSummary(&buf, result)
	output := buf.String()

	if !strings.Contains(output, "test-scenario") {
		t.Error("summary should contain scenario name")
	}
	if !strings.Contains(output, "PASS") {
		t.Error("summary should contain PASS")
	}
	if !strings.Contains(output, "3 actions") {
		t.Error("summary should contain action count")
	}
}

func TestPrintSummary_WithFailure(t *testing.T) {
	result := &ScenarioResult{
		Name:     "fail-scenario",
		Status:   StatusFail,
		Duration: 500 * time.Millisecond,
		Error:    "something broke",
		Phases: []PhaseResult{
			{
				Name:   "main",
				Status: StatusFail,
				Actions: []ActionResult{
					{Action: "step1", Status: StatusFail, Error: "boom"},
				},
			},
		},
	}

	var buf bytes.Buffer
	PrintSummary(&buf, result)
	output := buf.String()

	if !strings.Contains(output, "FAIL") {
		t.Error("summary should contain FAIL")
	}
	if !strings.Contains(output, "boom") {
		t.Error("summary should contain error message")
	}
}

func TestWriteJSON(t *testing.T) {
	result := &ScenarioResult{
		Name:     "json-test",
		Status:   StatusPass,
		Duration: 100 * time.Millisecond,
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")

	if err := WriteJSON(result, path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var loaded ScenarioResult
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if loaded.Name != "json-test" {
		t.Errorf("name = %q", loaded.Name)
	}
	if loaded.Status != StatusPass {
		t.Errorf("status = %s", loaded.Status)
	}
}

func TestWriteJUnitXML(t *testing.T) {
	result := &ScenarioResult{
		Name:     "junit-test",
		Status:   StatusFail,
		Duration: 200 * time.Millisecond,
		Phases: []PhaseResult{
			{
				Name:   "main",
				Status: StatusFail,
				Actions: []ActionResult{
					{Action: "ok_step", Status: StatusPass, Duration: 50 * time.Millisecond},
					{Action: "fail_step", Status: StatusFail, Duration: 100 * time.Millisecond, Error: "oops"},
				},
			},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "result.xml")

	if err := WriteJUnitXML(result, path); err != nil {
		t.Fatalf("WriteJUnitXML: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var suites JUnitTestSuites
	if err := xml.Unmarshal(data, &suites); err != nil {
		t.Fatalf("unmarshal XML: %v", err)
	}

	if len(suites.Suites) != 1 {
		t.Fatalf("suites = %d, want 1", len(suites.Suites))
	}
	suite := suites.Suites[0]
	if suite.Tests != 2 {
		t.Errorf("tests = %d, want 2", suite.Tests)
	}
	if suite.Failures != 1 {
		t.Errorf("failures = %d, want 1", suite.Failures)
	}
}

func TestPrintSummary_WithPerfTable(t *testing.T) {
	result := &ScenarioResult{
		Name:     "perf-scenario",
		Status:   StatusPass,
		Duration: 2 * time.Second,
		Phases: []PhaseResult{
			{
				Name:   "bench",
				Status: StatusPass,
				Actions: []ActionResult{
					{Action: "dd_write", Status: StatusPass, Duration: 1 * time.Second},
				},
			},
		},
		Vars: map[string]string{
			"iops": `{"count":100,"min":9000,"max":15000,"mean":12345,"stddev":500,"p50":12200,"p90":13500,"p99":14000}`,
		},
	}

	var buf bytes.Buffer
	PrintSummary(&buf, result)
	output := buf.String()

	if !strings.Contains(output, "Performance") {
		t.Error("summary should contain Performance table header")
	}
	if !strings.Contains(output, "iops") {
		t.Error("summary should contain metric name 'iops'")
	}
	if !strings.Contains(output, "12345") {
		t.Error("summary should contain mean value")
	}
}

func TestPrintSummary_WithMetricsTable(t *testing.T) {
	result := &ScenarioResult{
		Name:     "metrics-scenario",
		Status:   StatusPass,
		Duration: 1 * time.Second,
		Phases:   []PhaseResult{{Name: "m", Status: StatusPass}},
		Vars: map[string]string{
			"primary_metrics": `{"target":"primary","metrics":{"blockvol_write_bytes_total":1234567,"blockvol_read_bytes_total":987654}}`,
		},
	}

	var buf bytes.Buffer
	PrintSummary(&buf, result)
	output := buf.String()

	if !strings.Contains(output, "Metrics (primary)") {
		t.Error("summary should contain Metrics section with target name")
	}
	if !strings.Contains(output, "blockvol_write_bytes_total") {
		t.Error("summary should contain metric name")
	}
}

func TestPrintSummary_WithPerfStatsLine(t *testing.T) {
	result := &ScenarioResult{
		Name:     "perf-line-scenario",
		Status:   StatusPass,
		Duration: 1 * time.Second,
		Phases:   []PhaseResult{{Name: "p", Status: StatusPass}},
		Vars: map[string]string{
			"lat_stats": "lat_us: n=50 mean=420.5 stddev=30.1 p50=415.0 p90=460.0 p99=500.0 min=350.0 max=550.0",
		},
	}

	var buf bytes.Buffer
	PrintSummary(&buf, result)
	output := buf.String()

	if !strings.Contains(output, "Performance") {
		t.Error("summary should render perf table from FormatStats-style line")
	}
	if !strings.Contains(output, "lat_stats") {
		t.Error("summary should contain metric name from var key")
	}
}

func TestParsePerfStatsLine(t *testing.T) {
	line := "iops: n=100 mean=12345.00 stddev=500.00 p50=12200.00 p90=13500.00 p99=14000.00 min=9000.00 max=15000.00"
	ps, ok := parsePerfStatsLine(line)
	if !ok {
		t.Fatal("should parse FormatStats-style line")
	}
	if ps.Count != 100 {
		t.Errorf("count = %d, want 100", ps.Count)
	}
	if ps.Mean != 12345 {
		t.Errorf("mean = %f, want 12345", ps.Mean)
	}
	if ps.P99 != 14000 {
		t.Errorf("p99 = %f, want 14000", ps.P99)
	}
}

func TestParsePerfStatsLine_Invalid(t *testing.T) {
	_, ok := parsePerfStatsLine("not a perf line")
	if ok {
		t.Error("should not parse non-perf text")
	}
}

func TestFormatInt(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
	}
	for _, tt := range tests {
		got := formatInt(tt.n)
		if got != tt.want {
			t.Errorf("formatInt(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestWriteJSON_WithVars(t *testing.T) {
	result := &ScenarioResult{
		Name:   "json-vars-test",
		Status: StatusPass,
		Vars: map[string]string{
			"key1": "value1",
			"key2": `{"nested":"json"}`,
		},
		Artifacts: []ArtifactEntry{
			{Agent: "m01", Path: "/tmp/a/log.txt", Size: 512},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "result.json")

	if err := WriteJSON(result, path); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	data, _ := os.ReadFile(path)
	var loaded ScenarioResult
	json.Unmarshal(data, &loaded)

	if loaded.Vars["key1"] != "value1" {
		t.Errorf("vars[key1] = %q", loaded.Vars["key1"])
	}
	if loaded.Vars["key2"] != `{"nested":"json"}` {
		t.Errorf("vars[key2] = %q", loaded.Vars["key2"])
	}
	if len(loaded.Artifacts) != 1 {
		t.Fatalf("artifacts = %d, want 1", len(loaded.Artifacts))
	}
	if loaded.Artifacts[0].Agent != "m01" {
		t.Errorf("artifact agent = %q", loaded.Artifacts[0].Agent)
	}
}

func TestBaselineCompare(t *testing.T) {
	baseline := &ScenarioResult{
		Name: "baseline",
		Phases: []PhaseResult{
			{
				Name: "main",
				Actions: []ActionResult{
					{Action: "step1", Status: StatusPass},
					{Action: "step2", Status: StatusPass},
					{Action: "step3", Status: StatusFail},
				},
			},
		},
	}

	dir := t.TempDir()
	baselinePath := filepath.Join(dir, "baseline.json")
	if err := WriteJSON(baseline, baselinePath); err != nil {
		t.Fatalf("write baseline: %v", err)
	}

	current := &ScenarioResult{
		Name: "current",
		Phases: []PhaseResult{
			{
				Name: "main",
				Actions: []ActionResult{
					{Action: "step1", Status: StatusPass}, // still pass
					{Action: "step2", Status: StatusFail, Error: "regression"}, // regression!
					{Action: "step3", Status: StatusFail}, // was fail, still fail (not regression)
				},
			},
		},
	}

	regressions, err := BaselineCompare(current, baselinePath)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}

	if len(regressions) != 1 {
		t.Fatalf("regressions = %d, want 1", len(regressions))
	}
	if !strings.Contains(regressions[0], "step2") {
		t.Errorf("regression should mention step2: %s", regressions[0])
	}
}
