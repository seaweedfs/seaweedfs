package testrunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteHTMLReport_Basic(t *testing.T) {
	result := &ScenarioResult{
		Name:     "html-test",
		Status:   StatusPass,
		Duration: 2500 * time.Millisecond,
		Phases: []PhaseResult{
			{
				Name:     "setup",
				Status:   StatusPass,
				Duration: 1000 * time.Millisecond,
				Actions: []ActionResult{
					{Action: "build_deploy", Status: StatusPass, Duration: 800 * time.Millisecond},
					{Action: "start_target", Status: StatusPass, Duration: 200 * time.Millisecond},
				},
			},
			{
				Name:     "verify",
				Status:   StatusPass,
				Duration: 500 * time.Millisecond,
				Actions: []ActionResult{
					{Action: "assert_equal", Status: StatusPass, Duration: 5 * time.Millisecond},
				},
			},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")

	if err := WriteHTMLReport(result, path); err != nil {
		t.Fatalf("WriteHTMLReport: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	html := string(data)
	if !strings.Contains(html, "html-test") {
		t.Error("HTML should contain scenario name")
	}
	if !strings.Contains(html, "PASS") {
		t.Error("HTML should contain PASS status")
	}
	if !strings.Contains(html, "build_deploy") {
		t.Error("HTML should contain action name")
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("HTML should be valid HTML document")
	}
}

func TestWriteHTMLReport_WithFailure(t *testing.T) {
	result := &ScenarioResult{
		Name:     "fail-html",
		Status:   StatusFail,
		Duration: 300 * time.Millisecond,
		Error:    "test <broke> & failed",
		Phases: []PhaseResult{
			{
				Name:   "main",
				Status: StatusFail,
				Actions: []ActionResult{
					{Action: "bad_step", Status: StatusFail, Error: "something <bad> happened"},
				},
			},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")

	if err := WriteHTMLReport(result, path); err != nil {
		t.Fatalf("WriteHTMLReport: %v", err)
	}

	data, _ := os.ReadFile(path)
	html := string(data)

	if !strings.Contains(html, "FAIL") {
		t.Error("HTML should contain FAIL")
	}
	// Verify HTML escaping (review item #5).
	if strings.Contains(html, "<broke>") {
		t.Error("HTML should escape angle brackets in error text")
	}
	if !strings.Contains(html, "&lt;broke&gt;") {
		t.Error("HTML should contain escaped angle brackets")
	}
}

func TestWriteHTMLReport_WithPerfAndMetrics(t *testing.T) {
	result := &ScenarioResult{
		Name:     "perf-html",
		Status:   StatusPass,
		Duration: 1 * time.Second,
		Phases:   []PhaseResult{{Name: "bench", Status: StatusPass}},
		Vars: map[string]string{
			"iops":    `{"count":100,"min":9000,"max":15000,"mean":12345,"stddev":500,"p50":12200,"p90":13500,"p99":14000}`,
			"metrics": `{"target":"primary","metrics":{"blockvol_write_bytes_total":1234567,"blockvol_read_bytes_total":987654}}`,
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")

	if err := WriteHTMLReport(result, path); err != nil {
		t.Fatalf("WriteHTMLReport: %v", err)
	}

	data, _ := os.ReadFile(path)
	html := string(data)

	if !strings.Contains(html, "Performance") {
		t.Error("HTML should contain Performance section")
	}
	if !strings.Contains(html, "iops") {
		t.Error("HTML should contain perf metric name")
	}
	if !strings.Contains(html, "Metrics") {
		t.Error("HTML should contain Metrics section")
	}
	if !strings.Contains(html, "blockvol_write_bytes_total") {
		t.Error("HTML should contain metric name")
	}
}

func TestWriteHTMLReport_WithArtifacts(t *testing.T) {
	result := &ScenarioResult{
		Name:     "artifacts-html",
		Status:   StatusFail,
		Duration: 500 * time.Millisecond,
		Artifacts: []ArtifactEntry{
			{Agent: "m01", Path: "/tmp/artifacts/m01/target.log", Size: 4096},
			{Agent: "m02", Path: "/tmp/artifacts/m02/dmesg.log", Size: 1024},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "report.html")

	if err := WriteHTMLReport(result, path); err != nil {
		t.Fatalf("WriteHTMLReport: %v", err)
	}

	data, _ := os.ReadFile(path)
	html := string(data)

	if !strings.Contains(html, "Artifacts") {
		t.Error("HTML should contain Artifacts section")
	}
	if !strings.Contains(html, "target.log") {
		t.Error("HTML should contain artifact path")
	}
	if !strings.Contains(html, "4096 bytes") {
		t.Error("HTML should contain artifact size")
	}
}
