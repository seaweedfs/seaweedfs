//go:build integration

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPerf(t *testing.T) {
	if *flagEnv != "remote" {
		t.Skip("perf tests require remote mode (-env remote)")
	}
	if !clientNode.HasCommand("fio") {
		t.Skip("fio required")
	}

	t.Run("GoBench", testPerfGoBench)
	t.Run("FioRandWrite", testPerfFioRandWrite)
	t.Run("FioRandRead", testPerfFioRandRead)
	t.Run("LatencyP99", testPerfLatencyP99)
}

func testPerfGoBench(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	benchDir := "/opt/work/seaweedfs/weed/storage/blockvol"
	stdout, stderr, code, err := targetNode.Run(ctx,
		fmt.Sprintf("cd %s && go test -run=^$ -bench=. -benchmem -count=1 -timeout=5m ./...", benchDir))
	if err != nil || code != 0 {
		t.Fatalf("go bench: code=%d stderr=%s err=%v", code, stderr, err)
	}

	t.Log(stdout)
	artifacts.CollectPerf(t, "gobench", stdout)
}

func testPerfFioRandWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "1G", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=randwrite --filename=%s --rw=randwrite "+
			"--bs=4k --size=500M --direct=1 --ioengine=libaio --iodepth=32 "+
			"--numjobs=4 --runtime=120 --time_based --group_reporting "+
			"--output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}

	iops := extractIOPS(stdout, "write")
	t.Logf("random write IOPS: %.0f", iops)
	if iops < 10000 {
		t.Fatalf("IOPS %.0f below threshold 10000", iops)
	}

	artifacts.CollectPerf(t, "fio-randwrite", stdout)
}

func testPerfFioRandRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "1G", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	// Pre-fill
	clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=prefill --filename=%s --rw=write --bs=1M "+
			"--size=500M --direct=1 --ioengine=libaio", dev))

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=randread --filename=%s --rw=randread "+
			"--bs=4k --size=500M --direct=1 --ioengine=libaio --iodepth=32 "+
			"--numjobs=4 --runtime=120 --time_based --group_reporting "+
			"--output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}

	iops := extractIOPS(stdout, "read")
	t.Logf("random read IOPS: %.0f", iops)
	if iops < 10000 {
		t.Fatalf("IOPS %.0f below threshold 10000", iops)
	}

	artifacts.CollectPerf(t, "fio-randread", stdout)
}

func testPerfLatencyP99(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "1G", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("fio --name=latency --filename=%s --rw=randwrite "+
			"--bs=4k --size=500M --direct=1 --ioengine=libaio --iodepth=1 "+
			"--numjobs=1 --runtime=60 --time_based "+
			"--lat_percentiles=1 --output-format=json", dev))
	if code != 0 {
		t.Fatalf("fio: code=%d stderr=%s", code, stderr)
	}

	p99 := extractP99Latency(stdout) // nanoseconds (fio clat_ns)
	p99ms := p99 / 1_000_000       // ns -> ms
	t.Logf("P99 latency: %.2f ms", p99ms)
	if p99ms > 10 {
		t.Fatalf("P99 %.2fms exceeds 10ms threshold", p99ms)
	}

	artifacts.CollectPerf(t, "fio-latency", stdout)
}

// extractIOPS parses fio JSON output for IOPS.
func extractIOPS(fioJSON string, rw string) float64 {
	var result struct {
		Jobs []struct {
			Read  struct{ IOPS float64 `json:"iops"` } `json:"read"`
			Write struct{ IOPS float64 `json:"iops"` } `json:"write"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(fioJSON), &result); err != nil {
		return 0
	}
	if len(result.Jobs) == 0 {
		return 0
	}
	if rw == "read" {
		return result.Jobs[0].Read.IOPS
	}
	return result.Jobs[0].Write.IOPS
}

// extractP99Latency parses fio JSON output for P99 latency in microseconds.
func extractP99Latency(fioJSON string) float64 {
	// Look for clat_ns percentile 99.000000
	idx := strings.Index(fioJSON, "99.000000")
	if idx < 0 {
		return 0
	}
	// Find the value after the colon
	sub := fioJSON[idx:]
	colonIdx := strings.Index(sub, ":")
	if colonIdx < 0 {
		return 0
	}
	valStr := strings.TrimSpace(sub[colonIdx+1:])
	// Take until comma or closing bracket
	for i, c := range valStr {
		if c == ',' || c == '}' || c == ']' {
			valStr = valStr[:i]
			break
		}
	}
	var val float64
	fmt.Sscanf(strings.TrimSpace(valStr), "%f", &val)
	return val
}
