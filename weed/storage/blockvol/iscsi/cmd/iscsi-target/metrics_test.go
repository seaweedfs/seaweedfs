package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestMetrics_WriteIncrementsCounter writes 10 blocks and verifies
// write_ops_total >= 10 in the Prometheus output.
func TestMetrics_WriteIncrementsCounter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metrics.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	reg := prometheus.NewRegistry()
	inner := &blockVolAdapter{vol: vol, tpgID: 1}
	m := newMetricsAdapter(inner, vol, reg)

	// Write 10 blocks.
	data := make([]byte, 4096)
	for i := 0; i < 10; i++ {
		data[0] = byte(i)
		if err := m.WriteAt(uint64(i), data); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Gather metrics.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "seaweedfs_blockvol_write_ops_total" {
			val := f.GetMetric()[0].GetCounter().GetValue()
			if val < 10 {
				t.Fatalf("write_ops_total: expected >= 10, got %v", val)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("seaweedfs_blockvol_write_ops_total not found in metrics")
	}
}

// TestMetrics_EndpointServes starts the admin server and verifies that
// GET /metrics returns 200 with prometheus text format.
func TestMetrics_EndpointServes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "endpoint.blk")

	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	reg := prometheus.NewRegistry()
	inner := &blockVolAdapter{vol: vol, tpgID: 1}
	_ = newMetricsAdapter(inner, vol, reg)

	adm := newAdminServer(vol, "", log.New(os.Stderr, "[test] ", 0))
	adm.metricsRegistry = reg
	ln, err := startAdminServer("127.0.0.1:0", adm)
	if err != nil {
		t.Fatalf("start admin: %v", err)
	}
	defer ln.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "seaweedfs_blockvol_write_ops_total") {
		t.Fatal("expected write_ops_total in metrics output")
	}
	if !strings.Contains(string(body), "seaweedfs_blockvol_wal_used_fraction") {
		t.Fatal("expected wal_used_fraction in metrics output")
	}
}
