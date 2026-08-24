package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/telemetry/server/storage"
	protobuf "google.golang.org/protobuf/proto"
)

// promauto registers on the global registry: one storage per test binary.
var testHandler = NewHandler(storage.NewPrometheusStorage())

func validReport() *proto.TelemetryData {
	return &proto.TelemetryData{
		TopologyId:        "38422678-6a0d-4482-aa33-65b90010ac47",
		Version:           "4.40",
		Os:                "linux/amd64",
		VolumeServerCount: 5,
		TotalDiskBytes:    123456789,
		TotalVolumeCount:  42,
		FilerCount:        2,
		BrokerCount:       1,
	}
}

func postCollect(t *testing.T, h *Handler, body []byte, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/collect", bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	h.CollectTelemetry(w, req)
	return w
}

func marshalReport(t *testing.T, data *proto.TelemetryData) []byte {
	t.Helper()
	body, err := protobuf.Marshal(&proto.TelemetryRequest{Data: data})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func TestCollectTelemetryValidation(t *testing.T) {
	h := testHandler

	t.Run("valid report accepted", func(t *testing.T) {
		if w := postCollect(t, h, marshalReport(t, validReport()), "application/x-protobuf"); w.Code != http.StatusOK {
			t.Errorf("got %d, want 200: %s", w.Code, w.Body.String())
		}
	})

	t.Run("enterprise version accepted", func(t *testing.T) {
		data := validReport()
		data.Version = "4.40-enterprise"
		if w := postCollect(t, h, marshalReport(t, data), "application/x-protobuf"); w.Code != http.StatusOK {
			t.Errorf("got %d, want 200: %s", w.Code, w.Body.String())
		}
	})

	rejected := []struct {
		name   string
		mutate func(*proto.TelemetryData)
	}{
		{"non-UUID topology_id", func(d *proto.TelemetryData) { d.TopologyId = "claude-diagnostic-probe" }},
		{"empty topology_id", func(d *proto.TelemetryData) { d.TopologyId = "" }},
		{"junk version", func(d *proto.TelemetryData) { d.Version = "probe" }},
		{"version with suffix", func(d *proto.TelemetryData) { d.Version = "4.40-nightly" }},
		{"junk os", func(d *proto.TelemetryData) { d.Os = "probe/probe" }},
		{"os without slash", func(d *proto.TelemetryData) { d.Os = "linux" }},
		{"negative count", func(d *proto.TelemetryData) { d.VolumeServerCount = -1 }},
		{"absurd server count", func(d *proto.TelemetryData) { d.VolumeServerCount = 1_000_000 }},
		{"absurd disk size", func(d *proto.TelemetryData) { d.TotalDiskBytes = 1 << 62 }},
	}
	for _, tc := range rejected {
		t.Run(tc.name+" rejected", func(t *testing.T) {
			data := validReport()
			tc.mutate(data)
			if w := postCollect(t, h, marshalReport(t, data), "application/x-protobuf"); w.Code != http.StatusBadRequest {
				t.Errorf("got %d, want 400: %s", w.Code, w.Body.String())
			}
		})
	}

	t.Run("wrong content type rejected", func(t *testing.T) {
		if w := postCollect(t, h, marshalReport(t, validReport()), "application/json"); w.Code != http.StatusUnsupportedMediaType {
			t.Errorf("got %d, want 415", w.Code)
		}
	})

	t.Run("oversized body rejected", func(t *testing.T) {
		if w := postCollect(t, h, make([]byte, maxRequestBytes+1), "application/x-protobuf"); w.Code != http.StatusBadRequest {
			t.Errorf("got %d, want 400", w.Code)
		}
	})
}

// The dashboard looks confirmation windows up by the select's string value,
// so the stats JSON must key confirmed_by_days by decimal strings and carry
// unmet thresholds as zeros.
func TestStatsSerializedThresholds(t *testing.T) {
	data := validReport()
	data.TopologyId = "49533789-7b1e-4593-bb44-76ca1121bd58"
	if w := postCollect(t, testHandler, marshalReport(t, data), "application/x-protobuf"); w.Code != http.StatusOK {
		t.Fatalf("collect: got %d: %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	testHandler.GetStats(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("stats: got %d", w.Code)
	}

	var stats struct {
		ConfirmedByDays map[string]int `json:"confirmed_by_days"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(stats.ConfirmedByDays) != 5 {
		t.Fatalf("confirmed_by_days = %v, want the 5 thresholds", stats.ConfirmedByDays)
	}
	for _, key := range []string{"1", "3", "7", "14", "30"} {
		if _, ok := stats.ConfirmedByDays[key]; !ok {
			t.Errorf("confirmed_by_days missing %q: %v", key, stats.ConfirmedByDays)
		}
	}
	if stats.ConfirmedByDays["1"] < 1 {
		t.Errorf("fresh cluster missing from the 1-day count: %v", stats.ConfirmedByDays)
	}
	if stats.ConfirmedByDays["30"] != 0 {
		t.Errorf("unmet threshold not zero: %v", stats.ConfirmedByDays)
	}
}
