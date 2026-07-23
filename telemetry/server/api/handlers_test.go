package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/telemetry/server/storage"
	protobuf "google.golang.org/protobuf/proto"
)

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
	// promauto registers on the global registry: one storage per test binary.
	h := NewHandler(storage.NewPrometheusStorage())

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
