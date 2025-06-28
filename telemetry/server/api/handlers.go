package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/telemetry/server/storage"
	protobuf "google.golang.org/protobuf/proto"
)

type Handler struct {
	storage *storage.PrometheusStorage
}

func NewHandler(storage *storage.PrometheusStorage) *Handler {
	return &Handler{storage: storage}
}

func (h *Handler) CollectTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	contentType := r.Header.Get("Content-Type")

	// Only accept protobuf content type
	if contentType != "application/x-protobuf" && contentType != "application/protobuf" {
		http.Error(w, "Content-Type must be application/x-protobuf", http.StatusUnsupportedMediaType)
		return
	}

	// Read protobuf request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	req := &proto.TelemetryRequest{}
	if err := protobuf.Unmarshal(body, req); err != nil {
		http.Error(w, "Invalid protobuf data", http.StatusBadRequest)
		return
	}

	data := req.Data
	if data == nil {
		http.Error(w, "Missing telemetry data", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if data.ClusterId == "" || data.Version == "" || data.Os == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Set timestamp if not provided
	if data.Timestamp == 0 {
		data.Timestamp = time.Now().Unix()
	}

	// Store the telemetry data
	if err := h.storage.StoreTelemetry(data); err != nil {
		http.Error(w, "Failed to store data", http.StatusInternalServerError)
		return
	}

	// Return protobuf response
	resp := &proto.TelemetryResponse{
		Success: true,
		Message: "Telemetry data received",
	}

	respData, err := protobuf.Marshal(resp)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	w.Write(respData)
}

func (h *Handler) GetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := h.storage.GetStats()
	if err != nil {
		http.Error(w, "Failed to get stats", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (h *Handler) GetInstances(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100 // default
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	instances, err := h.storage.GetInstances(limit)
	if err != nil {
		http.Error(w, "Failed to get instances", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(instances)
}

func (h *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	daysStr := r.URL.Query().Get("days")
	days := 30 // default
	if daysStr != "" {
		if d, err := strconv.Atoi(daysStr); err == nil && d > 0 && d <= 365 {
			days = d
		}
	}

	metrics, err := h.storage.GetMetrics(days)
	if err != nil {
		http.Error(w, "Failed to get metrics", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
