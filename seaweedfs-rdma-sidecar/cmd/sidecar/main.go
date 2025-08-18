// Package main provides the main RDMA sidecar service that integrates with SeaweedFS
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"seaweedfs-rdma-sidecar/pkg/rdma"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	port         int
	engineSocket string
	debug        bool
	timeout      time.Duration
)

// Response structs for JSON encoding
type HealthResponse struct {
	Status              string `json:"status"`
	RdmaEngineConnected bool   `json:"rdma_engine_connected"`
	RdmaEngineLatency   string `json:"rdma_engine_latency"`
	Timestamp           string `json:"timestamp"`
}

type CapabilitiesResponse struct {
	Version         string   `json:"version"`
	DeviceName      string   `json:"device_name"`
	VendorId        uint32   `json:"vendor_id"`
	MaxSessions     uint32   `json:"max_sessions"`
	MaxTransferSize uint64   `json:"max_transfer_size"`
	ActiveSessions  uint32   `json:"active_sessions"`
	RealRdma        bool     `json:"real_rdma"`
	PortGid         string   `json:"port_gid"`
	PortLid         uint16   `json:"port_lid"`
	SupportedAuth   []string `json:"supported_auth"`
}

type PingResponse struct {
	Success       bool   `json:"success"`
	EngineLatency string `json:"engine_latency"`
	TotalLatency  string `json:"total_latency"`
	Timestamp     string `json:"timestamp"`
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "rdma-sidecar",
		Short: "SeaweedFS RDMA acceleration sidecar",
		Long: `RDMA sidecar that accelerates SeaweedFS read/write operations using UCX and Rust RDMA engine.

This sidecar acts as a bridge between SeaweedFS volume servers and the high-performance
Rust RDMA engine, providing significant performance improvements for data-intensive workloads.`,
		RunE: runSidecar,
	}

	// Flags
	rootCmd.Flags().IntVarP(&port, "port", "p", 8081, "HTTP server port")
	rootCmd.Flags().StringVarP(&engineSocket, "engine-socket", "e", "/tmp/rdma-engine.sock", "Path to RDMA engine Unix socket")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	rootCmd.Flags().DurationVarP(&timeout, "timeout", "t", 30*time.Second, "RDMA operation timeout")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runSidecar(cmd *cobra.Command, args []string) error {
	// Setup logging
	logger := logrus.New()
	if debug {
		logger.SetLevel(logrus.DebugLevel)
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp: true,
			ForceColors:   true,
		})
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.WithFields(logrus.Fields{
		"port":          port,
		"engine_socket": engineSocket,
		"debug":         debug,
		"timeout":       timeout,
	}).Info("🚀 Starting SeaweedFS RDMA Sidecar")

	// Create RDMA client
	rdmaConfig := &rdma.Config{
		EngineSocketPath: engineSocket,
		DefaultTimeout:   timeout,
		Logger:           logger,
	}

	rdmaClient := rdma.NewClient(rdmaConfig)

	// Connect to RDMA engine
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Info("🔗 Connecting to RDMA engine...")
	if err := rdmaClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to RDMA engine: %w", err)
	}
	logger.Info("✅ Connected to RDMA engine successfully")

	// Create HTTP server
	sidecar := &Sidecar{
		rdmaClient: rdmaClient,
		logger:     logger,
	}

	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", sidecar.healthHandler)

	// RDMA operations endpoints
	mux.HandleFunc("/rdma/read", sidecar.rdmaReadHandler)
	mux.HandleFunc("/rdma/capabilities", sidecar.capabilitiesHandler)
	mux.HandleFunc("/rdma/ping", sidecar.pingHandler)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.WithField("port", port).Info("🌐 HTTP server starting")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Fatal("HTTP server failed")
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("📡 Received shutdown signal, gracefully shutting down...")

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.WithError(err).Error("HTTP server shutdown failed")
	} else {
		logger.Info("🌐 HTTP server shutdown complete")
	}

	// Disconnect from RDMA engine
	rdmaClient.Disconnect()
	logger.Info("🛑 RDMA sidecar shutdown complete")

	return nil
}

// Sidecar represents the main sidecar service
type Sidecar struct {
	rdmaClient *rdma.Client
	logger     *logrus.Logger
}

// Health check handler
func (s *Sidecar) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Test RDMA engine connectivity
	if !s.rdmaClient.IsConnected() {
		s.logger.Warn("⚠️  RDMA engine not connected")
		http.Error(w, "RDMA engine not connected", http.StatusServiceUnavailable)
		return
	}

	// Ping RDMA engine
	latency, err := s.rdmaClient.Ping(ctx)
	if err != nil {
		s.logger.WithError(err).Error("❌ RDMA engine ping failed")
		http.Error(w, "RDMA engine ping failed", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := HealthResponse{
		Status:              "healthy",
		RdmaEngineConnected: true,
		RdmaEngineLatency:   latency.String(),
		Timestamp:           time.Now().Format(time.RFC3339),
	}
	json.NewEncoder(w).Encode(response)
}

// RDMA capabilities handler
func (s *Sidecar) capabilitiesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	caps := s.rdmaClient.GetCapabilities()
	if caps == nil {
		http.Error(w, "No capabilities available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := CapabilitiesResponse{
		Version:         caps.Version,
		DeviceName:      caps.DeviceName,
		VendorId:        caps.VendorId,
		MaxSessions:     uint32(caps.MaxSessions),
		MaxTransferSize: caps.MaxTransferSize,
		ActiveSessions:  uint32(caps.ActiveSessions),
		RealRdma:        caps.RealRdma,
		PortGid:         caps.PortGid,
		PortLid:         caps.PortLid,
		SupportedAuth:   caps.SupportedAuth,
	}
	json.NewEncoder(w).Encode(response)
}

// RDMA ping handler
func (s *Sidecar) pingHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	latency, err := s.rdmaClient.Ping(ctx)
	totalLatency := time.Since(start)

	if err != nil {
		s.logger.WithError(err).Error("❌ RDMA ping failed")
		http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := PingResponse{
		Success:       true,
		EngineLatency: latency.String(),
		TotalLatency:  totalLatency.String(),
		Timestamp:     time.Now().Format(time.RFC3339),
	}
	json.NewEncoder(w).Encode(response)
}

// RDMA read handler - uses GET method with query parameters for RESTful read operations
func (s *Sidecar) rdmaReadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	query := r.URL.Query()

	// Get file ID (e.g., "3,01637037d6") - this is the natural SeaweedFS identifier
	fileID := query.Get("file_id")
	if fileID == "" {
		http.Error(w, "missing 'file_id' parameter", http.StatusBadRequest)
		return
	}

	// Parse optional offset and size parameters
	offset := uint64(0) // default value
	if offsetStr := query.Get("offset"); offsetStr != "" {
		val, err := strconv.ParseUint(offsetStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid 'offset' parameter", http.StatusBadRequest)
			return
		}
		offset = val
	}

	size := uint64(4096) // default value
	if sizeStr := query.Get("size"); sizeStr != "" {
		val, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid 'size' parameter", http.StatusBadRequest)
			return
		}
		size = val
	}

	s.logger.WithFields(logrus.Fields{
		"file_id": fileID,
		"offset":  offset,
		"size":    size,
	}).Info("📖 Processing RDMA read request")

	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()
	resp, err := s.rdmaClient.ReadFileRange(ctx, fileID, offset, size)
	duration := time.Since(start)

	if err != nil {
		s.logger.WithError(err).Error("❌ RDMA read failed")
		http.Error(w, fmt.Sprintf("RDMA read failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.logger.WithFields(logrus.Fields{
		"file_id":       fileID,
		"bytes_read":    resp.BytesRead,
		"duration":      duration,
		"transfer_rate": resp.TransferRate,
		"session_id":    resp.SessionID,
	}).Info("✅ RDMA read completed successfully")

	// Set response headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-RDMA-Session-ID", resp.SessionID)
	w.Header().Set("X-RDMA-Duration", duration.String())
	w.Header().Set("X-RDMA-Transfer-Rate", fmt.Sprintf("%.2f", resp.TransferRate))
	w.Header().Set("X-RDMA-Bytes-Read", fmt.Sprintf("%d", resp.BytesRead))

	// Write the data
	w.Write(resp.Data)
}
