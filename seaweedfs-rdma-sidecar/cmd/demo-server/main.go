// Package main provides a demonstration server showing SeaweedFS RDMA integration
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"seaweedfs-rdma-sidecar/pkg/seaweedfs"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	port            int
	rdmaSocket      string
	volumeServerURL string
	enableRDMA      bool
	enableZeroCopy  bool
	tempDir         string
	enablePooling   bool
	maxConnections  int
	maxIdleTime     time.Duration
	debug           bool
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "demo-server",
		Short: "SeaweedFS RDMA integration demonstration server",
		Long: `Demonstration server that shows how SeaweedFS can integrate with the RDMA sidecar
for accelerated read operations. This server provides HTTP endpoints that demonstrate
the RDMA fast path with HTTP fallback capabilities.`,
		RunE: runServer,
	}

	rootCmd.Flags().IntVarP(&port, "port", "p", 8080, "Demo server HTTP port")
	rootCmd.Flags().StringVarP(&rdmaSocket, "rdma-socket", "r", "/tmp/rdma-engine.sock", "Path to RDMA engine Unix socket")
	rootCmd.Flags().StringVarP(&volumeServerURL, "volume-server", "v", "http://localhost:8080", "SeaweedFS volume server URL for HTTP fallback")
	rootCmd.Flags().BoolVarP(&enableRDMA, "enable-rdma", "e", true, "Enable RDMA acceleration")
	rootCmd.Flags().BoolVarP(&enableZeroCopy, "enable-zerocopy", "z", true, "Enable zero-copy optimization via temp files")
	rootCmd.Flags().StringVarP(&tempDir, "temp-dir", "t", "/tmp/rdma-cache", "Temp directory for zero-copy files")
	rootCmd.Flags().BoolVar(&enablePooling, "enable-pooling", true, "Enable RDMA connection pooling")
	rootCmd.Flags().IntVar(&maxConnections, "max-connections", 10, "Maximum connections in RDMA pool")
	rootCmd.Flags().DurationVar(&maxIdleTime, "max-idle-time", 5*time.Minute, "Maximum idle time for pooled connections")
	rootCmd.Flags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) error {
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
		"port":              port,
		"rdma_socket":       rdmaSocket,
		"volume_server_url": volumeServerURL,
		"enable_rdma":       enableRDMA,
		"enable_zerocopy":   enableZeroCopy,
		"temp_dir":          tempDir,
		"enable_pooling":    enablePooling,
		"max_connections":   maxConnections,
		"max_idle_time":     maxIdleTime,
		"debug":             debug,
	}).Info("🚀 Starting SeaweedFS RDMA Demo Server")

	// Create SeaweedFS RDMA client
	config := &seaweedfs.Config{
		RDMASocketPath:  rdmaSocket,
		VolumeServerURL: volumeServerURL,
		Enabled:         enableRDMA,
		DefaultTimeout:  30 * time.Second,
		Logger:          logger,
		TempDir:         tempDir,
		UseZeroCopy:     enableZeroCopy,
		EnablePooling:   enablePooling,
		MaxConnections:  maxConnections,
		MaxIdleTime:     maxIdleTime,
	}

	rdmaClient, err := seaweedfs.NewSeaweedFSRDMAClient(config)
	if err != nil {
		return fmt.Errorf("failed to create RDMA client: %w", err)
	}

	// Start RDMA client
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := rdmaClient.Start(ctx); err != nil {
		logger.WithError(err).Error("Failed to start RDMA client")
	}
	cancel()

	// Create demo server
	server := &DemoServer{
		rdmaClient: rdmaClient,
		logger:     logger,
	}

	// Setup HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/", server.homeHandler)
	mux.HandleFunc("/health", server.healthHandler)
	mux.HandleFunc("/stats", server.statsHandler)
	mux.HandleFunc("/read", server.readHandler)
	mux.HandleFunc("/benchmark", server.benchmarkHandler)
	mux.HandleFunc("/cleanup", server.cleanupHandler)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.WithField("port", port).Info("🌐 Demo server starting")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Fatal("HTTP server failed")
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("📡 Received shutdown signal, gracefully shutting down...")

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.WithError(err).Error("HTTP server shutdown failed")
	} else {
		logger.Info("🌐 HTTP server shutdown complete")
	}

	// Stop RDMA client
	rdmaClient.Stop()
	logger.Info("🛑 Demo server shutdown complete")

	return nil
}

// DemoServer demonstrates SeaweedFS RDMA integration
type DemoServer struct {
	rdmaClient *seaweedfs.SeaweedFSRDMAClient
	logger     *logrus.Logger
}

// homeHandler provides information about the demo server
func (s *DemoServer) homeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `<!DOCTYPE html>
<html>
<head>
    <title>SeaweedFS RDMA Demo Server</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }
        .container { max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; }
        .endpoint { margin: 20px 0; padding: 15px; background: #ecf0f1; border-radius: 4px; }
        .endpoint h3 { margin: 0 0 10px 0; color: #34495e; }
        .endpoint a { color: #3498db; text-decoration: none; }
        .endpoint a:hover { text-decoration: underline; }
        .status { padding: 10px; border-radius: 4px; margin: 10px 0; }
        .status.enabled { background: #d5f4e6; color: #27ae60; }
        .status.disabled { background: #fadbd8; color: #e74c3c; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 SeaweedFS RDMA Demo Server</h1>
        <p>This server demonstrates SeaweedFS integration with RDMA acceleration for high-performance reads.</p>
        
        <div class="status %s">
            <strong>RDMA Status:</strong> %s
        </div>

        <h2>📋 Available Endpoints</h2>
        
        <div class="endpoint">
            <h3>🏥 Health Check</h3>
            <p><a href="/health">/health</a> - Check server and RDMA engine health</p>
        </div>

        <div class="endpoint">
            <h3>📊 Statistics</h3>
            <p><a href="/stats">/stats</a> - Get RDMA client statistics and capabilities</p>
        </div>

        <div class="endpoint">
            <h3>📖 Read Needle</h3>
            <p><a href="/read?file_id=3,01637037d6&size=1024&volume_server=http://localhost:8080">/read</a> - Read a needle with RDMA fast path</p>
            <p><strong>Parameters:</strong> file_id OR (volume, needle, cookie), volume_server, offset (optional), size (optional)</p>
        </div>

        <div class="endpoint">
            <h3>🏁 Benchmark</h3>
            <p><a href="/benchmark?iterations=10&size=4096">/benchmark</a> - Run performance benchmark</p>
            <p><strong>Parameters:</strong> iterations (default: 10), size (default: 4096)</p>
        </div>

        <h2>📝 Example Usage</h2>
        <pre>
# Read a needle using file ID (recommended)
curl "http://localhost:%d/read?file_id=3,01637037d6&size=1024&volume_server=http://localhost:8080"

# Read a needle using individual parameters (legacy)
curl "http://localhost:%d/read?volume=1&needle=12345&cookie=305419896&size=1024&volume_server=http://localhost:8080"

# Read a needle (hex cookie)
curl "http://localhost:%d/read?volume=1&needle=12345&cookie=0x12345678&size=1024&volume_server=http://localhost:8080"

# Run benchmark
curl "http://localhost:%d/benchmark?iterations=5&size=2048"

# Check health
curl "http://localhost:%d/health"
        </pre>
    </div>
</body>
</html>`,
		map[bool]string{true: "enabled", false: "disabled"}[s.rdmaClient.IsEnabled()],
		map[bool]string{true: "RDMA Enabled ✅", false: "RDMA Disabled (HTTP Fallback Only) ⚠️"}[s.rdmaClient.IsEnabled()],
		port, port, port, port)
}

// healthHandler checks server and RDMA health
func (s *DemoServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"rdma": map[string]interface{}{
			"enabled":   false,
			"connected": false,
		},
	}

	if s.rdmaClient != nil {
		health["rdma"].(map[string]interface{})["enabled"] = s.rdmaClient.IsEnabled()
		health["rdma"].(map[string]interface{})["type"] = "local"

		if s.rdmaClient.IsEnabled() {
			if err := s.rdmaClient.HealthCheck(ctx); err != nil {
				s.logger.WithError(err).Warn("RDMA health check failed")
				health["rdma"].(map[string]interface{})["error"] = err.Error()
			} else {
				health["rdma"].(map[string]interface{})["connected"] = true
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// statsHandler returns RDMA statistics
func (s *DemoServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var stats map[string]interface{}

	if s.rdmaClient != nil {
		stats = s.rdmaClient.GetStats()
		stats["client_type"] = "local"
	} else {
		stats = map[string]interface{}{
			"client_type": "none",
			"error":       "no RDMA client available",
		}
	}

	stats["timestamp"] = time.Now().Format(time.RFC3339)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// readHandler demonstrates needle reading with RDMA
func (s *DemoServer) readHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse parameters - support both file_id and individual parameters for backward compatibility
	query := r.URL.Query()
	volumeServer := query.Get("volume_server")
	fileID := query.Get("file_id")

	var volumeID, cookie uint64
	var needleID uint64
	var err error

	if fileID != "" {
		// Use file ID format (e.g., "3,01637037d6")
		// Extract individual components using existing SeaweedFS parsing
		fid, parseErr := needle.ParseFileIdFromString(fileID)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("invalid 'file_id' parameter: %v", parseErr), http.StatusBadRequest)
			return
		}
		volumeID = uint64(fid.VolumeId)
		needleID = uint64(fid.Key)
		cookie = uint64(fid.Cookie)
	} else {
		// Use individual parameters (backward compatibility)
		volumeID, err = strconv.ParseUint(query.Get("volume"), 10, 32)
		if err != nil {
			http.Error(w, "invalid 'volume' parameter", http.StatusBadRequest)
			return
		}

		needleID, err = strconv.ParseUint(query.Get("needle"), 10, 64)
		if err != nil {
			http.Error(w, "invalid 'needle' parameter", http.StatusBadRequest)
			return
		}

		// Parse cookie parameter - support both decimal and hexadecimal formats
		cookieStr := query.Get("cookie")
		if strings.HasPrefix(strings.ToLower(cookieStr), "0x") {
			// Parse as hexadecimal (remove "0x" prefix)
			cookie, err = strconv.ParseUint(cookieStr[2:], 16, 32)
		} else {
			// Parse as decimal (default)
			cookie, err = strconv.ParseUint(cookieStr, 10, 32)
		}
		if err != nil {
			http.Error(w, "invalid 'cookie' parameter (expected decimal or hex with 0x prefix)", http.StatusBadRequest)
			return
		}
	}

	var offset uint64
	if offsetStr := query.Get("offset"); offsetStr != "" {
		var parseErr error
		offset, parseErr = strconv.ParseUint(offsetStr, 10, 64)
		if parseErr != nil {
			http.Error(w, "invalid 'offset' parameter", http.StatusBadRequest)
			return
		}
	}

	var size uint64
	if sizeStr := query.Get("size"); sizeStr != "" {
		var parseErr error
		size, parseErr = strconv.ParseUint(sizeStr, 10, 64)
		if parseErr != nil {
			http.Error(w, "invalid 'size' parameter", http.StatusBadRequest)
			return
		}
	}

	if volumeServer == "" {
		http.Error(w, "volume_server parameter is required", http.StatusBadRequest)
		return
	}

	if volumeID == 0 || needleID == 0 {
		http.Error(w, "volume and needle parameters are required", http.StatusBadRequest)
		return
	}

	// Note: cookie and size can have defaults for demo purposes when user provides empty values,
	// but invalid parsing is caught above with proper error responses
	if cookie == 0 {
		cookie = 0x12345678 // Default cookie for demo
	}

	if size == 0 {
		size = 4096 // Default size
	}

	logFields := logrus.Fields{
		"volume_server": volumeServer,
		"volume_id":     volumeID,
		"needle_id":     needleID,
		"cookie":        fmt.Sprintf("0x%x", cookie),
		"offset":        offset,
		"size":          size,
	}
	if fileID != "" {
		logFields["file_id"] = fileID
	}
	s.logger.WithFields(logFields).Info("📖 Processing needle read request")

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	start := time.Now()
	req := &seaweedfs.NeedleReadRequest{
		VolumeID:     uint32(volumeID),
		NeedleID:     needleID,
		Cookie:       uint32(cookie),
		Offset:       offset,
		Size:         size,
		VolumeServer: volumeServer,
	}

	resp, err := s.rdmaClient.ReadNeedle(ctx, req)

	if err != nil {
		s.logger.WithError(err).Error("❌ Needle read failed")
		http.Error(w, fmt.Sprintf("Read failed: %v", err), http.StatusInternalServerError)
		return
	}

	duration := time.Since(start)

	s.logger.WithFields(logrus.Fields{
		"volume_id": volumeID,
		"needle_id": needleID,
		"is_rdma":   resp.IsRDMA,
		"source":    resp.Source,
		"duration":  duration,
		"data_size": len(resp.Data),
	}).Info("✅ Needle read completed")

	// Return metadata and first few bytes
	result := map[string]interface{}{
		"success":       true,
		"volume_id":     volumeID,
		"needle_id":     needleID,
		"cookie":        fmt.Sprintf("0x%x", cookie),
		"is_rdma":       resp.IsRDMA,
		"source":        resp.Source,
		"session_id":    resp.SessionID,
		"duration":      duration.String(),
		"data_size":     len(resp.Data),
		"timestamp":     time.Now().Format(time.RFC3339),
		"use_temp_file": resp.UseTempFile,
		"temp_file":     resp.TempFilePath,
	}

	// Set headers for zero-copy optimization
	if resp.UseTempFile && resp.TempFilePath != "" {
		w.Header().Set("X-Use-Temp-File", "true")
		w.Header().Set("X-Temp-File", resp.TempFilePath)
		w.Header().Set("X-Source", resp.Source)
		w.Header().Set("X-RDMA-Used", fmt.Sprintf("%t", resp.IsRDMA))

		// For zero-copy, return minimal JSON response and let client read from temp file
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
		return
	}

	// Regular response with data
	w.Header().Set("X-Source", resp.Source)
	w.Header().Set("X-RDMA-Used", fmt.Sprintf("%t", resp.IsRDMA))

	// Include first 32 bytes as hex for verification
	if len(resp.Data) > 0 {
		displayLen := 32
		if len(resp.Data) < displayLen {
			displayLen = len(resp.Data)
		}
		result["data_preview"] = fmt.Sprintf("%x", resp.Data[:displayLen])
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// benchmarkHandler runs performance benchmarks
func (s *DemoServer) benchmarkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse parameters
	query := r.URL.Query()

	iterations := 10 // default value
	if iterationsStr := query.Get("iterations"); iterationsStr != "" {
		var parseErr error
		iterations, parseErr = strconv.Atoi(iterationsStr)
		if parseErr != nil {
			http.Error(w, "invalid 'iterations' parameter", http.StatusBadRequest)
			return
		}
	}

	size := uint64(4096) // default value
	if sizeStr := query.Get("size"); sizeStr != "" {
		var parseErr error
		size, parseErr = strconv.ParseUint(sizeStr, 10, 64)
		if parseErr != nil {
			http.Error(w, "invalid 'size' parameter", http.StatusBadRequest)
			return
		}
	}

	if iterations <= 0 {
		iterations = 10
	}
	if size == 0 {
		size = 4096
	}

	s.logger.WithFields(logrus.Fields{
		"iterations": iterations,
		"size":       size,
	}).Info("🏁 Starting benchmark")

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var rdmaSuccessful, rdmaFailed, httpSuccessful, httpFailed int
	var totalDuration time.Duration
	var totalBytes uint64

	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		req := &seaweedfs.NeedleReadRequest{
			VolumeID: 1,
			NeedleID: uint64(i + 1),
			Cookie:   0x12345678,
			Offset:   0,
			Size:     size,
		}

		opStart := time.Now()
		resp, err := s.rdmaClient.ReadNeedle(ctx, req)
		opDuration := time.Since(opStart)

		if err != nil {
			httpFailed++
			continue
		}

		totalDuration += opDuration
		totalBytes += uint64(len(resp.Data))

		if resp.IsRDMA {
			rdmaSuccessful++
		} else {
			httpSuccessful++
		}
	}

	benchDuration := time.Since(startTime)

	// Calculate statistics
	totalOperations := rdmaSuccessful + httpSuccessful
	avgLatency := time.Duration(0)
	if totalOperations > 0 {
		avgLatency = totalDuration / time.Duration(totalOperations)
	}

	throughputMBps := float64(totalBytes) / benchDuration.Seconds() / (1024 * 1024)
	opsPerSec := float64(totalOperations) / benchDuration.Seconds()

	result := map[string]interface{}{
		"benchmark_results": map[string]interface{}{
			"iterations":      iterations,
			"size_per_op":     size,
			"total_duration":  benchDuration.String(),
			"successful_ops":  totalOperations,
			"failed_ops":      rdmaFailed + httpFailed,
			"rdma_ops":        rdmaSuccessful,
			"http_ops":        httpSuccessful,
			"avg_latency":     avgLatency.String(),
			"throughput_mbps": fmt.Sprintf("%.2f", throughputMBps),
			"ops_per_sec":     fmt.Sprintf("%.1f", opsPerSec),
			"total_bytes":     totalBytes,
		},
		"rdma_enabled": s.rdmaClient.IsEnabled(),
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	s.logger.WithFields(logrus.Fields{
		"iterations":      iterations,
		"successful_ops":  totalOperations,
		"rdma_ops":        rdmaSuccessful,
		"http_ops":        httpSuccessful,
		"avg_latency":     avgLatency,
		"throughput_mbps": throughputMBps,
		"ops_per_sec":     opsPerSec,
	}).Info("📊 Benchmark completed")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// cleanupHandler handles temp file cleanup requests from mount clients
func (s *DemoServer) cleanupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get temp file path from query parameters
	tempFilePath := r.URL.Query().Get("temp_file")
	if tempFilePath == "" {
		http.Error(w, "missing 'temp_file' parameter", http.StatusBadRequest)
		return
	}

	s.logger.WithField("temp_file", tempFilePath).Debug("🗑️ Processing cleanup request")

	// Use the RDMA client's cleanup method (which delegates to seaweedfs client)
	err := s.rdmaClient.CleanupTempFile(tempFilePath)
	if err != nil {
		s.logger.WithError(err).WithField("temp_file", tempFilePath).Warn("Failed to cleanup temp file")
		http.Error(w, fmt.Sprintf("cleanup failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.logger.WithField("temp_file", tempFilePath).Debug("🧹 Temp file cleanup successful")

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"success":   true,
		"message":   "temp file cleaned up successfully",
		"temp_file": tempFilePath,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	json.NewEncoder(w).Encode(response)
}
