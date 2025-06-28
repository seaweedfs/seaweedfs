package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/telemetry/server/api"
	"github.com/seaweedfs/seaweedfs/telemetry/server/dashboard"
	"github.com/seaweedfs/seaweedfs/telemetry/server/storage"
)

var (
	port            = flag.Int("port", 8080, "HTTP server port")
	enableCORS      = flag.Bool("cors", true, "Enable CORS for dashboard")
	logRequests     = flag.Bool("log", true, "Log incoming requests")
	enableDashboard = flag.Bool("dashboard", true, "Enable built-in dashboard (optional when using Grafana)")
	cleanupInterval = flag.Duration("cleanup", 24*time.Hour, "Cleanup interval for old instances")
	maxInstanceAge  = flag.Duration("max-age", 30*24*time.Hour, "Maximum age for instances before cleanup")
)

func main() {
	flag.Parse()

	// Create Prometheus storage instance
	store := storage.NewPrometheusStorage()

	// Start cleanup routine
	go func() {
		ticker := time.NewTicker(*cleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			store.CleanupOldInstances(*maxInstanceAge)
		}
	}()

	// Setup HTTP handlers
	mux := http.NewServeMux()

	// Prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())

	// API endpoints
	apiHandler := api.NewHandler(store)
	mux.HandleFunc("/api/collect", corsMiddleware(logMiddleware(apiHandler.CollectTelemetry)))
	mux.HandleFunc("/api/stats", corsMiddleware(logMiddleware(apiHandler.GetStats)))
	mux.HandleFunc("/api/instances", corsMiddleware(logMiddleware(apiHandler.GetInstances)))
	mux.HandleFunc("/api/metrics", corsMiddleware(logMiddleware(apiHandler.GetMetrics)))

	// Dashboard (optional)
	if *enableDashboard {
		dashboardHandler := dashboard.NewHandler()
		mux.HandleFunc("/", corsMiddleware(dashboardHandler.ServeIndex))
		mux.HandleFunc("/dashboard", corsMiddleware(dashboardHandler.ServeIndex))
		mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static"))))
	}

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting telemetry server on %s", addr)
	log.Printf("Prometheus metrics: http://localhost%s/metrics", addr)
	if *enableDashboard {
		log.Printf("Dashboard: http://localhost%s/dashboard", addr)
	}
	log.Printf("Cleanup interval: %v, Max instance age: %v", *cleanupInterval, *maxInstanceAge)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if *enableCORS {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

func logMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if *logRequests {
			start := time.Now()
			next(w, r)
			log.Printf("%s %s %s %v", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
		} else {
			next(w, r)
		}
	}
}
