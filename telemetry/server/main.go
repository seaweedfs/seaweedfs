package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	maxInstanceAge  = flag.Duration("max-age", 90*24*time.Hour, "Maximum age for instances before cleanup")
	stateFile       = flag.String("state-file", "data/telemetry-state.json", "File for persisting in-memory state across restarts (empty to disable)")
	saveInterval    = flag.Duration("state-save", time.Hour, "How often to save changed state to -state-file")
)

func main() {
	flag.Parse()

	// Create Prometheus storage instance
	store := storage.NewPrometheusStorage()

	// Restore state from the previous run and keep saving it periodically,
	// so deploys and restarts don't reset the collected metrics
	if *stateFile != "" {
		if n, err := store.LoadState(*stateFile); err != nil {
			log.Printf("Failed to load state from %s: %v", *stateFile, err)
		} else if n > 0 {
			log.Printf("Restored %d instances from %s", n, *stateFile)
		}
		go func() {
			ticker := time.NewTicker(*saveInterval)
			defer ticker.Stop()
			for range ticker.C {
				if err := store.SaveStateIfDirty(*stateFile); err != nil {
					log.Printf("Failed to save state to %s: %v", *stateFile, err)
				}
			}
		}()
	}

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
	mux.HandleFunc("/api/history", corsMiddleware(logMiddleware(apiHandler.GetHistory)))
	mux.HandleFunc("/api/cluster-sizes", corsMiddleware(logMiddleware(apiHandler.GetClusterSizes)))

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

	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// On SIGTERM/SIGINT, stop serving and save state before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	if *stateFile != "" {
		if err := store.SaveStateIfDirty(*stateFile); err != nil {
			log.Printf("Failed to save state to %s: %v", *stateFile, err)
		}
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
