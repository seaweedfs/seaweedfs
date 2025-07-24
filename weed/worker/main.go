package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker"
)

var (
	workerID  = flag.String("worker.id", "", "Worker ID (required)")
	adminAddr = flag.String("admin.address", "localhost:9090", "Admin server address")
	grpcAddr  = flag.String("grpc.address", "localhost:18000", "Worker gRPC address")
	logLevel  = flag.Int("log.level", 1, "Log level (0-4)")
)

func main() {
	flag.Parse()

	// Validate required flags
	if *workerID == "" {
		fmt.Fprintf(os.Stderr, "Error: worker.id is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Set log level
	flag.Set("v", fmt.Sprintf("%d", *logLevel))

	glog.Infof("Starting SeaweedFS EC Worker")
	glog.Infof("Worker ID: %s", *workerID)
	glog.Infof("Admin Address: %s", *adminAddr)
	glog.Infof("gRPC Address: %s", *grpcAddr)

	// Create worker
	ecWorker := worker.NewECWorker(*workerID, *adminAddr, *grpcAddr)

	// Start worker
	err := ecWorker.Start()
	if err != nil {
		glog.Fatalf("Failed to start worker: %v", err)
	}

	// Wait for shutdown signal
	waitForShutdown(ecWorker)

	glog.Infof("Worker %s shutdown complete", *workerID)
}

// waitForShutdown waits for shutdown signal and gracefully stops the worker
func waitForShutdown(worker *worker.ECWorker) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	glog.Infof("Shutdown signal received, stopping worker...")

	worker.Stop()

	// Give a moment for cleanup
	time.Sleep(2 * time.Second)
}
