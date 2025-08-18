// Package main provides a test client for the RDMA engine integration
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"seaweedfs-rdma-sidecar/pkg/rdma"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	socketPath string
	debug      bool
	timeout    time.Duration
	volumeID   uint32
	needleID   uint64
	cookie     uint32
	offset     uint64
	size       uint64
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "test-rdma",
		Short: "Test client for SeaweedFS RDMA engine integration",
		Long: `Test client that demonstrates communication between Go sidecar and Rust RDMA engine.
		
This tool allows you to test various RDMA operations including:
- Engine connectivity and capabilities
- RDMA read operations with mock data
- Performance measurements
- IPC protocol validation`,
	}

	// Global flags
	defaultSocketPath := os.Getenv("RDMA_SOCKET_PATH")
	if defaultSocketPath == "" {
		defaultSocketPath = "/tmp/rdma-engine.sock"
	}
	rootCmd.PersistentFlags().StringVarP(&socketPath, "socket", "s", defaultSocketPath, "Path to RDMA engine Unix socket (env: RDMA_SOCKET_PATH)")
	rootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Enable debug logging")
	rootCmd.PersistentFlags().DurationVarP(&timeout, "timeout", "t", 30*time.Second, "Operation timeout")

	// Subcommands
	rootCmd.AddCommand(pingCmd())
	rootCmd.AddCommand(capsCmd())
	rootCmd.AddCommand(readCmd())
	rootCmd.AddCommand(benchCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func pingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ping",
		Short: "Test connectivity to RDMA engine",
		Long:  "Send a ping message to the RDMA engine and measure latency",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := createClient()
			defer client.Disconnect()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("🏓 Pinging RDMA engine at %s...\n", socketPath)

			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect: %w", err)
			}

			latency, err := client.Ping(ctx)
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}

			fmt.Printf("✅ Ping successful! Latency: %v\n", latency)
			return nil
		},
	}
}

func capsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "capabilities",
		Short: "Get RDMA engine capabilities",
		Long:  "Query the RDMA engine for its current capabilities and status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := createClient()
			defer client.Disconnect()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("🔍 Querying RDMA engine capabilities...\n")

			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect: %w", err)
			}

			caps := client.GetCapabilities()
			if caps == nil {
				return fmt.Errorf("no capabilities received")
			}

			fmt.Printf("\n📊 RDMA Engine Capabilities:\n")
			fmt.Printf("  Version: %s\n", caps.Version)
			fmt.Printf("  Max Sessions: %d\n", caps.MaxSessions)
			fmt.Printf("  Max Transfer Size: %d bytes (%.1f MB)\n", caps.MaxTransferSize, float64(caps.MaxTransferSize)/(1024*1024))
			fmt.Printf("  Active Sessions: %d\n", caps.ActiveSessions)
			fmt.Printf("  Real RDMA: %t\n", caps.RealRdma)
			fmt.Printf("  Port GID: %s\n", caps.PortGid)
			fmt.Printf("  Port LID: %d\n", caps.PortLid)
			fmt.Printf("  Supported Auth: %v\n", caps.SupportedAuth)

			if caps.RealRdma {
				fmt.Printf("🚀 Hardware RDMA enabled!\n")
			} else {
				fmt.Printf("🟡 Using mock RDMA (development mode)\n")
			}

			return nil
		},
	}
}

func readCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Test RDMA read operation",
		Long:  "Perform a test RDMA read operation with specified parameters",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := createClient()
			defer client.Disconnect()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("📖 Testing RDMA read operation...\n")
			fmt.Printf("  Volume ID: %d\n", volumeID)
			fmt.Printf("  Needle ID: %d\n", needleID)
			fmt.Printf("  Cookie: 0x%x\n", cookie)
			fmt.Printf("  Offset: %d\n", offset)
			fmt.Printf("  Size: %d bytes\n", size)

			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect: %w", err)
			}

			start := time.Now()
			resp, err := client.ReadRange(ctx, volumeID, needleID, cookie, offset, size)
			if err != nil {
				return fmt.Errorf("read failed: %w", err)
			}

			duration := time.Since(start)

			fmt.Printf("\n✅ RDMA read completed successfully!\n")
			fmt.Printf("  Session ID: %s\n", resp.SessionID)
			fmt.Printf("  Bytes Read: %d\n", resp.BytesRead)
			fmt.Printf("  Duration: %v\n", duration)
			fmt.Printf("  Transfer Rate: %.2f MB/s\n", resp.TransferRate)
			fmt.Printf("  Success: %t\n", resp.Success)
			fmt.Printf("  Message: %s\n", resp.Message)

			// Show first few bytes of data for verification
			if len(resp.Data) > 0 {
				displayLen := 32
				if len(resp.Data) < displayLen {
					displayLen = len(resp.Data)
				}
				fmt.Printf("  Data (first %d bytes): %x\n", displayLen, resp.Data[:displayLen])
			}

			return nil
		},
	}

	cmd.Flags().Uint32VarP(&volumeID, "volume", "v", 1, "Volume ID")
	cmd.Flags().Uint64VarP(&needleID, "needle", "n", 100, "Needle ID")
	cmd.Flags().Uint32VarP(&cookie, "cookie", "c", 0x12345678, "Needle cookie")
	cmd.Flags().Uint64VarP(&offset, "offset", "o", 0, "Read offset")
	cmd.Flags().Uint64VarP(&size, "size", "z", 4096, "Read size in bytes")

	return cmd
}

func benchCmd() *cobra.Command {
	var (
		iterations int
		readSize   uint64
	)

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark RDMA read performance",
		Long:  "Run multiple RDMA read operations and measure performance statistics",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := createClient()
			defer client.Disconnect()

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			fmt.Printf("🏁 Starting RDMA read benchmark...\n")
			fmt.Printf("  Iterations: %d\n", iterations)
			fmt.Printf("  Read Size: %d bytes\n", readSize)
			fmt.Printf("  Socket: %s\n", socketPath)

			if err := client.Connect(ctx); err != nil {
				return fmt.Errorf("failed to connect: %w", err)
			}

			// Warmup
			fmt.Printf("🔥 Warming up...\n")
			for i := 0; i < 5; i++ {
				_, err := client.ReadRange(ctx, 1, uint64(i+1), 0x12345678, 0, readSize)
				if err != nil {
					return fmt.Errorf("warmup read %d failed: %w", i+1, err)
				}
			}

			// Benchmark
			fmt.Printf("📊 Running benchmark...\n")
			var totalDuration time.Duration
			var totalBytes uint64
			successful := 0

			startTime := time.Now()
			for i := 0; i < iterations; i++ {
				opStart := time.Now()
				resp, err := client.ReadRange(ctx, 1, uint64(i+1), 0x12345678, 0, readSize)
				opDuration := time.Since(opStart)

				if err != nil {
					fmt.Printf("❌ Read %d failed: %v\n", i+1, err)
					continue
				}

				totalDuration += opDuration
				totalBytes += resp.BytesRead
				successful++

				if (i+1)%10 == 0 || i == iterations-1 {
					fmt.Printf("  Completed %d/%d reads\n", i+1, iterations)
				}
			}
			benchDuration := time.Since(startTime)

			// Calculate statistics
			avgLatency := totalDuration / time.Duration(successful)
			throughputMBps := float64(totalBytes) / benchDuration.Seconds() / (1024 * 1024)
			opsPerSec := float64(successful) / benchDuration.Seconds()

			fmt.Printf("\n📈 Benchmark Results:\n")
			fmt.Printf("  Total Duration: %v\n", benchDuration)
			fmt.Printf("  Successful Operations: %d/%d (%.1f%%)\n", successful, iterations, float64(successful)/float64(iterations)*100)
			fmt.Printf("  Total Bytes Transferred: %d (%.1f MB)\n", totalBytes, float64(totalBytes)/(1024*1024))
			fmt.Printf("  Average Latency: %v\n", avgLatency)
			fmt.Printf("  Throughput: %.2f MB/s\n", throughputMBps)
			fmt.Printf("  Operations/sec: %.1f\n", opsPerSec)

			return nil
		},
	}

	cmd.Flags().IntVarP(&iterations, "iterations", "i", 100, "Number of read operations")
	cmd.Flags().Uint64VarP(&readSize, "read-size", "r", 4096, "Size of each read in bytes")

	return cmd
}

func createClient() *rdma.Client {
	logger := logrus.New()
	if debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	config := &rdma.Config{
		EngineSocketPath: socketPath,
		DefaultTimeout:   timeout,
		Logger:           logger,
	}

	return rdma.NewClient(config)
}
