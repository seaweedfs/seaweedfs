package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

var (
	filers           = flag.String("filers", "localhost:8888", "comma-separated list of filer addresses")
	workers          = flag.Int("workers", 300, "number of concurrent workers")
	threadsPerWorker = flag.Int("threadsPerWorker", 4, "number of threads per worker")
	concurrentFiles  = flag.Int("concurrentFiles", 16, "number of files open concurrently per worker")
	filesPerWorker   = flag.Int("filesPerWorker", 4096, "total number of files each worker creates")
	chunkSize        = flag.Int64("chunkSize", 1024*1024, "chunk size in bytes")
	chunksPerFile    = flag.Int("chunksPerFile", 5, "number of chunks per file")
	testDir          = flag.String("testDir", "/benchmark", "test directory on filer")
	verbose          = flag.Bool("verbose", false, "verbose logging")
)

type BenchmarkStats struct {
	filesCreated   int64
	chunksAdded    int64
	errors         int64
	raceConditions int64
	totalDuration  time.Duration
}

type FilerClient struct {
	address string
	conn    *grpc.ClientConn
	client  filer_pb.SeaweedFilerClient
}

func main() {
	flag.Parse()

	// Configure logging based on verbose flag
	if !*verbose {
		log.SetFlags(log.LstdFlags) // Minimal logging
	}

	filerAddresses := util.StringSplit(*filers, ",")
	if len(filerAddresses) == 0 {
		log.Fatal("No filer addresses provided")
	}

	log.Printf("Starting filer benchmark: %d workers, %d threads each, %d concurrent files, %d files per worker, %d filers",
		*workers, *threadsPerWorker, *concurrentFiles, *filesPerWorker, len(filerAddresses))

	// Create filer clients
	clients, err := createFilerClients(filerAddresses)
	if err != nil {
		log.Fatalf("Failed to create filer clients: %v", err)
	}
	defer closeFilerClients(clients)

	// Ensure test directory exists
	if err := ensureDirectory(clients[0], *testDir); err != nil {
		log.Fatalf("Failed to create test directory: %v", err)
	}

	// Run benchmark
	stats := runBenchmark(clients)

	// Print results
	printResults(stats)
}

func createFilerClients(addresses []string) ([]*FilerClient, error) {
	var clients []*FilerClient

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	for _, addr := range addresses {
		conn, err := pb.GrpcDial(context.Background(), addr, true, grpcDialOption)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %v", addr, err)
		}

		client := &FilerClient{
			address: addr,
			conn:    conn,
			client:  filer_pb.NewSeaweedFilerClient(conn),
		}
		clients = append(clients, client)
	}

	return clients, nil
}

func closeFilerClients(clients []*FilerClient) {
	for _, client := range clients {
		client.conn.Close()
	}
}

func ensureDirectory(client *FilerClient, dir string) error {
	_, err := client.client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/",
		Entry: &filer_pb.Entry{
			Name:        dir[1:], // Remove leading slash
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: 0755,
			},
		},
		OExcl: false,
	})
	return err
}

func runBenchmark(clients []*FilerClient) *BenchmarkStats {
	stats := &BenchmarkStats{}
	var wg sync.WaitGroup
	startTime := time.Now()

	// Start workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(workerID, clients, stats)
		}(i)
	}

	// Wait for completion
	wg.Wait()
	stats.totalDuration = time.Since(startTime)

	return stats
}

func runWorker(workerID int, clients []*FilerClient, stats *BenchmarkStats) {
	// Create work queue with concurrency limit
	workQueue := make(chan int, *concurrentFiles)
	var workerWg sync.WaitGroup

	// Start threads for this worker
	for threadID := 0; threadID < *threadsPerWorker; threadID++ {
		workerWg.Add(1)
		go func(tID int) {
			defer workerWg.Done()
			runWorkerThread(workerID, tID, clients, stats, workQueue)
		}(threadID)
	}

	// Queue up all the file creation tasks
	go func() {
		defer close(workQueue)
		for fileID := 0; fileID < *filesPerWorker; fileID++ {
			workQueue <- fileID
		}
	}()

	// Wait for all threads in this worker to complete
	workerWg.Wait()
}

func runWorkerThread(workerID, threadID int, clients []*FilerClient, stats *BenchmarkStats, workQueue <-chan int) {
	for fileID := range workQueue {
		// Select random filer client
		client := clients[rand.Intn(len(clients))]

		// Create unique filename
		filename := fmt.Sprintf("file_%d_%d_%d_%d", workerID, threadID, fileID, time.Now().UnixNano())

		if err := createFileWithChunks(client, filename, stats); err != nil {
			atomic.AddInt64(&stats.errors, 1)
			if isRaceConditionError(err) {
				atomic.AddInt64(&stats.raceConditions, 1)
			}
			if *verbose {
				log.Printf("Worker %d Thread %d error: %v", workerID, threadID, err)
			}
		} else {
			atomic.AddInt64(&stats.filesCreated, 1)
		}

		// Small random delay to create timing variations
		if rand.Intn(10) == 0 {
			time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
		}
	}
}

func createFileWithChunks(client *FilerClient, filename string, stats *BenchmarkStats) error {
	ctx := context.Background()

	// Step 1: Create empty file
	entry := &filer_pb.Entry{
		Name: filename,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			Crtime:   time.Now().Unix(),
			FileMode: 0644,
			FileSize: 0,
		},
		Chunks: []*filer_pb.FileChunk{},
	}

	_, err := client.client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: *testDir,
		Entry:     entry,
	})
	if err != nil {
		return fmt.Errorf("create entry failed: %v", err)
	}

	// Step 2: Add chunks to the file
	var chunks []*filer_pb.FileChunk
	var offset int64 = 0

	for i := 0; i < *chunksPerFile; i++ {
		chunk := &filer_pb.FileChunk{
			FileId:       generateFakeFileId(),
			Offset:       offset,
			Size:         uint64(*chunkSize),
			ModifiedTsNs: time.Now().UnixNano(),
			ETag:         generateETag(),
		}
		chunks = append(chunks, chunk)
		offset += *chunkSize
		atomic.AddInt64(&stats.chunksAdded, 1)
	}

	// Update file with chunks
	entry.Chunks = chunks
	entry.Attributes.FileSize = uint64(offset)

	_, err = client.client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: *testDir,
		Entry:     entry,
	})
	if err != nil {
		return fmt.Errorf("update entry with chunks failed: %v", err)
	}

	// Step 3: Verify file was created properly (this may catch race conditions)
	_, err = client.client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
		Directory: *testDir,
		Name:      filename,
	})
	if err != nil {
		return fmt.Errorf("lookup after creation failed (race condition?): %v", err)
	}

	return nil
}

func generateFakeFileId() string {
	// Generate fake file ID that looks real but doesn't exist on volume servers
	volumeId := rand.Intn(100) + 1
	fileKey := rand.Int63()
	cookie := rand.Uint32()
	return fmt.Sprintf("%d,%x%08x", volumeId, fileKey, cookie)
}

func generateETag() string {
	// Generate fake ETag
	return fmt.Sprintf("%x", rand.Int63())
}

func isRaceConditionError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "leveldb: closed") ||
		strings.Contains(errStr, "transport is closing") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "not found") && strings.Contains(errStr, "after creation")
}

func printResults(stats *BenchmarkStats) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("FILER BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 60))

	totalOps := int64(*workers) * int64(*filesPerWorker)
	successRate := float64(stats.filesCreated) / float64(totalOps) * 100

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Filers: %s\n", *filers)
	fmt.Printf("  Workers: %d\n", *workers)
	fmt.Printf("  Threads per worker: %d\n", *threadsPerWorker)
	fmt.Printf("  Concurrent files per worker: %d\n", *concurrentFiles)
	fmt.Printf("  Files per worker: %d\n", *filesPerWorker)
	fmt.Printf("  Total threads: %d\n", *workers**threadsPerWorker)
	fmt.Printf("  Chunks per file: %d\n", *chunksPerFile)
	fmt.Printf("  Chunk size: %d bytes\n", *chunkSize)
	fmt.Printf("\n")

	fmt.Printf("Results:\n")
	fmt.Printf("  Total operations attempted: %d\n", totalOps)
	fmt.Printf("  Files successfully created: %d\n", stats.filesCreated)
	fmt.Printf("  Total chunks added: %d\n", stats.chunksAdded)
	fmt.Printf("  Errors: %d\n", stats.errors)
	fmt.Printf("  Race condition errors: %d\n", stats.raceConditions)
	fmt.Printf("  Success rate: %.2f%%\n", successRate)
	fmt.Printf("\n")

	fmt.Printf("Performance:\n")
	fmt.Printf("  Total duration: %v\n", stats.totalDuration)
	fmt.Printf("  Operations/second: %.2f\n", float64(totalOps)/stats.totalDuration.Seconds())
	fmt.Printf("  Files/second: %.2f\n", float64(stats.filesCreated)/stats.totalDuration.Seconds())
	fmt.Printf("  Chunks/second: %.2f\n", float64(stats.chunksAdded)/stats.totalDuration.Seconds())
	fmt.Printf("\n")

	// Race condition analysis
	fmt.Printf("Race Condition Analysis:\n")
	if stats.raceConditions > 0 {
		raceRate := float64(stats.raceConditions) / float64(totalOps) * 100
		fmt.Printf("  Race condition rate: %.4f%%\n", raceRate)
		fmt.Printf("  Race conditions detected: %d\n", stats.raceConditions)

		if raceRate > 1.0 {
			fmt.Printf("  🔴 HIGH race condition rate detected!\n")
		} else if raceRate > 0.1 {
			fmt.Printf("  🟡 MODERATE race condition rate\n")
		} else {
			fmt.Printf("  🟢 LOW race condition rate\n")
		}
	} else {
		fmt.Printf("  No race conditions detected\n")
		if stats.errors == 0 {
			fmt.Printf("  🟢 All operations completed successfully\n")
		}
	}

	if stats.errors > 0 {
		errorRate := float64(stats.errors) / float64(totalOps) * 100
		fmt.Printf("  Overall error rate: %.2f%%\n", errorRate)
	}

	fmt.Println(strings.Repeat("=", 60))

	// Recommendations
	if stats.raceConditions > 0 || stats.errors > totalOps/10 {
		fmt.Println("\nRecommendations:")
		if stats.raceConditions > 0 {
			fmt.Println("  • Race conditions detected - investigate filer concurrent access handling")
			fmt.Println("  • Check filer logs for 'leveldb: closed' or 'transport is closing' errors")
		}
		if stats.errors > totalOps/20 {
			fmt.Println("  • High error rate - check filer stability and resource limits")
		}
		fmt.Println("  • Consider running with -verbose flag for detailed error analysis")
	}
}
