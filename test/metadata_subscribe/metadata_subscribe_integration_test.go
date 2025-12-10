package metadata_subscribe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestMetadataSubscribeBasic tests basic metadata subscription functionality
func TestMetadataSubscribeBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_metadata_subscribe_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8888", 30*time.Second))

	t.Logf("SeaweedFS cluster started successfully")

	t.Run("subscribe_and_receive_events", func(t *testing.T) {
		// Create a channel to receive events
		eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 100)
		errChan := make(chan error, 1)

		// Start subscribing in a goroutine
		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()

		go func() {
			err := subscribeToMetadata(subCtx, "127.0.0.1:8888", "/", eventsChan)
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				errChan <- err
			}
		}()

		// Wait for subscription to be established
		time.Sleep(2 * time.Second)

		// Create test files via HTTP
		testFiles := []string{
			"/test/file1.txt",
			"/test/file2.txt",
			"/test/subdir/file3.txt",
		}

		for _, path := range testFiles {
			err := uploadFile("http://127.0.0.1:8888"+path, []byte("test content for "+path))
			require.NoError(t, err, "Failed to upload %s", path)
			t.Logf("Uploaded %s", path)
		}

		// Collect events with timeout
		receivedPaths := make(map[string]bool)
		timeout := time.After(30 * time.Second)

	eventLoop:
		for {
			select {
			case event := <-eventsChan:
				if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
					path := filepath.Join(event.Directory, event.EventNotification.NewEntry.Name)
					t.Logf("Received event for: %s", path)
					receivedPaths[path] = true
				}
				// Check if we received all expected events
				allReceived := true
				for _, p := range testFiles {
					if !receivedPaths[p] {
						allReceived = false
						break
					}
				}
				if allReceived {
					break eventLoop
				}
			case err := <-errChan:
				t.Fatalf("Subscription error: %v", err)
			case <-timeout:
				t.Logf("Timeout waiting for events. Received %d/%d events", len(receivedPaths), len(testFiles))
				break eventLoop
			}
		}

		// Verify we received events for all test files
		for _, path := range testFiles {
			assert.True(t, receivedPaths[path], "Should have received event for %s", path)
		}
	})
}

// TestMetadataSubscribeSingleFilerNoStall tests that subscription doesn't stall
// in single-filer setups (regression test for issue #4977)
func TestMetadataSubscribeSingleFilerNoStall(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_single_filer_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8888", 30*time.Second))

	t.Logf("Single-filer cluster started")

	t.Run("high_load_subscription_no_stall", func(t *testing.T) {
		// This test simulates the scenario from issue #4977:
		// High-load writes while a subscriber tries to keep up

		var receivedCount int64
		var uploadedCount int64

		eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 1000)
		errChan := make(chan error, 1)

		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()

		// Start subscriber
		go func() {
			err := subscribeToMetadata(subCtx, "127.0.0.1:8888", "/", eventsChan)
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				errChan <- err
			}
		}()

		// Wait for subscription to be established
		time.Sleep(2 * time.Second)

		// Start counting received events
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case event := <-eventsChan:
					if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
						if !event.EventNotification.NewEntry.IsDirectory {
							atomic.AddInt64(&receivedCount, 1)
						}
					}
				case <-subCtx.Done():
					return
				}
			}
		}()

		// Upload files concurrently (simulate high load)
		numFiles := 100
		numWorkers := 10

		uploadWg := sync.WaitGroup{}
		for w := 0; w < numWorkers; w++ {
			uploadWg.Add(1)
			go func(workerId int) {
				defer uploadWg.Done()
				for i := 0; i < numFiles/numWorkers; i++ {
					path := fmt.Sprintf("/load_test/worker%d/file%d.txt", workerId, i)
					err := uploadFile("http://127.0.0.1:8888"+path, []byte(fmt.Sprintf("content %d-%d", workerId, i)))
					if err == nil {
						atomic.AddInt64(&uploadedCount, 1)
					}
				}
			}(w)
		}

		uploadWg.Wait()
		uploaded := atomic.LoadInt64(&uploadedCount)
		t.Logf("Uploaded %d files", uploaded)

		// Wait for events to be received (with timeout to detect stall)
		stallTimeout := time.After(60 * time.Second)
		checkInterval := time.NewTicker(2 * time.Second)
		defer checkInterval.Stop()

		lastReceived := atomic.LoadInt64(&receivedCount)
		staleCount := 0

	waitLoop:
		for {
			select {
			case <-stallTimeout:
				received := atomic.LoadInt64(&receivedCount)
				t.Logf("Timeout: received %d/%d events (%.1f%%)",
					received, uploaded, float64(received)/float64(uploaded)*100)
				break waitLoop
			case <-checkInterval.C:
				received := atomic.LoadInt64(&receivedCount)
				if received >= uploaded {
					t.Logf("All %d events received", received)
					break waitLoop
				}
				if received == lastReceived {
					staleCount++
					if staleCount >= 5 {
						// If no progress for 10 seconds, subscription may be stalled
						t.Logf("WARNING: No progress for %d checks. Received %d/%d (%.1f%%)",
							staleCount, received, uploaded, float64(received)/float64(uploaded)*100)
					}
				} else {
					staleCount = 0
					t.Logf("Progress: received %d/%d events (%.1f%%)",
						received, uploaded, float64(received)/float64(uploaded)*100)
				}
				lastReceived = received
			case err := <-errChan:
				t.Fatalf("Subscription error: %v", err)
			}
		}

		subCancel()
		wg.Wait()

		received := atomic.LoadInt64(&receivedCount)

		// With the fix for #4977, we should receive a high percentage of events
		// Before the fix, this would stall at ~20-40%
		percentage := float64(received) / float64(uploaded) * 100
		t.Logf("Final: received %d/%d events (%.1f%%)", received, uploaded, percentage)

		// We should receive at least 80% of events (allowing for some timing issues)
		assert.GreaterOrEqual(t, percentage, 80.0,
			"Should receive at least 80%% of events (received %.1f%%)", percentage)
	})
}

// TestMetadataSubscribeResumeFromDisk tests that subscription can resume from disk
func TestMetadataSubscribeResumeFromDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_resume_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForHTTPServer("http://127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8888", 30*time.Second))

	t.Run("upload_before_subscribe", func(t *testing.T) {
		// Upload files BEFORE starting subscription
		numFiles := 20
		for i := 0; i < numFiles; i++ {
			path := fmt.Sprintf("/pre_subscribe/file%d.txt", i)
			err := uploadFile("http://127.0.0.1:8888"+path, []byte(fmt.Sprintf("content %d", i)))
			require.NoError(t, err)
		}
		t.Logf("Uploaded %d files before subscription", numFiles)

		// Wait for logs to be flushed to disk
		time.Sleep(15 * time.Second)

		// Now start subscription from the beginning
		eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 100)
		errChan := make(chan error, 1)

		subCtx, subCancel := context.WithTimeout(ctx, 30*time.Second)
		defer subCancel()

		go func() {
			err := subscribeToMetadataFromBeginning(subCtx, "127.0.0.1:8888", "/pre_subscribe/", eventsChan)
			if err != nil && !strings.Contains(err.Error(), "context") {
				errChan <- err
			}
		}()

		// Count received events
		receivedCount := 0
		timeout := time.After(30 * time.Second)

	countLoop:
		for {
			select {
			case event := <-eventsChan:
				if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
					if !event.EventNotification.NewEntry.IsDirectory {
						receivedCount++
						t.Logf("Received event %d: %s/%s", receivedCount,
							event.Directory, event.EventNotification.NewEntry.Name)
					}
				}
				if receivedCount >= numFiles {
					break countLoop
				}
			case err := <-errChan:
				t.Fatalf("Subscription error: %v", err)
			case <-timeout:
				t.Logf("Timeout: received %d/%d events", receivedCount, numFiles)
				break countLoop
			}
		}

		// Should receive all pre-uploaded files from disk
		assert.GreaterOrEqual(t, receivedCount, numFiles-2, // Allow small margin
			"Should receive most pre-uploaded files from disk (received %d/%d)", receivedCount, numFiles)
	})
}

// TestMetadataSubscribeConcurrentWrites tests subscription with concurrent writes
func TestMetadataSubscribeConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_concurrent_writes_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForHTTPServer("http://127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8888", 30*time.Second))

	t.Logf("Cluster started for concurrent writes test")

	t.Run("concurrent_goroutine_writes", func(t *testing.T) {
		var receivedCount int64
		var uploadedCount int64

		eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 10000)
		errChan := make(chan error, 1)

		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()

		// Start subscriber
		go func() {
			err := subscribeToMetadata(subCtx, "127.0.0.1:8888", "/concurrent/", eventsChan)
			if err != nil && !strings.Contains(err.Error(), "context") {
				errChan <- err
			}
		}()

		time.Sleep(2 * time.Second)

		// Start counting received events
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case event := <-eventsChan:
					if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
						if !event.EventNotification.NewEntry.IsDirectory {
							atomic.AddInt64(&receivedCount, 1)
						}
					}
				case <-subCtx.Done():
					return
				}
			}
		}()

		// Launch many concurrent writers
		numWorkers := 50
		filesPerWorker := 20
		totalExpected := int64(numWorkers * filesPerWorker)

		uploadWg := sync.WaitGroup{}
		for w := 0; w < numWorkers; w++ {
			uploadWg.Add(1)
			go func(workerId int) {
				defer uploadWg.Done()
				for i := 0; i < filesPerWorker; i++ {
					path := fmt.Sprintf("/concurrent/w%d/f%d.txt", workerId, i)
					content := []byte(fmt.Sprintf("worker%d-file%d", workerId, i))
					if err := uploadFile("http://127.0.0.1:8888"+path, content); err == nil {
						atomic.AddInt64(&uploadedCount, 1)
					}
				}
			}(w)
		}

		uploadWg.Wait()
		uploaded := atomic.LoadInt64(&uploadedCount)
		t.Logf("Uploaded %d/%d files from %d concurrent workers", uploaded, totalExpected, numWorkers)

		// Wait for events with progress tracking
		stallTimeout := time.After(90 * time.Second)
		checkInterval := time.NewTicker(3 * time.Second)
		defer checkInterval.Stop()

		lastReceived := int64(0)
		stableCount := 0

	waitLoop:
		for {
			select {
			case <-stallTimeout:
				break waitLoop
			case <-checkInterval.C:
				received := atomic.LoadInt64(&receivedCount)
				if received >= uploaded {
					t.Logf("All %d events received", received)
					break waitLoop
				}
				if received == lastReceived {
					stableCount++
					if stableCount >= 5 {
						t.Logf("No progress for %d checks, received %d/%d", stableCount, received, uploaded)
						break waitLoop
					}
				} else {
					stableCount = 0
					t.Logf("Progress: %d/%d (%.1f%%)", received, uploaded, float64(received)/float64(uploaded)*100)
				}
				lastReceived = received
			case err := <-errChan:
				t.Fatalf("Subscription error: %v", err)
			}
		}

		subCancel()
		wg.Wait()

		received := atomic.LoadInt64(&receivedCount)
		percentage := float64(received) / float64(uploaded) * 100
		t.Logf("Final: received %d/%d events (%.1f%%)", received, uploaded, percentage)

		// Should receive at least 80% of events
		assert.GreaterOrEqual(t, percentage, 80.0,
			"Should receive at least 80%% of concurrent write events")
	})
}

// TestMetadataSubscribeMillionUpdates tests subscription with 1 million metadata updates
// This test creates metadata entries directly via gRPC without actual file content
func TestMetadataSubscribeMillionUpdates(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_million_updates_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	require.NoError(t, waitForHTTPServer("http://127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForHTTPServer("http://127.0.0.1:8888", 30*time.Second))

	t.Logf("Cluster started for million updates test")

	t.Run("million_metadata_updates", func(t *testing.T) {
		var receivedCount int64
		var createdCount int64
		totalEntries := int64(1_000_000)

		eventsChan := make(chan *filer_pb.SubscribeMetadataResponse, 100000)
		errChan := make(chan error, 1)

		subCtx, subCancel := context.WithCancel(ctx)
		defer subCancel()

		// Start subscriber
		go func() {
			err := subscribeToMetadata(subCtx, "127.0.0.1:8888", "/million/", eventsChan)
			if err != nil && !strings.Contains(err.Error(), "context") {
				errChan <- err
			}
		}()

		time.Sleep(2 * time.Second)

		// Start counting received events
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case event := <-eventsChan:
					if event.EventNotification != nil && event.EventNotification.NewEntry != nil {
						if !event.EventNotification.NewEntry.IsDirectory {
							atomic.AddInt64(&receivedCount, 1)
						}
					}
				case <-subCtx.Done():
					return
				}
			}
		}()

		// Create metadata entries directly via gRPC (no actual file content)
		numWorkers := 100
		entriesPerWorker := int(totalEntries) / numWorkers

		startTime := time.Now()
		createWg := sync.WaitGroup{}

		for w := 0; w < numWorkers; w++ {
			createWg.Add(1)
			go func(workerId int) {
				defer createWg.Done()
				grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

				err := pb.WithFilerClient(false, 0, pb.ServerAddress("127.0.0.1:8888"), grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
					for i := 0; i < entriesPerWorker; i++ {
						dir := fmt.Sprintf("/million/bucket%d", workerId%100)
						name := fmt.Sprintf("entry_%d_%d", workerId, i)

						_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
							Directory: dir,
							Entry: &filer_pb.Entry{
								Name:        name,
								IsDirectory: false,
								Attributes: &filer_pb.FuseAttributes{
									FileSize: 100,
									Mtime:    time.Now().Unix(),
									FileMode: 0644,
									Uid:      1000,
									Gid:      1000,
								},
							},
						})
						if err == nil {
							atomic.AddInt64(&createdCount, 1)
						}

						// Log progress every 10000 entries per worker
						if i > 0 && i%10000 == 0 {
							created := atomic.LoadInt64(&createdCount)
							elapsed := time.Since(startTime)
							rate := float64(created) / elapsed.Seconds()
							t.Logf("Worker %d: created %d entries, total %d (%.0f/sec)",
								workerId, i, created, rate)
						}
					}
					return nil
				})
				if err != nil {
					t.Logf("Worker %d error: %v", workerId, err)
				}
			}(w)
		}

		// Progress reporter
		progressDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					created := atomic.LoadInt64(&createdCount)
					received := atomic.LoadInt64(&receivedCount)
					elapsed := time.Since(startTime)
					createRate := float64(created) / elapsed.Seconds()
					receiveRate := float64(received) / elapsed.Seconds()
					t.Logf("Progress: created %d (%.0f/sec), received %d (%.0f/sec), lag %d",
						created, createRate, received, receiveRate, created-received)
				case <-progressDone:
					return
				}
			}
		}()

		createWg.Wait()
		close(progressDone)

		created := atomic.LoadInt64(&createdCount)
		elapsed := time.Since(startTime)
		t.Logf("Created %d entries in %v (%.0f/sec)", created, elapsed, float64(created)/elapsed.Seconds())

		// Wait for subscription to catch up
		catchupTimeout := time.After(5 * time.Minute)
		checkInterval := time.NewTicker(5 * time.Second)
		defer checkInterval.Stop()

		lastReceived := int64(0)
		stableCount := 0

	waitLoop:
		for {
			select {
			case <-catchupTimeout:
				t.Logf("Catchup timeout reached")
				break waitLoop
			case <-checkInterval.C:
				received := atomic.LoadInt64(&receivedCount)
				if received >= created {
					t.Logf("All %d events received", received)
					break waitLoop
				}
				if received == lastReceived {
					stableCount++
					if stableCount >= 10 {
						t.Logf("No progress for %d checks", stableCount)
						break waitLoop
					}
				} else {
					stableCount = 0
					rate := float64(received-lastReceived) / 5.0
					t.Logf("Catching up: %d/%d (%.1f%%) at %.0f/sec",
						received, created, float64(received)/float64(created)*100, rate)
				}
				lastReceived = received
			case err := <-errChan:
				t.Fatalf("Subscription error: %v", err)
			}
		}

		subCancel()
		wg.Wait()

		received := atomic.LoadInt64(&receivedCount)
		percentage := float64(received) / float64(created) * 100
		totalTime := time.Since(startTime)
		t.Logf("Final: created %d, received %d (%.1f%%) in %v", created, received, percentage, totalTime)

		// For million entries, we expect at least 90% to be received
		assert.GreaterOrEqual(t, percentage, 90.0,
			"Should receive at least 90%% of million metadata events (received %.1f%%)", percentage)
	})
}

// Helper types and functions

type TestCluster struct {
	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd
	filerCmd  *exec.Cmd
	testDir   string
}

func (c *TestCluster) Stop() {
	if c.filerCmd != nil && c.filerCmd.Process != nil {
		c.filerCmd.Process.Kill()
		c.filerCmd.Wait()
	}
	if c.volumeCmd != nil && c.volumeCmd.Process != nil {
		c.volumeCmd.Process.Kill()
		c.volumeCmd.Wait()
	}
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}
}

func startSeaweedFSCluster(ctx context.Context, dataDir string) (*TestCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &TestCluster{testDir: dataDir}

	// Create directories
	masterDir := filepath.Join(dataDir, "master")
	volumeDir := filepath.Join(dataDir, "volume")
	filerDir := filepath.Join(dataDir, "filer")
	if err := os.MkdirAll(masterDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create master dir: %v", err)
	}
	if err := os.MkdirAll(volumeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create volume dir: %v", err)
	}
	if err := os.MkdirAll(filerDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create filer dir: %v", err)
	}

	// Start master server
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9333",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
		"-peers", "none",
	)
	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile
	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master: %v", err)
	}
	cluster.masterCmd = masterCmd

	time.Sleep(2 * time.Second)

	// Start volume server
	volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
		"-port", "8080",
		"-dir", volumeDir,
		"-max", "10",
		"-master", "127.0.0.1:9333",
		"-ip", "127.0.0.1",
	)
	volumeLogFile, err := os.Create(filepath.Join(volumeDir, "volume.log"))
	if err != nil {
		cluster.Stop()
		return nil, fmt.Errorf("failed to create volume log file: %v", err)
	}
	volumeCmd.Stdout = volumeLogFile
	volumeCmd.Stderr = volumeLogFile
	if err := volumeCmd.Start(); err != nil {
		cluster.Stop()
		return nil, fmt.Errorf("failed to start volume: %v", err)
	}
	cluster.volumeCmd = volumeCmd

	time.Sleep(2 * time.Second)

	// Start filer server
	filerCmd := exec.CommandContext(ctx, weedBinary, "filer",
		"-port", "8888",
		"-master", "127.0.0.1:9333",
		"-ip", "127.0.0.1",
	)
	filerLogFile, err := os.Create(filepath.Join(filerDir, "filer.log"))
	if err != nil {
		cluster.Stop()
		return nil, fmt.Errorf("failed to create filer log file: %v", err)
	}
	filerCmd.Stdout = filerLogFile
	filerCmd.Stderr = filerLogFile
	if err := filerCmd.Start(); err != nil {
		cluster.Stop()
		return nil, fmt.Errorf("failed to start filer: %v", err)
	}
	cluster.filerCmd = filerCmd

	time.Sleep(3 * time.Second)

	return cluster, nil
}

func findWeedBinary() string {
	candidates := []string{
		"../../../weed/weed",
		"../../weed/weed",
		"./weed",
		"weed",
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}
	return ""
}

func waitForHTTPServer(url string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for server %s", url)
}

func uploadFile(url string, content []byte) error {
	// Create multipart form data
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Extract filename from URL path
	parts := strings.Split(url, "/")
	filename := parts[len(parts)-1]
	if filename == "" {
		filename = "file.txt"
	}

	// Create form file field
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return fmt.Errorf("create form file: %v", err)
	}
	if _, err := part.Write(content); err != nil {
		return fmt.Errorf("write content: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %v", err)
	}

	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func subscribeToMetadata(ctx context.Context, filerGrpcAddress, pathPrefix string, eventsChan chan<- *filer_pb.SubscribeMetadataResponse) error {
	return subscribeToMetadataWithOptions(ctx, filerGrpcAddress, pathPrefix, time.Now().UnixNano(), eventsChan)
}

func subscribeToMetadataFromBeginning(ctx context.Context, filerGrpcAddress, pathPrefix string, eventsChan chan<- *filer_pb.SubscribeMetadataResponse) error {
	// Start from Unix epoch to get all events
	return subscribeToMetadataWithOptions(ctx, filerGrpcAddress, pathPrefix, 0, eventsChan)
}

func subscribeToMetadataWithOptions(ctx context.Context, filerGrpcAddress, pathPrefix string, sinceNs int64, eventsChan chan<- *filer_pb.SubscribeMetadataResponse) error {
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	if grpcDialOption == nil {
		grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	return pb.WithFilerClient(false, 0, pb.ServerAddress(filerGrpcAddress), grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.SubscribeMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
			ClientName: "integration_test",
			PathPrefix: pathPrefix,
			SinceNs:    sinceNs,
			ClientId:   util.RandomInt32(),
		})
		if err != nil {
			return fmt.Errorf("subscribe: %v", err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF || ctx.Err() != nil {
					return nil
				}
				return err
			}

		select {
		case eventsChan <- resp:
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
			// Channel full after brief wait, log warning
			glog.Warningf("Event channel full, skipping event for %s", resp.Directory)
		}
		}
	})
}
