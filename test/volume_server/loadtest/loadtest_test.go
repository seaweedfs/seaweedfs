package loadtest

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
)

// Run with:
//   go test -v -count=1 -timeout 300s -run BenchmarkVolumeServer ./test/volume_server/loadtest/...
//   VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 300s -run BenchmarkVolumeServer ./test/volume_server/loadtest/...
//
// Compare results:
//   go test -count=1 -timeout 300s -run BenchmarkVolumeServer -bench . ./test/volume_server/loadtest/... | tee go.txt
//   VOLUME_SERVER_IMPL=rust go test -count=1 -timeout 300s -run BenchmarkVolumeServer -bench . ./test/volume_server/loadtest/... | tee rust.txt

// Step-by-step payload sizes: 1KB → 4KB → 16KB → 64KB → 256KB → 1MB → 4MB → 8MB
var payloadSteps = []struct {
	name string
	size int
}{
	{"1KB", 1 << 10},
	{"4KB", 4 << 10},
	{"16KB", 16 << 10},
	{"64KB", 64 << 10},
	{"256KB", 256 << 10},
	{"1MB", 1 << 20},
	{"4MB", 4 << 20},
	{"8MB", 8 << 20},
}

func implName() string {
	if os.Getenv("VOLUME_SERVER_IMPL") == "rust" {
		return "rust"
	}
	return "go"
}

// setupCluster starts a volume cluster and returns the admin URL and cleanup.
func setupCluster(tb testing.TB) (adminURL string, grpcAddr string, cleanup func()) {
	tb.Helper()
	cluster := framework.StartVolumeCluster(tb, matrix.P1())
	return cluster.VolumeAdminURL(), cluster.VolumeGRPCAddress(), cluster.Stop
}

// allocateVolume allocates a volume via gRPC and returns its ID.
func allocateVolume(tb testing.TB, grpcAddr string, volumeID uint32) {
	tb.Helper()
	conn, client := framework.DialVolumeServer(tb, grpcAddr)
	defer conn.Close()
	framework.AllocateVolume(tb, client, volumeID, "loadtest")
}

func makePayload(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// uploadFile uploads data and returns the file ID used.
func uploadFile(client *http.Client, adminURL string, volumeID uint32, key uint64, cookie uint32, data []byte) error {
	fid := framework.NewFileID(volumeID, key, cookie)
	url := fmt.Sprintf("%s/%s", adminURL, fid)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("upload %s: status %d", fid, resp.StatusCode)
	}
	return nil
}

// downloadFile reads a file and discards the body.
func downloadFile(client *http.Client, adminURL string, volumeID uint32, key uint64, cookie uint32) error {
	fid := framework.NewFileID(volumeID, key, cookie)
	url := fmt.Sprintf("%s/%s", adminURL, fid)
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("download %s: status %d", fid, resp.StatusCode)
	}
	return nil
}

// deleteFile deletes a file.
func deleteFile(client *http.Client, adminURL string, volumeID uint32, key uint64, cookie uint32) error {
	fid := framework.NewFileID(volumeID, key, cookie)
	url := fmt.Sprintf("%s/%s", adminURL, fid)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

// --- Throughput load tests (not Go benchmarks, manual timing for comparison) ---

// TestBenchmarkVolumeServer runs a suite of load tests printing ops/sec and latency.
func TestBenchmarkVolumeServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	impl := implName()
	adminURL, grpcAddr, cleanup := setupCluster(t)
	defer cleanup()

	const volumeID = uint32(10)
	allocateVolume(t, grpcAddr, volumeID)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 128,
			MaxConnsPerHost:     128,
		},
	}

	// opsForSize returns fewer ops for larger payloads to keep test time reasonable.
	opsForSize := func(size, concurrency int) int {
		switch {
		case size >= 4<<20:
			if concurrency > 1 {
				return 64
			}
			return 30
		case size >= 1<<20:
			if concurrency > 1 {
				return 200
			}
			return 100
		case size >= 64<<10:
			if concurrency > 1 {
				return 500
			}
			return 300
		default:
			if concurrency > 1 {
				return 1000
			}
			return 500
		}
	}

	// Step-by-step upload: 1KB → 4KB → 16KB → 64KB → 256KB → 1MB → 4MB → 8MB
	for _, ps := range payloadSteps {
		for _, mode := range []struct {
			label       string
			concurrency int
		}{
			{"seq", 1},
			{"c16", 16},
		} {
			name := fmt.Sprintf("Upload/%s/%s", ps.name, mode.label)
			numOps := opsForSize(ps.size, mode.concurrency)
			t.Run(fmt.Sprintf("%s/%s", impl, name), func(t *testing.T) {
				payload := makePayload(ps.size)
				runThroughputTest(t, impl, name, httpClient, adminURL, volumeID,
					payload, numOps, mode.concurrency, false, false)
			})
		}
	}

	// Step-by-step download: 1KB → 4KB → 16KB → 64KB → 256KB → 1MB → 4MB → 8MB
	for _, ps := range payloadSteps {
		for _, mode := range []struct {
			label       string
			concurrency int
		}{
			{"seq", 1},
			{"c16", 16},
		} {
			name := fmt.Sprintf("Download/%s/%s", ps.name, mode.label)
			numOps := opsForSize(ps.size, mode.concurrency)
			t.Run(fmt.Sprintf("%s/%s", impl, name), func(t *testing.T) {
				payload := makePayload(ps.size)
				runThroughputTest(t, impl, name, httpClient, adminURL, volumeID,
					payload, numOps, mode.concurrency, true, false)
			})
		}
	}

	// Mixed read/write at each size
	for _, ps := range payloadSteps {
		name := fmt.Sprintf("Mixed/%s/c16", ps.name)
		numOps := opsForSize(ps.size, 16)
		t.Run(fmt.Sprintf("%s/%s", impl, name), func(t *testing.T) {
			payload := makePayload(ps.size)
			runThroughputTest(t, impl, name, httpClient, adminURL, volumeID,
				payload, numOps, 16, false, true)
		})
	}

	// Delete test
	t.Run(fmt.Sprintf("%s/Delete/1KB/c16", impl), func(t *testing.T) {
		payload := makePayload(1 << 10)
		numOps := 1000
		baseKey := uint64(900000)

		for i := 0; i < numOps; i++ {
			if err := uploadFile(httpClient, adminURL, volumeID, baseKey+uint64(i), 1, payload); err != nil {
				t.Fatalf("pre-upload for delete %d: %v", i, err)
			}
		}

		var ops atomic.Int64
		var totalLatency atomic.Int64

		start := time.Now()
		var wg sync.WaitGroup
		concurrency := 16
		opsPerWorker := numOps / concurrency

		for w := 0; w < concurrency; w++ {
			workerBase := baseKey + uint64(w*opsPerWorker)
			wg.Add(1)
			go func(wb uint64) {
				defer wg.Done()
				for i := 0; i < opsPerWorker; i++ {
					opStart := time.Now()
					deleteFile(httpClient, adminURL, volumeID, wb+uint64(i), 1)
					totalLatency.Add(time.Since(opStart).Nanoseconds())
					ops.Add(1)
				}
			}(workerBase)
		}
		wg.Wait()
		elapsed := time.Since(start)

		totalOps := ops.Load()
		avgLatencyUs := float64(totalLatency.Load()) / float64(totalOps) / 1000.0
		opsPerSec := float64(totalOps) / elapsed.Seconds()

		t.Logf("RESULT impl=%-4s test=%-22s ops=%-6d errors=%-4d elapsed=%-10s ops/s=%-10.1f avg_lat=%-10.0fus",
			impl, "Delete/1KB/c16", totalOps, 0, elapsed.Round(time.Millisecond), opsPerSec, avgLatencyUs)
	})
}

// runThroughputTest is the shared core for throughput tests.
// keyOffset separates key ranges so concurrent tests in the same volume don't collide.
// keyCounter provides globally unique key ranges. Starts at 1 because key=0 is invalid.
var keyCounter atomic.Uint64

func init() {
	keyCounter.Store(1)
}

func runThroughputTest(
	t *testing.T, impl, name string,
	httpClient *http.Client, adminURL string, volumeID uint32,
	payload []byte, numOps, concurrency int,
	isDownload, isMixed bool,
) {
	t.Helper()

	// Each call gets a unique key range
	baseKey := keyCounter.Add(uint64(numOps*2)) - uint64(numOps*2)

	// Pre-upload for download / mixed
	if isDownload || isMixed {
		for i := 0; i < numOps; i++ {
			if err := uploadFile(httpClient, adminURL, volumeID, baseKey+uint64(i), 1, payload); err != nil {
				t.Fatalf("pre-upload %d: %v", i, err)
			}
		}
	}

	uploadBase := baseKey
	if !isDownload && !isMixed {
		uploadBase = baseKey + uint64(numOps) // fresh range for uploads
	}

	var ops atomic.Int64
	var errors atomic.Int64
	var totalLatency atomic.Int64

	start := time.Now()

	var wg sync.WaitGroup
	opsPerWorker := numOps / concurrency
	remainder := numOps % concurrency

	for w := 0; w < concurrency; w++ {
		n := opsPerWorker
		if w < remainder {
			n++
		}
		var workerBase uint64
		if w < remainder {
			workerBase = uploadBase + uint64(w*(opsPerWorker+1))
		} else {
			workerBase = uploadBase + uint64(remainder*(opsPerWorker+1)) + uint64((w-remainder)*opsPerWorker)
		}

		wg.Add(1)
		go func(wb uint64, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				key := wb + uint64(i)
				opStart := time.Now()
				var err error

				if isMixed {
					if i%2 == 0 {
						err = uploadFile(httpClient, adminURL, volumeID, key, 1, payload)
					} else {
						err = downloadFile(httpClient, adminURL, volumeID, key, 1)
					}
				} else if isDownload {
					err = downloadFile(httpClient, adminURL, volumeID, key, 1)
				} else {
					err = uploadFile(httpClient, adminURL, volumeID, key, 1, payload)
				}

				totalLatency.Add(time.Since(opStart).Nanoseconds())
				ops.Add(1)
				if err != nil {
					errors.Add(1)
				}
			}
		}(workerBase, n)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := ops.Load()
	totalErrs := errors.Load()
	avgLatencyUs := float64(totalLatency.Load()) / float64(totalOps) / 1000.0
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	throughputMBs := opsPerSec * float64(len(payload)) / (1024 * 1024)

	t.Logf("RESULT impl=%-4s test=%-22s ops=%-6d errors=%-4d elapsed=%-10s ops/s=%-10.1f avg_lat=%-10.0fus throughput=%.2f MB/s",
		impl, name, totalOps, totalErrs, elapsed.Round(time.Millisecond), opsPerSec, avgLatencyUs, throughputMBs)
}

// TestLatencyPercentiles measures p50/p95/p99 latencies for upload and download at each size.
func TestLatencyPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	impl := implName()
	adminURL, grpcAddr, cleanup := setupCluster(t)
	defer cleanup()

	const volumeID = uint32(20)
	allocateVolume(t, grpcAddr, volumeID)

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 64,
			MaxConnsPerHost:     64,
		},
	}

	latOpsForSize := func(size int) int {
		switch {
		case size >= 4<<20:
			return 30
		case size >= 1<<20:
			return 100
		default:
			return 300
		}
	}

	for _, ps := range payloadSteps {
		for _, dl := range []struct {
			prefix     string
			isDownload bool
		}{
			{"Upload", false},
			{"Download", true},
		} {
			name := fmt.Sprintf("%s/%s", dl.prefix, ps.name)
			numOps := latOpsForSize(ps.size)

			t.Run(fmt.Sprintf("%s/%s", impl, name), func(t *testing.T) {
				payload := makePayload(ps.size)
				baseKey := keyCounter.Add(uint64(numOps * 2))

				if dl.isDownload {
					for i := 0; i < numOps; i++ {
						if err := uploadFile(httpClient, adminURL, volumeID, baseKey+uint64(i), 2, payload); err != nil {
							t.Fatalf("pre-upload: %v", err)
						}
					}
				}

				uploadBase := baseKey
				if !dl.isDownload {
					uploadBase = baseKey + uint64(numOps)
				}

				latencies := make([]time.Duration, numOps)
				for i := 0; i < numOps; i++ {
					key := uploadBase + uint64(i)
					start := time.Now()
					if dl.isDownload {
						downloadFile(httpClient, adminURL, volumeID, key, 2)
					} else {
						uploadFile(httpClient, adminURL, volumeID, key, 2, payload)
					}
					latencies[i] = time.Since(start)
				}

				sortDurations(latencies)

				p50 := latencies[len(latencies)*50/100]
				p95 := latencies[len(latencies)*95/100]
				p99 := latencies[len(latencies)*99/100]
				min := latencies[0]
				max := latencies[len(latencies)-1]

				t.Logf("RESULT impl=%-4s test=%-20s n=%-4d min=%-10s p50=%-10s p95=%-10s p99=%-10s max=%-10s",
					impl, name, numOps, min.Round(time.Microsecond), p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), max.Round(time.Microsecond))
			})
		}
	}
}

func sortDurations(d []time.Duration) {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
}

// TestSustainedP99 runs high-concurrency load for a sustained period (default 60s,
// override with LOADTEST_DURATION=120s) and reports p50/p95/p99/p999 latencies.
// This reveals tail latency differences that short tests miss (GC pauses, lock contention, etc).
//
// Run:
//   go test -v -count=1 -timeout 600s -run TestSustainedP99 ./test/volume_server/loadtest/...
//   VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 600s -run TestSustainedP99 ./test/volume_server/loadtest/...
//   LOADTEST_DURATION=120s VOLUME_SERVER_IMPL=rust go test -v -count=1 -timeout 600s -run TestSustainedP99 ./test/volume_server/loadtest/...
func TestSustainedP99(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained load test in short mode")
	}

	duration := 60 * time.Second
	if d := os.Getenv("LOADTEST_DURATION"); d != "" {
		parsed, err := time.ParseDuration(d)
		if err == nil && parsed > 0 {
			duration = parsed
		}
	}

	impl := implName()
	adminURL, grpcAddr, cleanup := setupCluster(t)
	defer cleanup()

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 128,
			MaxConnsPerHost:     128,
		},
	}

	type scenario struct {
		name        string
		size        int
		concurrency int
		isDownload  bool
	}

	scenarios := []scenario{
		{"Upload/1KB/c16", 1 << 10, 16, false},
		{"Upload/64KB/c16", 64 << 10, 16, false},
		{"Download/1KB/c16", 1 << 10, 16, true},
		{"Download/64KB/c16", 64 << 10, 16, true},
	}

	var nextVolID atomic.Uint32
	nextVolID.Store(30)

	for _, sc := range scenarios {
		t.Run(fmt.Sprintf("%s/%s", impl, sc.name), func(t *testing.T) {
			// Each scenario gets its own volume to avoid filling up
			volumeID := nextVolID.Add(1) - 1
			allocateVolume(t, grpcAddr, volumeID)

			payload := makePayload(sc.size)

			// Pre-upload a pool of files for download tests
			poolSize := 500
			baseKey := keyCounter.Add(uint64(poolSize*2)) - uint64(poolSize*2)

			if sc.isDownload {
				t.Logf("Pre-uploading %d files for download test...", poolSize)
				for i := 0; i < poolSize; i++ {
					if err := uploadFile(httpClient, adminURL, volumeID, baseKey+uint64(i), 3, payload); err != nil {
						t.Fatalf("pre-upload %d: %v", i, err)
					}
				}
			}

			// Collect latencies from all workers
			type latencyBucket struct {
				mu        sync.Mutex
				latencies []time.Duration
			}
			bucket := &latencyBucket{
				latencies: make([]time.Duration, 0, 100000),
			}

			var totalOps atomic.Int64
			var totalErrors atomic.Int64

			deadline := time.Now().Add(duration)
			start := time.Now()

			// For uploads, pre-seed the pool so subsequent writes are overwrites (no volume fill)
			if !sc.isDownload {
				t.Logf("Pre-seeding %d files for upload overwrite test...", poolSize)
				for i := 0; i < poolSize; i++ {
					if err := uploadFile(httpClient, adminURL, volumeID, baseKey+uint64(i), 3, payload); err != nil {
						t.Fatalf("pre-seed %d: %v", i, err)
					}
				}
			}

			var wg sync.WaitGroup
			for w := 0; w < sc.concurrency; w++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					localLats := make([]time.Duration, 0, 8192)

					var i uint64
					for time.Now().Before(deadline) {
						// Cycle through the pool to avoid filling up the volume
						key := baseKey + uint64(int(i)%poolSize)

						opStart := time.Now()
						var err error
						if sc.isDownload {
							err = downloadFile(httpClient, adminURL, volumeID, key, 3)
						} else {
							err = uploadFile(httpClient, adminURL, volumeID, key, 3, payload)
						}
						lat := time.Since(opStart)

						localLats = append(localLats, lat)
						totalOps.Add(1)
						if err != nil {
							totalErrors.Add(1)
						}
						i++

						// Flush local buffer periodically
						if len(localLats) >= 8192 {
							bucket.mu.Lock()
							bucket.latencies = append(bucket.latencies, localLats...)
							bucket.mu.Unlock()
							localLats = localLats[:0]
						}
					}
					// Final flush
					if len(localLats) > 0 {
						bucket.mu.Lock()
						bucket.latencies = append(bucket.latencies, localLats...)
						bucket.mu.Unlock()
					}
				}(w)
			}

			wg.Wait()
			elapsed := time.Since(start)

			lats := bucket.latencies
			n := len(lats)
			ops := totalOps.Load()
			errs := totalErrors.Load()
			opsPerSec := float64(ops) / elapsed.Seconds()

			sortDurations(lats)

			pct := func(p float64) time.Duration {
				idx := int(float64(n) * p / 100.0)
				if idx >= n {
					idx = n - 1
				}
				return lats[idx]
			}

			t.Logf("RESULT impl=%-4s test=%-22s duration=%-6s ops=%-8d errors=%-4d ops/s=%-10.1f",
				impl, sc.name, elapsed.Round(time.Second), ops, errs, opsPerSec)
			t.Logf("       p50=%-10s p90=%-10s p95=%-10s p99=%-10s p999=%-10s max=%-10s",
				pct(50).Round(time.Microsecond),
				pct(90).Round(time.Microsecond),
				pct(95).Round(time.Microsecond),
				pct(99).Round(time.Microsecond),
				pct(99.9).Round(time.Microsecond),
				lats[n-1].Round(time.Microsecond))
		})
	}
}
