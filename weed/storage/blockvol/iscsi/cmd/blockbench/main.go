// blockbench is a simple block device benchmark tool.
// Usage: blockbench [-f path] [-size 256M] [-bs 4K] [-duration 10s] [-pattern seq|rand] [-rw read|write|mixed]
package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	filePath := flag.String("f", "", "test file path (e.g. F:\\testfile.dat)")
	fileSize := flag.String("size", "256M", "test file size")
	blockSize := flag.String("bs", "4K", "block size")
	duration := flag.Duration("duration", 10*time.Second, "test duration")
	pattern := flag.String("pattern", "seq", "access pattern: seq or rand")
	mode := flag.String("rw", "write", "read, write, or mixed")
	workers := flag.Int("workers", 1, "number of concurrent workers")
	direct := flag.Bool("direct", false, "use O_DIRECT (not supported on all OS)")
	flag.Parse()

	if *filePath == "" {
		fmt.Fprintln(os.Stderr, "error: -f is required")
		flag.Usage()
		os.Exit(1)
	}

	fSize, err := parseSize(*fileSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad -size: %v\n", err)
		os.Exit(1)
	}
	bSize, err := parseSize(*blockSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bad -bs: %v\n", err)
		os.Exit(1)
	}

	_ = *direct // placeholder for future O_DIRECT support

	fmt.Printf("=== BlockBench ===\n")
	fmt.Printf("file=%s size=%s bs=%s duration=%s pattern=%s mode=%s workers=%d\n",
		*filePath, *fileSize, *blockSize, *duration, *pattern, *mode, *workers)

	// Create/open file
	f, err := os.OpenFile(*filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// Pre-allocate
	if err := f.Truncate(int64(fSize)); err != nil {
		fmt.Fprintf(os.Stderr, "truncate: %v\n", err)
		os.Exit(1)
	}

	totalBlocks := fSize / bSize

	// Prepare write buffer
	writeBuf := make([]byte, bSize)
	rand.Read(writeBuf)

	var ops atomic.Int64
	var bytes atomic.Int64
	var maxLatUs atomic.Int64

	isRand := *pattern == "rand"
	doWrite := *mode == "write" || *mode == "mixed"
	doRead := *mode == "read" || *mode == "mixed"

	stop := make(chan struct{})

	fmt.Printf("running %s %s for %s...\n", *pattern, *mode, *duration)

	start := time.Now()

	for w := 0; w < *workers; w++ {
		wf, err := os.OpenFile(*filePath, os.O_RDWR, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "worker open: %v\n", err)
			os.Exit(1)
		}
		defer wf.Close()

		go func(wf *os.File, workerID int) {
			buf := make([]byte, bSize)
			copy(buf, writeBuf)
			var seqBlock uint64
			readBuf := make([]byte, bSize)

			for {
				select {
				case <-stop:
					return
				default:
				}

				var blockNum uint64
				if isRand {
					n, _ := rand.Int(rand.Reader, big.NewInt(int64(totalBlocks)))
					blockNum = n.Uint64()
				} else {
					blockNum = seqBlock % totalBlocks
					seqBlock++
				}

				off := int64(blockNum * bSize)
				opStart := time.Now()

				if doWrite && (!doRead || seqBlock%2 == 0) {
					_, err = wf.WriteAt(buf, off)
				} else if doRead {
					_, err = wf.ReadAt(readBuf, off)
				}

				us := time.Since(opStart).Microseconds()
				if err == nil {
					ops.Add(1)
					bytes.Add(int64(bSize))
					for {
						old := maxLatUs.Load()
						if us <= old || maxLatUs.CompareAndSwap(old, us) {
							break
						}
					}
				}
			}
		}(wf, w)
	}

	time.Sleep(*duration)
	close(stop)
	elapsed := time.Since(start)

	// Small sleep to let workers finish
	time.Sleep(10 * time.Millisecond)

	totalOps := ops.Load()
	totalBytes := bytes.Load()
	maxLat := maxLatUs.Load()

	iops := float64(totalOps) / elapsed.Seconds()
	mbps := float64(totalBytes) / (1024 * 1024) / elapsed.Seconds()
	avgUs := int64(0)
	if totalOps > 0 {
		avgUs = int64(elapsed.Microseconds()) / totalOps * int64(*workers)
		// Rough approximation — actual avg needs per-op tracking
		avgUs = int64(float64(elapsed.Microseconds()) / float64(totalOps) * float64(*workers))
	}

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("ops:       %d\n", totalOps)
	fmt.Printf("IOPS:      %.0f\n", iops)
	fmt.Printf("throughput: %.1f MB/s\n", mbps)
	fmt.Printf("max_lat:   %d us (%.1f ms)\n", maxLat, float64(maxLat)/1000)
	fmt.Printf("elapsed:   %.1fs\n", elapsed.Seconds())
	if totalOps > 0 {
		fmt.Printf("avg_lat:   ~%d us (%.2f ms) [estimated]\n", avgUs, float64(avgUs)/1000)
	}
}

func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty size")
	}
	mul := uint64(1)
	switch s[len(s)-1] {
	case 'K', 'k':
		mul = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		mul = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		mul = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}
	var n uint64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n * mul, err
}
