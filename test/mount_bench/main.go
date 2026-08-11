// Command mount_bench times common filesystem operations against a mounted
// directory and writes the results as JSON, so runs against different mounts
// (WinFsp, rclone, libfuse) can be compared with -compare.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	seqFileSize   = 256 << 20
	seqBufSize    = 1 << 20
	randReads     = 500
	randReadSize  = 4 << 10
	smallCount    = 400
	smallSize     = 64 << 10
	overwriteN    = 100
	statRounds    = 3
	listRounds    = 10
	bigListCount  = 5000
	bigListRounds = 3
)

type phaseResult struct {
	Name    string  `json:"name"`
	Seconds float64 `json:"seconds"`
	Bytes   int64   `json:"bytes,omitempty"`
	Ops     int64   `json:"ops,omitempty"`
}

type runResult struct {
	Label  string        `json:"label"`
	Phases []phaseResult `json:"phases"`
}

func main() {
	dir := flag.String("dir", "", "directory on the mount to benchmark in; created if missing, must be a direct child of the mount root")
	label := flag.String("label", "bench", "name for this result set")
	out := flag.String("out", "", "write results as JSON to this file")
	filer := flag.String("filer", "", "filer http address for out-of-band setup of the big-listing phase")
	seed := flag.String("seed", "", "seed the big-listing directory at this filer-relative path and exit; for mounts that cache listings and would miss files created behind them")
	compare := flag.String("compare", "", "comma-separated result JSON files; print a comparison table and exit")
	flag.Parse()

	if *compare != "" {
		if err := printComparison(strings.Split(*compare, ",")); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *seed != "" {
		if *filer == "" {
			log.Fatal("-seed requires -filer")
		}
		if err := seedFilerDir(*filer, *seed, bigListCount); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *dir == "" {
		log.Fatal("either -dir or -compare is required")
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}

	r := &runner{dir: *dir, filer: *filer, result: runResult{Label: *label}}
	r.run()

	data, err := json.MarshalIndent(r.result, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if *out != "" {
		if err := os.WriteFile(*out, data, 0644); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(string(data))
}

type runner struct {
	dir    string
	filer  string
	result runResult
}

func (r *runner) phase(name string, bytes, ops int64, fn func() error) {
	start := time.Now()
	if err := fn(); err != nil {
		log.Fatalf("%s: %v", name, err)
	}
	elapsed := time.Since(start)
	r.result.Phases = append(r.result.Phases, phaseResult{
		Name: name, Seconds: elapsed.Seconds(), Bytes: bytes, Ops: ops,
	})
	fmt.Printf("%-14s %10.3fs%s\n", name, elapsed.Seconds(), rateSuffix(bytes, ops, elapsed.Seconds()))
}

func rateSuffix(bytes, ops int64, seconds float64) string {
	if seconds <= 0 {
		return ""
	}
	var parts []string
	if bytes > 0 {
		parts = append(parts, fmt.Sprintf("%8.1f MB/s", float64(bytes)/seconds/1e6))
	}
	if ops > 0 {
		parts = append(parts, fmt.Sprintf("%8.1f ops/s", float64(ops)/seconds))
	}
	if len(parts) == 0 {
		return ""
	}
	return "  " + strings.Join(parts, "  ")
}

func (r *runner) run() {
	buf := make([]byte, seqBufSize)
	rnd := rand.New(rand.NewSource(42))
	rnd.Read(buf)

	bigFile := filepath.Join(r.dir, "big.dat")
	r.phase("seq_write", seqFileSize, 0, func() error {
		f, err := os.Create(bigFile)
		if err != nil {
			return err
		}
		for written := 0; written < seqFileSize; written += len(buf) {
			if _, err := f.Write(buf); err != nil {
				f.Close()
				return err
			}
		}
		return f.Close()
	})

	r.phase("seq_read", seqFileSize, 0, func() error {
		f, err := os.Open(bigFile)
		if err != nil {
			return err
		}
		defer f.Close()
		n, err := io.CopyBuffer(io.Discard, f, buf)
		if err != nil {
			return err
		}
		if n != seqFileSize {
			return fmt.Errorf("read %d bytes, want %d", n, seqFileSize)
		}
		return nil
	})

	r.phase("rand_read", randReads*randReadSize, randReads, func() error {
		f, err := os.Open(bigFile)
		if err != nil {
			return err
		}
		defer f.Close()
		block := make([]byte, randReadSize)
		for i := 0; i < randReads; i++ {
			off := rnd.Int63n(seqFileSize - randReadSize)
			if _, err := f.ReadAt(block, off); err != nil {
				return err
			}
		}
		return nil
	})

	smallDir := filepath.Join(r.dir, "small")
	small := buf[:smallSize]
	r.phase("small_write", smallCount*smallSize, smallCount, func() error {
		if err := os.MkdirAll(smallDir, 0755); err != nil {
			return err
		}
		for i := 0; i < smallCount; i++ {
			if err := os.WriteFile(filepath.Join(smallDir, fmt.Sprintf("f%04d.dat", i)), small, 0644); err != nil {
				return err
			}
		}
		return nil
	})

	r.phase("small_read", smallCount*smallSize, smallCount, func() error {
		for i := 0; i < smallCount; i++ {
			data, err := os.ReadFile(filepath.Join(smallDir, fmt.Sprintf("f%04d.dat", i)))
			if err != nil {
				return err
			}
			if len(data) != smallSize {
				return fmt.Errorf("file %d: read %d bytes, want %d", i, len(data), smallSize)
			}
		}
		return nil
	})

	r.phase("stat_files", 0, smallCount*statRounds, func() error {
		for round := 0; round < statRounds; round++ {
			for i := 0; i < smallCount; i++ {
				if _, err := os.Stat(filepath.Join(smallDir, fmt.Sprintf("f%04d.dat", i))); err != nil {
					return err
				}
			}
		}
		return nil
	})

	r.phase("list_dir", 0, listRounds, func() error {
		for round := 0; round < listRounds; round++ {
			entries, err := os.ReadDir(smallDir)
			if err != nil {
				return err
			}
			if len(entries) != smallCount {
				return fmt.Errorf("listed %d entries, want %d", len(entries), smallCount)
			}
		}
		return nil
	})

	r.phase("overwrite", overwriteN*smallSize, overwriteN, func() error {
		for i := 0; i < overwriteN; i++ {
			if err := os.WriteFile(filepath.Join(smallDir, fmt.Sprintf("f%04d.dat", i)), small, 0644); err != nil {
				return err
			}
		}
		return nil
	})

	r.phase("delete", 0, smallCount, func() error {
		for i := 0; i < smallCount; i++ {
			if err := os.Remove(filepath.Join(smallDir, fmt.Sprintf("f%04d.dat", i))); err != nil {
				return err
			}
		}
		return nil
	})

	if r.filer != "" {
		r.bigListPhase()
	}
}

// bigListPhase enumerates a directory whose files were created directly
// against the filer, so the listing is measured on its own rather than after
// this process has created (and possibly cached) every entry itself.
func (r *runner) bigListPhase() {
	// The bench dir is a direct child of the mount root, so its base name is
	// also its path on the filer.
	mountName := filepath.Base(r.dir)
	bigDir := filepath.Join(r.dir, "biglist")
	if err := seedFilerDir(r.filer, mountName+"/biglist", bigListCount); err != nil {
		log.Fatalf("seeding %d files via filer: %v", bigListCount, err)
	}

	r.phase("list_5k", 0, bigListCount*bigListRounds, func() error {
		for round := 0; round < bigListRounds; round++ {
			entries, err := os.ReadDir(bigDir)
			if err != nil {
				return err
			}
			if len(entries) != bigListCount {
				return fmt.Errorf("listed %d entries, want %d", len(entries), bigListCount)
			}
		}
		return nil
	})

	r.phase("walk_5k", 0, bigListCount, func() error {
		entries, err := os.ReadDir(bigDir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if _, err := entry.Info(); err != nil {
				return err
			}
		}
		return nil
	})
}

// seedFilerDir creates count tiny files under dir on the filer over plain
// HTTP, in parallel, bypassing the mount entirely.
func seedFilerDir(filer, dir string, count int) error {
	content := []byte("x")
	var wg sync.WaitGroup
	errs := make(chan error, count)
	sem := make(chan struct{}, 64)
	client := &http.Client{Timeout: 30 * time.Second}
	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			url := fmt.Sprintf("http://%s/%s/img%05d.jpg", filer, dir, i)
			if err := uploadOne(client, url, content); err != nil {
				errs <- fmt.Errorf("%s: %w", url, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func uploadOne(client *http.Client, url string, content []byte) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(url))
	if err != nil {
		return err
	}
	if _, err := part.Write(content); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

// printComparison renders the named result files side by side as a markdown
// table, one row per phase, with throughput where the phase moved bytes and
// operation rate otherwise.
func printComparison(files []string) error {
	var runs []runResult
	for _, file := range files {
		data, err := os.ReadFile(strings.TrimSpace(file))
		if err != nil {
			return err
		}
		var run runResult
		if err := json.Unmarshal(data, &run); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		runs = append(runs, run)
	}
	if len(runs) == 0 {
		return fmt.Errorf("no result files")
	}

	// Preserve the phase order of the first run, appending any extras.
	var order []string
	seen := map[string]bool{}
	for _, run := range runs {
		for _, p := range run.Phases {
			if !seen[p.Name] {
				seen[p.Name] = true
				order = append(order, p.Name)
			}
		}
	}
	byName := make([]map[string]phaseResult, len(runs))
	for i, run := range runs {
		byName[i] = map[string]phaseResult{}
		for _, p := range run.Phases {
			byName[i][p.Name] = p
		}
	}

	header := []string{"phase"}
	for _, run := range runs {
		header = append(header, run.Label+" (s)", run.Label+" rate")
	}
	fmt.Println("| " + strings.Join(header, " | ") + " |")
	fmt.Println("|" + strings.Repeat("---|", len(header)))
	for _, name := range order {
		row := []string{name}
		for i := range runs {
			p, ok := byName[i][name]
			if !ok {
				row = append(row, "-", "-")
				continue
			}
			row = append(row, fmt.Sprintf("%.2f", p.Seconds), strings.TrimSpace(rateSuffix(p.Bytes, p.Ops, p.Seconds)))
		}
		fmt.Println("| " + strings.Join(row, " | ") + " |")
	}
	return nil
}
