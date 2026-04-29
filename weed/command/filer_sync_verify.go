package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// verifySyncConcurrency caps concurrent directory I/O across the entire
// recursive walk. A single shared semaphore is created in runVerifySync and
// passed down so the limit applies globally — a per-call semaphore would only
// cap concurrency per directory level, allowing fanout to grow as
// verifySyncConcurrency^depth on deep trees.
//
// Trade-off: higher values reduce wall time on wide trees by parallelizing
// listEntries RPCs, at the cost of more concurrent load on both filers and
// more memory from queued goroutines (each waiting goroutine ~2KB stack).
// Each compareDirectory holds a slot only for its own listing+compare phase
// and releases before recursing, so a parent never blocks waiting for
// children to acquire slots.
const verifySyncConcurrency = 5

type VerifyResult struct {
	dirCount      atomic.Int64
	fileCount     atomic.Int64
	missingCount  atomic.Int64
	sizeMismatch  atomic.Int64
	etagMismatch  atomic.Int64
	onlyInB       atomic.Int64
	skippedRecent atomic.Int64

	// outputMu serializes writes to stdout. Multiple goroutines call
	// reportDiff concurrently from compareDirectory worker pool.
	outputMu   sync.Mutex
	jsonOutput bool
}

type verifyDiffType int

const (
	diffMissing      verifyDiffType = iota // in A but not in B
	diffOnlyInB                            // in B but not in A
	diffSizeMismatch                       // size differs
	diffETagMismatch                       // etag differs
)

// diffRecord is the JSON Lines schema for a single diff entry.
type diffRecord struct {
	Type          string       `json:"type"`
	Path          string       `json:"path"`
	IsDirectory   bool         `json:"isDirectory,omitempty"`
	A             *entryRecord `json:"a,omitempty"`
	B             *entryRecord `json:"b,omitempty"`
	MtimeRelation string       `json:"mtimeRelation,omitempty"` // EQUAL | A_NEWER | B_NEWER
	MtimeDelta    string       `json:"mtimeDelta,omitempty"`    // human-readable, e.g. "5d", "12h"
	Hint          string       `json:"hint,omitempty"`          // late_updates_skip_likely | sync_lag_or_event_miss
}

type entryRecord struct {
	Size  uint64 `json:"size"`
	Mtime int64  `json:"mtime"`
	ETag  string `json:"etag,omitempty"`
}

type summaryRecord struct {
	Type          string `json:"type"`
	Directories   int64  `json:"directories"`
	Files         int64  `json:"files"`
	SkippedRecent int64  `json:"skippedRecent"`
	Missing       int64  `json:"missing"`
	SizeMismatch  int64  `json:"sizeMismatch"`
	ETagMismatch  int64  `json:"etagMismatch"`
	OnlyInB       int64  `json:"onlyInB"`
	TotalErrors   int64  `json:"totalErrors"`
}

// simpleFilerClient implements filer_pb.FilerClient for gRPC connections
type simpleFilerClient struct {
	grpcAddress    pb.ServerAddress
	grpcDialOption grpc.DialOption
}

func (c *simpleFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcClient(streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, c.grpcAddress.ToGrpcAddress(), false, c.grpcDialOption)
}

func (c *simpleFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (c *simpleFilerClient) GetDataCenter() string {
	return ""
}

func runVerifySync(filerA, filerB pb.ServerAddress, aPath, bPath string,
	isActivePassive bool, modifyTimeAgo time.Duration,
	jsonOutput bool,
	grpcDialOptionA, grpcDialOptionB grpc.DialOption) error {

	clientA := &simpleFilerClient{grpcAddress: filerA, grpcDialOption: grpcDialOptionA}
	clientB := &simpleFilerClient{grpcAddress: filerB, grpcDialOption: grpcDialOptionB}

	var cutoffTime time.Time
	if modifyTimeAgo > 0 {
		cutoffTime = time.Now().Add(-modifyTimeAgo)
	}

	if !jsonOutput {
		if !cutoffTime.IsZero() {
			fmt.Fprintf(os.Stdout, "Verifying files modified before %v (modifyTimeAgo=%v)\n",
				cutoffTime.Format(time.RFC3339), modifyTimeAgo)
		}
		fmt.Fprintf(os.Stdout, "Comparing %s%s => %s%s (isActivePassive=%v)\n\n",
			filerA, aPath, filerB, bPath, isActivePassive)
	}

	result := &VerifyResult{jsonOutput: jsonOutput}
	ctx := context.Background()
	sem := make(chan struct{}, verifySyncConcurrency)

	err := compareDirectory(ctx, clientA, clientB, aPath, bPath, isActivePassive, cutoffTime, sem, result)

	totalErrors := result.missingCount.Load() + result.sizeMismatch.Load() + result.etagMismatch.Load()
	if !isActivePassive {
		totalErrors += result.onlyInB.Load()
	}

	if jsonOutput {
		summary := summaryRecord{
			Type:          "SUMMARY",
			Directories:   result.dirCount.Load(),
			Files:         result.fileCount.Load(),
			SkippedRecent: result.skippedRecent.Load(),
			Missing:       result.missingCount.Load(),
			SizeMismatch:  result.sizeMismatch.Load(),
			ETagMismatch:  result.etagMismatch.Load(),
			OnlyInB:       result.onlyInB.Load(),
			TotalErrors:   totalErrors,
		}
		writeJSONLine(result, summary)
	} else {
		fmt.Fprintf(os.Stdout, "\nSummary:\n")
		fmt.Fprintf(os.Stdout, "  Directories compared: %d\n", result.dirCount.Load())
		fmt.Fprintf(os.Stdout, "  Files verified:       %d\n", result.fileCount.Load())
		if result.skippedRecent.Load() > 0 {
			fmt.Fprintf(os.Stdout, "  Skipped (too recent): %d\n", result.skippedRecent.Load())
		}
		fmt.Fprintf(os.Stdout, "  Missing in B:         %d\n", result.missingCount.Load())
		fmt.Fprintf(os.Stdout, "  Size mismatch:        %d\n", result.sizeMismatch.Load())
		fmt.Fprintf(os.Stdout, "  ETag mismatch:        %d\n", result.etagMismatch.Load())
		if !isActivePassive {
			fmt.Fprintf(os.Stdout, "  Only in B:            %d\n", result.onlyInB.Load())
		}
		fmt.Fprintf(os.Stdout, "  Total errors:         %d\n", totalErrors)
	}

	if err != nil {
		return err
	}
	if totalErrors > 0 {
		return fmt.Errorf("found %d differences", totalErrors)
	}
	return nil
}

// entryStream is a sorted, streaming view of a single directory's entries.
// A background goroutine pages through the directory via ReadDirAllEntries
// and forwards each entry to a buffered channel; the caller consumes entries
// one at a time through peek/advance. Memory usage is O(channel buffer) —
// independent of directory size — rather than O(total entries).
type entryStream struct {
	ch   <-chan *filer_pb.Entry
	head *filer_pb.Entry
	done bool
	err  error // written before ch is closed; safe to read once done==true
}

// newEntryStream starts the background goroutine. It exits when listing
// completes, an error occurs, or ctx is cancelled; the channel is always
// closed before exit so consumers do not block indefinitely.
func newEntryStream(ctx context.Context, client filer_pb.FilerClient, dir string) *entryStream {
	ch := make(chan *filer_pb.Entry, 64)
	s := &entryStream{ch: ch}
	go func() {
		defer close(ch)
		s.err = filer_pb.ReadDirAllEntries(ctx, client, util.FullPath(dir), "",
			func(entry *filer_pb.Entry, isLast bool) error {
				select {
				case ch <- entry:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
	}()
	return s
}

// peek returns the next entry without consuming it, or nil at end-of-stream.
func (s *entryStream) peek() *filer_pb.Entry {
	if s.done {
		return nil
	}
	if s.head == nil {
		e, ok := <-s.ch
		if !ok {
			s.done = true
			return nil
		}
		s.head = e
	}
	return s.head
}

// advance consumes and returns the next entry.
func (s *entryStream) advance() *filer_pb.Entry {
	e := s.peek()
	s.head = nil
	return e
}

func compareDirectory(ctx context.Context,
	clientA, clientB filer_pb.FilerClient,
	dirA, dirB string,
	isActivePassive bool,
	cutoffTime time.Time,
	sem chan struct{},
	result *VerifyResult) error {

	// Hold a slot only for this directory's I/O phase (listings + merge).
	// Released before recursing so parents never block waiting for children
	// to acquire slots — see verifySyncConcurrency for the rationale.
	sem <- struct{}{}
	released := false
	releaseSlot := func() {
		if !released {
			released = true
			<-sem
		}
	}
	defer releaseSlot()

	result.dirCount.Add(1)

	// A child context ensures that stream goroutines are cancelled and their
	// channels are closed if compareDirectory returns early (e.g. on error).
	mergeCtx, cancelMerge := context.WithCancel(ctx)
	defer cancelMerge()

	streamA := newEntryStream(mergeCtx, clientA, dirA)
	streamB := newEntryStream(mergeCtx, clientB, dirB)

	// collect subdirectories for recursive comparison
	type dirPair struct{ a, b string }
	var subDirs []dirPair

	for streamA.peek() != nil || streamB.peek() != nil {
		eA := streamA.peek()
		eB := streamB.peek()

		switch {
		case eA != nil && (eB == nil || eA.Name < eB.Name):
			// entry only in A
			entryA := streamA.advance()
			if !cutoffTime.IsZero() && entryA.Attributes != nil && entryA.Attributes.Mtime > cutoffTime.Unix() {
				result.skippedRecent.Add(1)
			} else {
				reportDiff(diffMissing, dirA, entryA, nil, result)
				if entryA.IsDirectory {
					// directory missing in B: count all files under it as missing
					countMissingRecursive(ctx, clientA, fmt.Sprintf("%s/%s", dirA, entryA.Name), cutoffTime, result)
				}
			}

		case eB != nil && (eA == nil || eB.Name < eA.Name):
			// entry only in B
			entryB := streamB.advance()
			if !isActivePassive {
				reportDiff(diffOnlyInB, dirB, entryB, nil, result)
			}

		default:
			// same name in both
			entryA := streamA.advance()
			entryB := streamB.advance()

			if entryA.IsDirectory && entryB.IsDirectory {
				subDirs = append(subDirs, dirPair{
					a: fmt.Sprintf("%s/%s", dirA, entryA.Name),
					b: fmt.Sprintf("%s/%s", dirB, entryB.Name),
				})
			} else if !entryA.IsDirectory && !entryB.IsDirectory {
				// skip recently modified files
				if !cutoffTime.IsZero() && entryA.Attributes != nil && entryA.Attributes.Mtime > cutoffTime.Unix() {
					result.skippedRecent.Add(1)
				} else {
					compareEntries(dirA, entryA, entryB, result)
				}
			} else {
				// type mismatch: one is dir, other is file
				reportDiff(diffMissing, dirA, entryA, nil, result)
				if !isActivePassive {
					reportDiff(diffOnlyInB, dirB, entryB, nil, result)
				}
			}
		}
	}

	// Both channels are closed: close happens-before the receive of the zero
	// value, so stream.err is visible here without additional synchronisation.
	if err := streamA.err; err != nil && err != context.Canceled {
		return fmt.Errorf("list %s on filer A: %v", dirA, err)
	}
	if err := streamB.err; err != nil && err != context.Canceled {
		return fmt.Errorf("list %s on filer B: %v", dirB, err)
	}

	// Release our slot before recursing so children can acquire it. Holding
	// it across wg.Wait would deadlock once depth exceeds verifySyncConcurrency.
	releaseSlot()

	if len(subDirs) > 0 {
		var wg sync.WaitGroup
		errCh := make(chan error, len(subDirs))

		for _, pair := range subDirs {
			wg.Add(1)
			go func(a, b string) {
				defer wg.Done()
				if err := compareDirectory(ctx, clientA, clientB, a, b, isActivePassive, cutoffTime, sem, result); err != nil {
					errCh <- err
				}
			}(pair.a, pair.b)
		}
		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				return err
			}
		}
	}

	return nil
}


func compareEntries(dir string, entryA, entryB *filer_pb.Entry, result *VerifyResult) {
	result.fileCount.Add(1)

	sizeA := filer.FileSize(entryA)
	sizeB := filer.FileSize(entryB)
	if sizeA != sizeB {
		reportDiff(diffSizeMismatch, dir, entryA, entryB, result)
		return
	}

	etagA := filer.ETag(entryA)
	etagB := filer.ETag(entryB)
	if etagA != etagB {
		reportDiff(diffETagMismatch, dir, entryA, entryB, result)
		return
	}
}

// mtimeRelation classifies B.mtime vs A.mtime. Both entries must be non-nil.
// Returns relation, absolute delta in seconds, and human-readable string.
func mtimeRelation(entryA, entryB *filer_pb.Entry) (relation, deltaStr string, deltaSec int64) {
	if entryA == nil || entryB == nil || entryA.Attributes == nil || entryB.Attributes == nil {
		return "", "", 0
	}
	delta := entryB.Attributes.Mtime - entryA.Attributes.Mtime
	abs := delta
	if abs < 0 {
		abs = -abs
	}
	switch {
	case delta == 0:
		return "EQUAL", "0s", 0
	case delta > 0:
		return "B_NEWER", formatSeconds(abs), abs
	default:
		return "A_NEWER", formatSeconds(abs), abs
	}
}

func formatSeconds(s int64) string {
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm", s/60)
	case s < 86400:
		return fmt.Sprintf("%dh", s/3600)
	default:
		return fmt.Sprintf("%dd", s/86400)
	}
}

// hintFor returns an automatic interpretation hint based on mtime relation.
// Only emitted for SIZE_MISMATCH and ETAG_MISMATCH where both entries exist.
func hintFor(relation string) string {
	switch relation {
	case "B_NEWER":
		return "late_updates_skip_likely"
	case "A_NEWER":
		return "sync_lag_or_event_miss"
	}
	return ""
}

func reportDiff(diffType verifyDiffType, dir string, entryA, entryB *filer_pb.Entry, result *VerifyResult) {
	switch diffType {
	case diffMissing:
		result.missingCount.Add(1)
	case diffOnlyInB:
		result.onlyInB.Add(1)
	case diffSizeMismatch:
		result.sizeMismatch.Add(1)
	case diffETagMismatch:
		result.etagMismatch.Add(1)
	}

	if result.jsonOutput {
		writeJSONDiff(result, diffType, dir, entryA, entryB)
	} else {
		writeTextDiff(result, diffType, dir, entryA, entryB)
	}
}

func writeTextDiff(result *VerifyResult, diffType verifyDiffType, dir string, entryA, entryB *filer_pb.Entry) {
	path := fmt.Sprintf("%s/%s", dir, entryA.Name)

	result.outputMu.Lock()
	defer result.outputMu.Unlock()

	switch diffType {
	case diffMissing:
		if entryA.IsDirectory {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s/ (directory)\n", path)
		} else {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s (size=%d, etag=%s)\n",
				path, filer.FileSize(entryA), filer.ETag(entryA))
		}
	case diffOnlyInB:
		fmt.Fprintf(os.Stdout, "[ONLY_IN_B]     %s\n", path)
	case diffSizeMismatch:
		ann := annotation(entryA, entryB)
		fmt.Fprintf(os.Stdout, "[SIZE_MISMATCH] %s (a=%d, b=%d%s)\n",
			path, filer.FileSize(entryA), filer.FileSize(entryB), ann)
	case diffETagMismatch:
		ann := annotation(entryA, entryB)
		fmt.Fprintf(os.Stdout, "[ETAG_MISMATCH] %s (a=%s, b=%s%s)\n",
			path, filer.ETag(entryA), filer.ETag(entryB), ann)
	}
}

// annotation builds the trailing ", mtime: ... [hint]" segment for text output.
// Returns empty string if entries are unavailable.
func annotation(entryA, entryB *filer_pb.Entry) string {
	relation, delta, _ := mtimeRelation(entryA, entryB)
	if relation == "" {
		return ""
	}
	switch relation {
	case "EQUAL":
		return ", mtime equal [chunk-level issue]"
	case "B_NEWER":
		return fmt.Sprintf(", B newer +%s [late-updates skip likely]", delta)
	case "A_NEWER":
		return fmt.Sprintf(", A newer +%s [sync lag or event miss]", delta)
	}
	return ""
}

func writeJSONDiff(result *VerifyResult, diffType verifyDiffType, dir string, entryA, entryB *filer_pb.Entry) {
	path := fmt.Sprintf("%s/%s", dir, entryA.Name)
	rec := diffRecord{Path: path}

	switch diffType {
	case diffMissing:
		rec.Type = "MISSING"
		rec.IsDirectory = entryA.IsDirectory
		rec.A = toEntryRecord(entryA)
	case diffOnlyInB:
		rec.Type = "ONLY_IN_B"
		// for diffOnlyInB the existing convention passes the entry as entryA
		rec.IsDirectory = entryA.IsDirectory
		rec.B = toEntryRecord(entryA)
	case diffSizeMismatch:
		rec.Type = "SIZE_MISMATCH"
		rec.A = toEntryRecord(entryA)
		rec.B = toEntryRecord(entryB)
		relation, delta, _ := mtimeRelation(entryA, entryB)
		rec.MtimeRelation = relation
		rec.MtimeDelta = delta
		rec.Hint = hintFor(relation)
	case diffETagMismatch:
		rec.Type = "ETAG_MISMATCH"
		rec.A = toEntryRecord(entryA)
		rec.B = toEntryRecord(entryB)
		relation, delta, _ := mtimeRelation(entryA, entryB)
		rec.MtimeRelation = relation
		rec.MtimeDelta = delta
		rec.Hint = hintFor(relation)
	}

	writeJSONLine(result, rec)
}

func toEntryRecord(entry *filer_pb.Entry) *entryRecord {
	if entry == nil {
		return nil
	}
	r := &entryRecord{
		Size: filer.FileSize(entry),
		ETag: filer.ETag(entry),
	}
	if entry.Attributes != nil {
		r.Mtime = entry.Attributes.Mtime
	}
	return r
}

// writeJSONLine emits a single JSON object followed by newline. Holds outputMu
// across marshal+write so concurrent goroutines never interleave.
func writeJSONLine(result *VerifyResult, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		glog.Warningf("marshal verify record: %v", err)
		return
	}
	result.outputMu.Lock()
	defer result.outputMu.Unlock()
	os.Stdout.Write(data)
	os.Stdout.Write([]byte{'\n'})
}

func countMissingRecursive(ctx context.Context, client filer_pb.FilerClient, dir string, cutoffTime time.Time, result *VerifyResult) {
	err := filer_pb.ReadDirAllEntries(ctx, client, util.FullPath(dir), "",
		func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				countMissingRecursive(ctx, client, fmt.Sprintf("%s/%s", dir, entry.Name), cutoffTime, result)
				return nil
			}
			if !cutoffTime.IsZero() && entry.Attributes != nil && entry.Attributes.Mtime > cutoffTime.Unix() {
				result.skippedRecent.Add(1)
				return nil
			}
			reportDiff(diffMissing, dir, entry, nil, result)
			return nil
		})
	if err != nil {
		glog.Warningf("list missing directory %s: %v", dir, err)
	}
}
