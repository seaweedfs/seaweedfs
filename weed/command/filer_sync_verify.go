package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type SyncVerifyOptions struct {
	filerA          *string
	filerB          *string
	aPath           *string
	bPath           *string
	aSecurity       *string
	bSecurity       *string
	isActivePassive *bool
	modifiedTimeAgo *time.Duration
	jsonOutput      *bool
}

var syncVerifyOptions SyncVerifyOptions

func init() {
	cmdFilerSyncVerify.Run = runFilerSyncVerify // break init cycle
	syncVerifyOptions.filerA = cmdFilerSyncVerify.Flag.String("a", "", "filer A in one SeaweedFS cluster")
	syncVerifyOptions.filerB = cmdFilerSyncVerify.Flag.String("b", "", "filer B in the other SeaweedFS cluster")
	syncVerifyOptions.aPath = cmdFilerSyncVerify.Flag.String("a.path", "/", "directory to verify on filer A")
	syncVerifyOptions.bPath = cmdFilerSyncVerify.Flag.String("b.path", "/", "directory to verify on filer B")
	syncVerifyOptions.aSecurity = cmdFilerSyncVerify.Flag.String("a.security", "", "security.toml file for filer A when clusters use different certificates")
	syncVerifyOptions.bSecurity = cmdFilerSyncVerify.Flag.String("b.security", "", "security.toml file for filer B when clusters use different certificates")
	syncVerifyOptions.isActivePassive = cmdFilerSyncVerify.Flag.Bool("isActivePassive", false, "one directional comparison from A to B; entries only in B are not reported")
	syncVerifyOptions.modifiedTimeAgo = cmdFilerSyncVerify.Flag.Duration("modifiedTimeAgo", 0, "only verify files modified before this duration ago (e.g. 1h) for sync-lag tolerance")
	syncVerifyOptions.jsonOutput = cmdFilerSyncVerify.Flag.Bool("jsonOutput", false, "emit NDJSON output (one JSON object per line) for external tooling")
}

var cmdFilerSyncVerify = &Command{
	UsageLine: "filer.sync.verify -a=<oneFilerHost>:<oneFilerPort> -b=<otherFilerHost>:<otherFilerPort>",
	Short:     "compare entries between two filers and report differences",
	Long: `compare entries between two filers and report differences, then exit.

	Useful for validating active/passive sync targets agree with the source.
	Reports MISSING (in A but not in B), ONLY_IN_B (in B but not in A; suppressed
	in active-passive mode), SIZE_MISMATCH, and ETAG_MISMATCH. Honors
	-modifiedTimeAgo to skip recently-modified files (sync-lag tolerance) and
	-isActivePassive for unidirectional comparison.

	Exits with code 0 on agreement, 2 on differences or operational errors.

`,
}

func runFilerSyncVerify(cmd *Command, args []string) bool {
	*syncVerifyOptions.aSecurity = util.ResolvePath(*syncVerifyOptions.aSecurity)
	*syncVerifyOptions.bSecurity = util.ResolvePath(*syncVerifyOptions.bSecurity)
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	grpcDialOptionA := grpcDialOption
	grpcDialOptionB := grpcDialOption
	if *syncVerifyOptions.aSecurity != "" {
		var err error
		if grpcDialOptionA, err = security.LoadClientTLSFromFile(*syncVerifyOptions.aSecurity, "grpc.client"); err != nil {
			glog.Fatalf("load security config for filer A: %v", err)
		}
	}
	if *syncVerifyOptions.bSecurity != "" {
		var err error
		if grpcDialOptionB, err = security.LoadClientTLSFromFile(*syncVerifyOptions.bSecurity, "grpc.client"); err != nil {
			glog.Fatalf("load security config for filer B: %v", err)
		}
	}

	filerA := pb.ServerAddress(*syncVerifyOptions.filerA)
	filerB := pb.ServerAddress(*syncVerifyOptions.filerB)

	if err := runVerifySync(filerA, filerB, *syncVerifyOptions.aPath, *syncVerifyOptions.bPath,
		*syncVerifyOptions.isActivePassive, *syncVerifyOptions.modifiedTimeAgo,
		*syncVerifyOptions.jsonOutput,
		grpcDialOptionA, grpcDialOptionB); err != nil {
		glog.Errorf("verify sync: %v", err)
		os.Exit(2)
	}
	return true
}

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

// isTooRecent reports whether entry's mtime is past cutoff (sync-lag tolerance).
// Returns false when cutoff is zero or attributes are missing.
func isTooRecent(entry *filer_pb.Entry, cutoff time.Time) bool {
	return !cutoff.IsZero() && entry != nil && entry.Attributes != nil && entry.Attributes.Mtime > cutoff.Unix()
}

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
	isActivePassive bool, modifiedTimeAgo time.Duration,
	jsonOutput bool,
	grpcDialOptionA, grpcDialOptionB grpc.DialOption) error {

	clientA := &simpleFilerClient{grpcAddress: filerA, grpcDialOption: grpcDialOptionA}
	clientB := &simpleFilerClient{grpcAddress: filerB, grpcDialOption: grpcDialOptionB}

	var cutoffTime time.Time
	if modifiedTimeAgo > 0 {
		cutoffTime = time.Now().Add(-modifiedTimeAgo)
	}

	if !jsonOutput {
		if !cutoffTime.IsZero() {
			fmt.Fprintf(os.Stdout, "Verifying files modified before %v (modifiedTimeAgo=%v)\n",
				cutoffTime.Format(time.RFC3339), modifiedTimeAgo)
		}
		fmt.Fprintf(os.Stdout, "Comparing %s%s => %s%s (isActivePassive=%v)\n\n",
			filerA, aPath, filerB, bPath, isActivePassive)
	}

	result := &VerifyResult{jsonOutput: jsonOutput}
	ctx := context.Background()
	sem := make(chan struct{}, verifySyncConcurrency)

	if err := compareDirectory(ctx, clientA, clientB, aPath, bPath, isActivePassive, cutoffTime, sem, result); err != nil {
		return err
	}

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
			if entryA.IsDirectory {
				// Always recurse for missing-in-B directories: a recent
				// child write can bump the parent's mtime even though
				// older missing files exist underneath. The cutoff is
				// applied per-file inside countMissingRecursive.
				reportDiff(diffMissing, dirA, entryA, nil, result)
				countMissingRecursive(ctx, clientA, path.Join(dirA, entryA.Name), cutoffTime, result)
			} else if isTooRecent(entryA, cutoffTime) {
				result.skippedRecent.Add(1)
			} else {
				reportDiff(diffMissing, dirA, entryA, nil, result)
			}

		case eB != nil && (eA == nil || eB.Name < eA.Name):
			// entry only in B
			entryB := streamB.advance()
			if !isActivePassive {
				if isTooRecent(entryB, cutoffTime) {
					result.skippedRecent.Add(1)
				} else {
					reportDiff(diffOnlyInB, dirB, entryB, nil, result)
				}
			}

		default:
			// same name in both
			entryA := streamA.advance()
			entryB := streamB.advance()

			if entryA.IsDirectory && entryB.IsDirectory {
				subDirs = append(subDirs, dirPair{
					a: path.Join(dirA, entryA.Name),
					b: path.Join(dirB, entryB.Name),
				})
			} else if !entryA.IsDirectory && !entryB.IsDirectory {
				// Skip if either side was modified recently (sync-lag tolerance).
				if isTooRecent(entryA, cutoffTime) || isTooRecent(entryB, cutoffTime) {
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
		// Bounded worker pool: cap goroutines per directory level instead
		// of spawning one per child. A directory with thousands of subdirs
		// would otherwise park ~2KB per waiting goroutine even though
		// only `verifySyncConcurrency` can do I/O at once.
		workers := verifySyncConcurrency
		if len(subDirs) < workers {
			workers = len(subDirs)
		}
		jobs := make(chan dirPair, len(subDirs))
		errCh := make(chan error, 1) // first error wins; others dropped
		var wg sync.WaitGroup

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for pair := range jobs {
					if err := compareDirectory(ctx, clientA, clientB, pair.a, pair.b, isActivePassive, cutoffTime, sem, result); err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}
			}()
		}
		for _, pair := range subDirs {
			jobs <- pair
		}
		close(jobs)
		wg.Wait()
		close(errCh)
		if err := <-errCh; err != nil {
			return err
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
	entryPath := path.Join(dir, entryA.Name)

	result.outputMu.Lock()
	defer result.outputMu.Unlock()

	switch diffType {
	case diffMissing:
		if entryA.IsDirectory {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s/ (directory)\n", entryPath)
		} else {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s (size=%d, etag=%s)\n",
				entryPath, filer.FileSize(entryA), filer.ETag(entryA))
		}
	case diffOnlyInB:
		fmt.Fprintf(os.Stdout, "[ONLY_IN_B]     %s\n", entryPath)
	case diffSizeMismatch:
		ann := annotation(entryA, entryB)
		fmt.Fprintf(os.Stdout, "[SIZE_MISMATCH] %s (a=%d, b=%d%s)\n",
			entryPath, filer.FileSize(entryA), filer.FileSize(entryB), ann)
	case diffETagMismatch:
		ann := annotation(entryA, entryB)
		fmt.Fprintf(os.Stdout, "[ETAG_MISMATCH] %s (a=%s, b=%s%s)\n",
			entryPath, filer.ETag(entryA), filer.ETag(entryB), ann)
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
	rec := diffRecord{Path: path.Join(dir, entryA.Name)}

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
				countMissingRecursive(ctx, client, path.Join(dir, entry.Name), cutoffTime, result)
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
