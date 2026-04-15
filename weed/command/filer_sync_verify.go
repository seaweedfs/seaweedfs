package command

import (
	"context"
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

type VerifyResult struct {
	dirCount      atomic.Int64
	fileCount     atomic.Int64
	missingCount  atomic.Int64
	sizeMismatch  atomic.Int64
	etagMismatch  atomic.Int64
	onlyInB       atomic.Int64
	skippedRecent atomic.Int64
}

type verifyDiffType int

const (
	diffMissing      verifyDiffType = iota // in A but not in B
	diffOnlyInB                            // in B but not in A
	diffSizeMismatch                       // size differs
	diffETagMismatch                       // etag differs
)

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
	grpcDialOptionA, grpcDialOptionB grpc.DialOption) error {

	clientA := &simpleFilerClient{grpcAddress: filerA, grpcDialOption: grpcDialOptionA}
	clientB := &simpleFilerClient{grpcAddress: filerB, grpcDialOption: grpcDialOptionB}

	var cutoffTime time.Time
	if modifyTimeAgo > 0 {
		cutoffTime = time.Now().Add(-modifyTimeAgo)
		fmt.Fprintf(os.Stdout, "Verifying files modified before %v (modifyTimeAgo=%v)\n", cutoffTime.Format(time.RFC3339), modifyTimeAgo)
	}

	fmt.Fprintf(os.Stdout, "Comparing %s%s => %s%s (isActivePassive=%v)\n\n",
		filerA, aPath, filerB, bPath, isActivePassive)

	result := &VerifyResult{}
	ctx := context.Background()

	err := compareDirectory(ctx, clientA, clientB, aPath, bPath, isActivePassive, cutoffTime, result)

	// print summary
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

	totalErrors := result.missingCount.Load() + result.sizeMismatch.Load() + result.etagMismatch.Load()
	if !isActivePassive {
		totalErrors += result.onlyInB.Load()
	}
	fmt.Fprintf(os.Stdout, "  Total errors:         %d\n", totalErrors)

	if err != nil {
		return err
	}
	if totalErrors > 0 {
		return fmt.Errorf("found %d differences", totalErrors)
	}
	return nil
}

func compareDirectory(ctx context.Context,
	clientA, clientB filer_pb.FilerClient,
	dirA, dirB string,
	isActivePassive bool,
	cutoffTime time.Time,
	result *VerifyResult) error {

	result.dirCount.Add(1)

	entriesA, errA := listEntries(ctx, clientA, dirA)
	if errA != nil {
		return fmt.Errorf("list %s on filer A: %v", dirA, errA)
	}
	entriesB, errB := listEntries(ctx, clientB, dirB)
	if errB != nil {
		return fmt.Errorf("list %s on filer B: %v", dirB, errB)
	}

	// collect subdirectories for recursive comparison
	type dirPair struct{ a, b string }
	var subDirs []dirPair

	i, j := 0, 0
	for i < len(entriesA) || j < len(entriesB) {
		var nameA, nameB string
		if i < len(entriesA) {
			nameA = entriesA[i].Name
		}
		if j < len(entriesB) {
			nameB = entriesB[j].Name
		}

		switch {
		case i < len(entriesA) && (j >= len(entriesB) || nameA < nameB):
			// entry only in A
			entryA := entriesA[i]
			if !cutoffTime.IsZero() && entryA.Attributes != nil && entryA.Attributes.Mtime > cutoffTime.Unix() {
				result.skippedRecent.Add(1)
			} else {
				reportDiff(diffMissing, dirA, entryA, nil, result)
				if entryA.IsDirectory {
					// directory missing in B: count all files under it as missing
					countMissingRecursive(ctx, clientA, fmt.Sprintf("%s/%s", dirA, entryA.Name), cutoffTime, result)
				}
			}
			i++

		case j < len(entriesB) && (i >= len(entriesA) || nameB < nameA):
			// entry only in B
			if !isActivePassive {
				reportDiff(diffOnlyInB, dirB, entriesB[j], nil, result)
			}
			j++

		default:
			// same name in both
			entryA := entriesA[i]
			entryB := entriesB[j]

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
			i++
			j++
		}
	}

	// recurse into subdirectories with concurrency
	if len(subDirs) > 0 {
		var wg sync.WaitGroup
		errCh := make(chan error, len(subDirs))
		sem := make(chan struct{}, 5) // concurrency limit

		for _, pair := range subDirs {
			wg.Add(1)
			go func(a, b string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := compareDirectory(ctx, clientA, clientB, a, b, isActivePassive, cutoffTime, result); err != nil {
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

func listEntries(ctx context.Context, client filer_pb.FilerClient, dir string) ([]*filer_pb.Entry, error) {
	var entries []*filer_pb.Entry
	err := filer_pb.ReadDirAllEntries(ctx, client, util.FullPath(dir), "", func(entry *filer_pb.Entry, isLast bool) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
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

func reportDiff(diffType verifyDiffType, dir string, entryA, entryB *filer_pb.Entry, result *VerifyResult) {
	path := fmt.Sprintf("%s/%s", dir, entryA.Name)

	switch diffType {
	case diffMissing:
		result.missingCount.Add(1)
		if entryA.IsDirectory {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s/ (directory)\n", path)
		} else {
			fmt.Fprintf(os.Stdout, "[MISSING]       %s (size=%d, etag=%s)\n",
				path, filer.FileSize(entryA), filer.ETag(entryA))
		}
	case diffOnlyInB:
		result.onlyInB.Add(1)
		fmt.Fprintf(os.Stdout, "[ONLY_IN_B]     %s\n", fmt.Sprintf("%s/%s", dir, entryA.Name))
	case diffSizeMismatch:
		result.sizeMismatch.Add(1)
		fmt.Fprintf(os.Stdout, "[SIZE_MISMATCH] %s (a=%d, b=%d)\n",
			path, filer.FileSize(entryA), filer.FileSize(entryB))
	case diffETagMismatch:
		result.etagMismatch.Add(1)
		fmt.Fprintf(os.Stdout, "[ETAG_MISMATCH] %s (a=%s, b=%s)\n",
			path, filer.ETag(entryA), filer.ETag(entryB))
	}
}

func countMissingRecursive(ctx context.Context, client filer_pb.FilerClient, dir string, cutoffTime time.Time, result *VerifyResult) {
	entries, err := listEntries(ctx, client, dir)
	if err != nil {
		glog.Warningf("list missing directory %s: %v", dir, err)
		return
	}
	for _, entry := range entries {
		if entry.IsDirectory {
			countMissingRecursive(ctx, client, fmt.Sprintf("%s/%s", dir, entry.Name), cutoffTime, result)
		} else {
			if !cutoffTime.IsZero() && entry.Attributes != nil && entry.Attributes.Mtime > cutoffTime.Unix() {
				result.skippedRecent.Add(1)
				continue
			}
			result.missingCount.Add(1)
			fmt.Fprintf(os.Stdout, "[MISSING]       %s/%s (size=%d, etag=%s)\n",
				dir, entry.Name, filer.FileSize(entry), filer.ETag(entry))
		}
	}
}
