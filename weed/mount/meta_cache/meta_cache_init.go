package meta_cache

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) error {
	// Collect all uncached paths from target directory up to root
	var uncachedPaths []util.FullPath
	currentPath := dirPath

	for {
		// If this path is cached, all ancestors are also cached
		if mc.isCachedFn(currentPath) {
			break
		}
		uncachedPaths = append(uncachedPaths, currentPath)

		// Continue to parent directory
		if currentPath != mc.root {
			parent, _ := currentPath.DirAndName()
			currentPath = util.FullPath(parent)
		} else {
			break
		}
	}

	if len(uncachedPaths) == 0 {
		return nil
	}

	// Fetch all uncached directories in parallel with context for cancellation
	// If one fetch fails, cancel the others to avoid unnecessary work
	g, ctx := errgroup.WithContext(context.Background())
	for _, p := range uncachedPaths {
		path := p // capture for closure
		g.Go(func() error {
			return doEnsureVisited(ctx, mc, client, path)
		})
	}
	return g.Wait()
}

// batchInsertSize is the number of entries to accumulate before flushing to LevelDB.
// 100 provides a balance between memory usage (~100 Entry pointers) and write efficiency
// (fewer disk syncs). Larger values reduce I/O overhead but increase memory and latency.
const batchInsertSize = 100

const (
	emptyRebuildConfirmations = 2
	emptyRebuildConfirmDelay  = 50 * time.Millisecond
)

func doEnsureVisited(ctx context.Context, mc *MetaCache, client filer_pb.FilerClient, path util.FullPath) error {
	// Use singleflight to deduplicate concurrent requests for the same path
	_, err, _ := mc.visitGroup.Do(string(path), func() (interface{}, error) {
		// Check for cancellation before starting
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Double-check if already cached (another goroutine may have completed)
		if mc.isCachedFn(path) {
			return nil, nil
		}

		glog.V(4).Infof("ReadDirAllEntries %s ...", path)

		// Use context.Background() for build lifecycle calls so that
		// errgroup cancellation of ctx doesn't cause enqueueAndWait to
		// return early, which would trigger cleanupBuild while the
		// operation is still queued.
		if err := mc.BeginDirectoryBuild(context.Background(), path); err != nil {
			return nil, fmt.Errorf("begin build %s: %w", path, err)
		}
		cleanupDone := false
		cleanupBuild := func(reason string) {
			if cleanupDone {
				return
			}
			cleanupDone = true
			if deleteErr := mc.deleteFolderChildrenForRebuild(context.Background(), path); deleteErr != nil {
				glog.V(2).Infof("clear %s build %s: %v", reason, path, deleteErr)
			}
			if abortErr := mc.AbortDirectoryBuild(context.Background(), path); abortErr != nil {
				glog.V(2).Infof("abort %s build %s: %v", reason, path, abortErr)
			}
		}
		defer func() {
			if !cleanupDone && ctx.Err() != nil {
				cleanupBuild("canceled")
			}
		}()

		// reloadFromFiler wipes the cached children and reloads them from the filer.
		reloadFromFiler := func() (entryCount int, snapshotTsNs int64, err error) {
			err = util.Retry("ReadDirAllEntries", func() error {
				entryCount = 0
				var batch []*filer.Entry // reset on retry, allow GC of previous entries
				if err := mc.deleteFolderChildrenForRebuild(ctx, path); err != nil {
					return fmt.Errorf("clear existing entries for %s: %w", path, err)
				}
				var listErr error
				snapshotTsNs, listErr = filer_pb.ReadDirAllEntriesWithSnapshot(ctx, client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
					entry := filer.FromPbEntry(string(path), pbEntry)
					if !mc.includeSystemEntries && IsHiddenSystemEntry(string(path), entry.Name()) {
						return nil
					}

					batch = append(batch, entry)
					entryCount++

					// flush by size, not isLast: hidden entries can return early
					if len(batch) >= batchInsertSize {
						if err := mc.doBatchInsertEntries(ctx, batch); err != nil {
							return fmt.Errorf("batch insert for %s: %w", path, err)
						}
						batch = make([]*filer.Entry, 0, batchInsertSize)
					}
					return nil
				})
				if listErr != nil {
					return listErr
				}
				if len(batch) > 0 {
					if err := mc.doBatchInsertEntries(ctx, batch); err != nil {
						return fmt.Errorf("batch insert remaining for %s: %w", path, err)
					}
				}
				return nil
			})
			return entryCount, snapshotTsNs, err
		}

		entryCount, snapshotTsNs, fetchErr := reloadFromFiler()
		if fetchErr != nil {
			cleanupBuild("failed")
			return nil, fmt.Errorf("list %s: %w", path, fetchErr)
		}

		// A transient empty listing would strand a populated directory cached over
		// an empty store; re-read to confirm before trusting it. First re-read is
		// immediate (a clean-EOF stream glitch clears at once), later ones space out.
		// On cancellation the deferred cleanup aborts the build.
		for attempt := 0; entryCount == 0 && attempt < emptyRebuildConfirmations; attempt++ {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt > 0 {
				select {
				case <-time.After(emptyRebuildConfirmDelay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			if entryCount, snapshotTsNs, fetchErr = reloadFromFiler(); fetchErr != nil {
				cleanupBuild("failed")
				return nil, fmt.Errorf("confirm empty list %s: %w", path, fetchErr)
			}
			if entryCount > 0 {
				glog.Warningf("rebuild of %s saw a transient empty listing, recovered %d entries on confirmation", path, entryCount)
			}
		}

		if err := mc.CompleteDirectoryBuild(context.Background(), path, snapshotTsNs); err != nil {
			cleanupBuild("unreplayed")
			return nil, fmt.Errorf("complete build for %s: %w", path, err)
		}
		cleanupDone = true // Prevent deferred cleanup after successful publish
		return nil, nil
	})
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
