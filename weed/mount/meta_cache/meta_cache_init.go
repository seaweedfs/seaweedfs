package meta_cache

import (
	"context"
	"fmt"

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

		// Phase 1: Collect all entries from filer into memory (no lock held).
		// Network I/O happens here without blocking MetaCache operations.
		var allEntries []*filer.Entry

		fetchErr := util.Retry("ReadDirAllEntries", func() error {
			allEntries = nil // Reset on retry, allow GC of previous entries
			return filer_pb.ReadDirAllEntries(ctx, client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				entry := filer.FromPbEntry(string(path), pbEntry)
				if IsHiddenSystemEntry(string(path), entry.Name()) {
					return nil
				}
				allEntries = append(allEntries, entry)
				return nil
			})
		})

		if fetchErr != nil {
			return nil, fmt.Errorf("list %s: %w", path, fetchErr)
		}

		// Phase 2: Atomically replace all children under a single lock.
		// Concurrent DeleteEntry (from Unlink) and AtomicUpdateEntryFromFiler
		// (from subscription) will block during the replace, then execute after
		// the lock is released — correctly removing any stale entries that were
		// in the filer listing snapshot but deleted during collection.
		if err := mc.ReplaceDirectoryEntries(ctx, path, allEntries); err != nil {
			return nil, fmt.Errorf("replace entries for %s: %w", path, err)
		}

		mc.markCachedFn(path)
		return nil, nil
	})
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
