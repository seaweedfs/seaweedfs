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

	// Fetch all uncached directories in parallel
	// singleflight in doEnsureVisited handles deduplication
	g := new(errgroup.Group)
	for _, p := range uncachedPaths {
		path := p // capture for closure
		g.Go(func() error {
			return doEnsureVisited(mc, client, path)
		})
	}
	return g.Wait()
}

// batchInsertSize is the number of entries to accumulate before flushing to LevelDB.
// 100 provides a balance between memory usage (~100 Entry pointers) and write efficiency
// (fewer disk syncs). Larger values reduce I/O overhead but increase memory and latency.
const batchInsertSize = 100

func doEnsureVisited(mc *MetaCache, client filer_pb.FilerClient, path util.FullPath) error {
	// Use singleflight to deduplicate concurrent requests for the same path
	_, err, _ := mc.visitGroup.Do(string(path), func() (interface{}, error) {
		// Double-check if already cached (another goroutine may have completed)
		if mc.isCachedFn(path) {
			return nil, nil
		}

		glog.V(4).Infof("ReadDirAllEntries %s ...", path)

		// Collect entries in batches for efficient LevelDB writes
		var batch []*filer.Entry

		fetchErr := util.Retry("ReadDirAllEntries", func() error {
			batch = nil // Reset batch on retry, allow GC of previous entries
			return filer_pb.ReadDirAllEntries(context.Background(), client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				entry := filer.FromPbEntry(string(path), pbEntry)
				if IsHiddenSystemEntry(string(path), entry.Name()) {
					return nil
				}

				batch = append(batch, entry)

				// Flush batch when it reaches the threshold
				// Don't rely on isLast here - hidden entries may cause early return
				if len(batch) >= batchInsertSize {
					// No lock needed - LevelDB Write() is thread-safe
					if err := mc.doBatchInsertEntries(batch); err != nil {
						return fmt.Errorf("batch insert for %s: %w", path, err)
					}
					// Create new slice to allow GC of flushed entries
					batch = make([]*filer.Entry, 0, batchInsertSize)
				}
				return nil
			})
		})

		if fetchErr != nil {
			return nil, fmt.Errorf("list %s: %v", path, fetchErr)
		}

		// Flush any remaining entries in the batch
		if len(batch) > 0 {
			if err := mc.doBatchInsertEntries(batch); err != nil {
				return nil, fmt.Errorf("batch insert remaining for %s: %w", path, err)
			}
		}
		mc.markCachedFn(path)
		return nil, nil
	})
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
