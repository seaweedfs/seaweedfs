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
			batch = batch[:0] // Reset batch on retry
			return filer_pb.ReadDirAllEntries(context.Background(), client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				entry := filer.FromPbEntry(string(path), pbEntry)
				if IsHiddenSystemEntry(string(path), entry.Name()) {
					return nil
				}

				batch = append(batch, entry)

				// Flush batch when it reaches the threshold or on last entry
				if len(batch) >= batchInsertSize || isLast {
					// No lock needed - LevelDB Write() is thread-safe
					if err := mc.doBatchInsertEntries(batch); err != nil {
						glog.V(0).Infof("batch insert %s: %v", path, err)
						return err
					}
					batch = batch[:0] // Reset for next batch
				}
				return nil
			})
		})

		if fetchErr != nil {
			return nil, fmt.Errorf("list %s: %v", path, fetchErr)
		}
		mc.markCachedFn(path)
		return nil, nil
	})
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
