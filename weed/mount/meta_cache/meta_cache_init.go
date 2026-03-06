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

		// Start buffering subscription events for this directory.
		// Any events that arrive during the fetch will be replayed
		// after the snapshot is committed, preventing lost mutations.
		mc.BeginRefresh(path)

		// Collect all entries from the filer. No lock is held during
		// network I/O so subscription events can still be buffered.
		var allEntries []*filer.Entry

		fetchErr := util.Retry("ReadDirAllEntries", func() error {
			allEntries = nil // Reset on retry
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
			mc.CancelRefresh(path)
			return nil, fmt.Errorf("list %s: %w", path, fetchErr)
		}

		// Atomically replace cached entries with the snapshot and replay
		// any buffered events that arrived during the fetch.
		if err := mc.CommitRefresh(ctx, path, allEntries); err != nil {
			return nil, fmt.Errorf("commit refresh for %s: %w", path, err)
		}
		mc.markCachedFn(path)
		return nil, nil
	})
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
