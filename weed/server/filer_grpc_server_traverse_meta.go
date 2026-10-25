package weed_server

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
	"google.golang.org/grpc/metadata"
)

// TraverseBfsMetadata streams every entry at and below req.Directory: by
// default a breadth-first walk, parents first, each response stamped with the
// path's commit timestamp. With request metadata TraverseModeHeaderKey=snapshot
// the entries come from one store snapshot instead (filer.EntryScanner), in any
// order, and the response header of the same name says which happened. Snapshot
// responses leave entry_ts_ns at 0: that lookup is one store round trip per
// entry, and the consumers that order by it use the walk.
func (fs *FilerServer) TraverseBfsMetadata(req *filer_pb.TraverseBfsMetadataRequest, stream filer_pb.SeaweedFiler_TraverseBfsMetadataServer) error {

	glog.V(0).Infof("TraverseBfsMetadata %v", req)

	excludedTrie := ptrie.New[bool]()
	for _, excluded := range req.ExcludedPrefixes {
		excludedTrie.Put([]byte(excluded), true)
	}

	ctx := stream.Context()

	isExcluded := func(path util.FullPath) bool {
		excluded := false
		excludedTrie.MatchPrefix([]byte(path), func(key []byte, value bool) bool {
			// Only exclude when the prefix ends on a path-component boundary,
			// so /a/b does not also exclude a sibling like /a/bc.
			if len(path) == len(key) || path[len(key)] == '/' {
				excluded = true
				return false
			}
			return true
		})
		return excluded
	}

	// Send each entry as it is visited and queue only directory paths to
	// descend into. Queuing the entries themselves would hold the whole
	// subtree in memory, including every file's chunk list, and can exhaust
	// the filer on large trees (e.g. during a peer's first-time bootstrap).
	send := func(entry *filer.Entry, entryTsNs int64) error {
		parent, _ := entry.FullPath.DirAndName()
		if err := stream.Send(&filer_pb.TraverseBfsMetadataResponse{
			Directory: parent,
			Entry:     entry.ToProtoEntry(),
			EntryTsNs: entryTsNs,
		}); err != nil {
			return fmt.Errorf("send traverse bfs metadata response: %w", err)
		}
		return nil
	}
	sendEntry := func(entry *filer.Entry) error {
		entryTsNs, err := fs.filer.EntryCommitTsNs(ctx, entry, entry.Mtime.UnixNano())
		if err != nil {
			return fmt.Errorf("commit timestamp of %s: %w", entry.FullPath, err)
		}
		return send(entry, entryTsNs)
	}
	sendEntryUnstamped := func(entry *filer.Entry) error {
		return send(entry, 0)
	}

	dirEntry, err := fs.filer.FindEntry(ctx, util.FullPath(req.Directory))
	if err != nil {
		return fmt.Errorf("find dir %s: %v", req.Directory, err)
	}
	if isExcluded(dirEntry.FullPath) {
		return nil
	}

	if traverseSnapshotRequested(ctx) {
		if scanner, ok := fs.filer.Store.(filer.EntryScanner); ok && dirEntry.IsDirectory() {
			served, scanErr := traverseFromSnapshot(ctx, scanner, dirEntry, isExcluded, stream.SendHeader, sendEntryUnstamped)
			if scanErr != nil {
				return scanErr
			}
			if served {
				return nil
			}
		}
		if err := stream.SendHeader(metadata.Pairs(filer_pb.TraverseModeHeaderKey, filer_pb.TraverseModeBfs)); err != nil {
			return fmt.Errorf("send traverse mode header: %w", err)
		}
	}

	if err := sendEntry(dirEntry); err != nil {
		return err
	}

	queue := util.NewQueue[util.FullPath]()
	if dirEntry.IsDirectory() {
		queue.Enqueue(dirEntry.FullPath)
	}

	for queue.Len() > 0 {
		dirPath := queue.Dequeue()
		if err := fs.iterateDirectory(ctx, dirPath, func(entry *filer.Entry) error {
			if isExcluded(entry.FullPath) {
				return nil
			}
			if err := sendEntry(entry); err != nil {
				return err
			}
			if entry.IsDirectory() {
				queue.Enqueue(entry.FullPath)
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

// traverseSnapshotRequested reports whether the client asked for the entries
// from one store snapshot rather than a breadth-first walk.
func traverseSnapshotRequested(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	modes := md.Get(filer_pb.TraverseModeHeaderKey)
	return len(modes) > 0 && modes[0] == filer_pb.TraverseModeSnapshot
}

// traverseFromSnapshot serves the traversal from the store's snapshot scan,
// root included: the scan yields the root's own row, so it is as current as
// its descendants (only "/" has no row and is sent as the synthetic root).
// It returns served=false, nothing sent, when the store declines before
// yielding anything; once the first entry is out, any failure is an error.
func traverseFromSnapshot(ctx context.Context, scanner filer.EntryScanner, dirEntry *filer.Entry, isExcluded func(util.FullPath) bool, sendHeader func(metadata.MD) error, sendEntry func(*filer.Entry) error) (served bool, err error) {
	started := false
	start := func() error {
		if started {
			return nil
		}
		started = true
		if err := sendHeader(metadata.Pairs(filer_pb.TraverseModeHeaderKey, filer_pb.TraverseModeSnapshot)); err != nil {
			return fmt.Errorf("send traverse mode header: %w", err)
		}
		if dirEntry.FullPath == "/" {
			return sendEntry(dirEntry)
		}
		return nil
	}

	scanErr := scanner.ScanEntries(ctx, dirEntry.FullPath, func(_ context.Context, entry *filer.Entry) error {
		if isExcluded(entry.FullPath) {
			return nil
		}
		if err := start(); err != nil {
			return err
		}
		return sendEntry(entry)
	})
	if scanErr != nil {
		if !started && errors.Is(scanErr, filer.ErrUnsupportedEntryScan) {
			glog.V(1).Infof("TraverseBfsMetadata %s: %v, walking instead", dirEntry.FullPath, scanErr)
			return false, nil
		}
		glog.V(0).Infof("TraverseBfsMetadata %s: snapshot scan failed: %v", dirEntry.FullPath, scanErr)
		return started, fmt.Errorf("snapshot scan of %s: %w", dirEntry.FullPath, scanErr)
	}
	// Nothing yielded (root gone since the lookup): still commit to the mode.
	if err := start(); err != nil {
		return true, err
	}
	return true, nil
}

func (fs *FilerServer) iterateDirectory(ctx context.Context, dirPath util.FullPath, fn func(entry *filer.Entry) error) (err error) {
	var lastFileName string
	var listErr error
	for {
		var hasEntries bool
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(ctx, dirPath, lastFileName, false, 1024, "", "", "", func(entry *filer.Entry) (bool, error) {
			hasEntries = true
			if fnErr := fn(entry); fnErr != nil {
				err = fnErr
				return false, err
			}
			return true, nil
		})
		if listErr != nil {
			return listErr
		}
		if err != nil {
			return err
		}
		if !hasEntries {
			return nil
		}
	}
}
