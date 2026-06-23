package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/viant/ptrie"
)

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
	sendEntry := func(entry *filer.Entry) error {
		parent, _ := entry.FullPath.DirAndName()
		if err := stream.Send(&filer_pb.TraverseBfsMetadataResponse{
			Directory: parent,
			Entry:     entry.ToProtoEntry(),
		}); err != nil {
			return fmt.Errorf("send traverse bfs metadata response: %w", err)
		}
		return nil
	}

	dirEntry, err := fs.filer.FindEntry(ctx, util.FullPath(req.Directory))
	if err != nil {
		return fmt.Errorf("find dir %s: %v", req.Directory, err)
	}
	if isExcluded(dirEntry.FullPath) {
		return nil
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
