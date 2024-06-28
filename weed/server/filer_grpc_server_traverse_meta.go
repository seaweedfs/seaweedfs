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

	queue := util.NewQueue[*filer.Entry]()
	dirEntry, err := fs.filer.FindEntry(ctx, util.FullPath(req.Directory))
	if err != nil {
		return fmt.Errorf("find dir %s: %v", req.Directory, err)
	}
	queue.Enqueue(dirEntry)

	for item := queue.Dequeue(); item != nil; item = queue.Dequeue() {
		if excludedTrie.MatchPrefix([]byte(item.FullPath), func(key []byte, value bool) bool {
			return true
		}) {
			// println("excluded", item.FullPath)
			continue
		}
		parent, _ := item.FullPath.DirAndName()
		if err := stream.Send(&filer_pb.TraverseBfsMetadataResponse{
			Directory: parent,
			Entry:     item.ToProtoEntry(),
		}); err != nil {
			return fmt.Errorf("send traverse bfs metadata response: %v", err)
		}

		if !item.IsDirectory() {
			continue
		}

		if err := fs.iterateDirectory(ctx, item.FullPath, func(entry *filer.Entry) error {
			queue.Enqueue(entry)
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
		lastFileName, listErr = fs.filer.StreamListDirectoryEntries(ctx, dirPath, lastFileName, false, 1024, "", "", "", func(entry *filer.Entry) bool {
			hasEntries = true
			if fnErr := fn(entry); fnErr != nil {
				err = fnErr
				return false
			}
			return true
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
