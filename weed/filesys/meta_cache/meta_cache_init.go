package meta_cache

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func InitMetaCache(mc *MetaCache, client filer_pb.FilerClient, path string) error {
	filer_pb.TraverseBfs(client, util.FullPath(path), func(parentPath util.FullPath, pbEntry *filer_pb.Entry) {
		entry := filer2.FromPbEntry(string(parentPath), pbEntry)
		if err := mc.InsertEntry(context.Background(), entry); err != nil {
			glog.V(0).Infof("read %s: %v", entry.FullPath, err)
		}
	})
	return nil
}
