package meta_cache

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func InitMetaCache(mc *MetaCache, client filer_pb.FilerClient, path string) error {
	return nil
	glog.V(0).Infof("synchronizing meta data ...")
	filer_pb.TraverseBfs(client, util.FullPath(path), func(parentPath util.FullPath, pbEntry *filer_pb.Entry) {
		entry := filer2.FromPbEntry(string(parentPath), pbEntry)
		if err := mc.InsertEntry(context.Background(), entry); err != nil {
			glog.V(0).Infof("read %s: %v", entry.FullPath, err)
		}
	})
	return nil
}

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) {

	mc.visitedBoundary.EnsureVisited(dirPath, func(path util.FullPath) (childDirectories []string, err error) {

		glog.V(2).Infof("ReadDirAllEntries %s ...", path)

		err = filer_pb.ReadDirAllEntries(client, dirPath, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
			entry := filer2.FromPbEntry(string(dirPath), pbEntry)
			if err := mc.InsertEntry(context.Background(), entry); err != nil {
				glog.V(0).Infof("read %s: %v", entry.FullPath, err)
				return err
			}
			if entry.IsDirectory() {
				childDirectories = append(childDirectories, entry.Name())
			}
			return nil
		})
		if err != nil {
			err = fmt.Errorf("list %s: %v", dirPath, err)
		}
		return
	})
}
