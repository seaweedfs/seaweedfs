package meta_cache

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) error {

	return mc.visitedBoundary.EnsureVisited(dirPath, func(path util.FullPath) (childDirectories []string, err error) {

		glog.V(4).Infof("ReadDirAllEntries %s ...", path)

		util.Retry("ReadDirAllEntries", func() error {
			err = filer_pb.ReadDirAllEntries(client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				entry := filer.FromPbEntry(string(path), pbEntry)
				if IsHiddenSystemEntry(string(path), entry.Name()) {
					return nil
				}
				if err := mc.doInsertEntry(context.Background(), entry); err != nil {
					glog.V(0).Infof("read %s: %v", entry.FullPath, err)
					return err
				}
				if entry.IsDirectory() {
					childDirectories = append(childDirectories, entry.Name())
				}
				return nil
			})
			return err
		})

		if err != nil {
			err = fmt.Errorf("list %s: %v", path, err)
		}

		return
	})
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && name == "topics"
}
