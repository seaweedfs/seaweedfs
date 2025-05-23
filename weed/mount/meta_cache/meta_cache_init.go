package meta_cache

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) error {

	currentPath := dirPath

	for {

		// the directory children are already cached
		// so no need for this and upper directories
		if mc.isCachedFn(currentPath) {
			return nil
		}

		if err := doEnsureVisited(mc, client, currentPath); err != nil {
			return err
		}

		// continue to parent directory
		if currentPath != mc.root {
			parent, _ := currentPath.DirAndName()
			currentPath = util.FullPath(parent)
		} else {
			break
		}
	}

	return nil

}

func doEnsureVisited(mc *MetaCache, client filer_pb.FilerClient, path util.FullPath) error {

	glog.V(4).Infof("ReadDirAllEntries %s ...", path)

	err := util.Retry("ReadDirAllEntries", func() error {
		return filer_pb.ReadDirAllEntries(context.Background(), client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
			entry := filer.FromPbEntry(string(path), pbEntry)
			if IsHiddenSystemEntry(string(path), entry.Name()) {
				return nil
			}
			if err := mc.doInsertEntry(context.Background(), entry); err != nil {
				glog.V(0).Infof("read %s: %v", entry.FullPath, err)
				return err
			}
			return nil
		})
	})

	if err != nil {
		err = fmt.Errorf("list %s: %v", path, err)
	} else {
		mc.markCachedFn(path)
	}
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
