package meta_cache

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath, entryChan chan *filer.Entry) error {

	currentPath := dirPath

	for {

		// the directory children are already cached
		// so no need for this and upper directories
		if mc.isCachedFn(currentPath) {
			return nil
		}

		if entryChan != nil && dirPath == currentPath {
			if err := doEnsureVisited(mc, client, currentPath, entryChan); err != nil {
				return err
			}
		} else {
			if err := doEnsureVisited(mc, client, currentPath, nil); err != nil {
				return err
			}
		}

		// continue to parent directory
		if currentPath != "/" {
			parent, _ := currentPath.DirAndName()
			currentPath = util.FullPath(parent)
		} else {
			break
		}
	}

	return nil

}

func doEnsureVisited(mc *MetaCache, client filer_pb.FilerClient, path util.FullPath, entryChan chan *filer.Entry) error {

	glog.V(4).Infof("ReadDirAllEntries %s ...", path)

	err := util.Retry("ReadDirAllEntries", func() error {
		return filer_pb.ReadDirAllEntries(client, path, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
			entry := filer.FromPbEntry(string(path), pbEntry)
			if IsHiddenSystemEntry(string(path), entry.Name()) {
				return nil
			}
			if err := mc.doInsertEntry(context.Background(), entry); err != nil {
				glog.V(0).Infof("read %s: %v", entry.FullPath, err)
				return err
			}
			if entryChan != nil {
				entryChan <- entry
			}
			return nil
		})
	})

	if err != nil {
		err = fmt.Errorf("list %s: %v", path, err)
	}
	mc.markCachedFn(path)
	return err
}

func IsHiddenSystemEntry(dir, name string) bool {
	return dir == "/" && (name == "topics" || name == "etc")
}
