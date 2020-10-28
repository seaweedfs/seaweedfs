package meta_cache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func EnsureVisited(mc *MetaCache, client filer_pb.FilerClient, dirPath util.FullPath) error {

	return mc.visitedBoundary.EnsureVisited(dirPath, func(path util.FullPath) (childDirectories []string, err error) {

		glog.V(4).Infof("ReadDirAllEntries %s ...", path)

		for waitTime := time.Second; waitTime < filer.ReadWaitTime; waitTime += waitTime / 2 {
			err = filer_pb.ReadDirAllEntries(client, dirPath, "", func(pbEntry *filer_pb.Entry, isLast bool) error {
				entry := filer.FromPbEntry(string(dirPath), pbEntry)
				if err := mc.doInsertEntry(context.Background(), entry); err != nil {
					glog.V(0).Infof("read %s: %v", entry.FullPath, err)
					return err
				}
				if entry.IsDirectory() {
					childDirectories = append(childDirectories, entry.Name())
				}
				return nil
			})
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), "transport: ") {
				glog.V(0).Infof("ReadDirAllEntries %s: %v. Retry in %v", path, err, waitTime)
				time.Sleep(waitTime)
				continue
			}
			err = fmt.Errorf("list %s: %v", dirPath, err)
			break
		}
		return
	})
}
