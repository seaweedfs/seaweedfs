package main

import (
	"flag"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

var (
	volumePath       = flag.String("dir", "/tmp", "data directory to store files")
	volumeCollection = flag.String("collection", "", "the volume collection name")
	volumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func main() {
	flag.Parse()

	var version storage.Version
	vid := storage.VolumeId(*volumeId)
	err := storage.ScanVolumeFile(*volumePath, *volumeCollection, vid,
		storage.NeedleMapInMemory,
		func(superBlock storage.SuperBlock) error {
			version = superBlock.Version()
			return nil
		}, false, func(n *storage.Needle, offset int64) error {
			glog.V(0).Infof("%d,%s%x offset %d size %d cookie %x",
				*volumeId, n.Id, n.Cookie, offset, n.Size, n.Cookie)
			return nil
		})
	if err != nil {
		glog.Fatalf("Reading Volume File [ERROR] %s\n", err)
	}

}
