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

type VolumeFileScanner4SeeDat struct {
	version storage.Version
}

func (scanner *VolumeFileScanner4SeeDat) VisitSuperBlock(superBlock storage.SuperBlock) error {
	scanner.version = superBlock.Version()
	return nil

}
func (scanner *VolumeFileScanner4SeeDat) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4SeeDat) VisitNeedle(n *storage.Needle, offset int64) error {
	glog.V(0).Infof("%d,%s%x offset %d size %d cookie %x", *volumeId, n.Id, n.Cookie, offset, n.Size, n.Cookie)
	return nil
}

func main() {
	flag.Parse()

	vid := storage.VolumeId(*volumeId)

	scanner := &VolumeFileScanner4SeeDat{}
	err := storage.ScanVolumeFile(*volumePath, *volumeCollection, vid, storage.NeedleMapInMemory, scanner)
	if err != nil {
		glog.Fatalf("Reading Volume File [ERROR] %s\n", err)
	}

}
