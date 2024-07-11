package main

import (
	"flag"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	volumePath       = flag.String("dir", "/tmp", "data directory to store files")
	volumeCollection = flag.String("collection", "", "the volume collection name")
	volumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

type VolumeFileScanner4SeeDat struct {
	version needle.Version
}

func (scanner *VolumeFileScanner4SeeDat) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil

}
func (scanner *VolumeFileScanner4SeeDat) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4SeeDat) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	t := time.Unix(int64(n.AppendAtNs)/int64(time.Second), int64(n.AppendAtNs)%int64(time.Second))
	glog.V(0).Infof("%d,%s%08x offset %d size %d(%s) cookie %08x appendedAt %v name %s",
		*volumeId, n.Id, n.Cookie, offset, n.Size, util.BytesToHumanReadable(uint64(n.Size)), n.Cookie, t, n.Name)
	return nil
}

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	vid := needle.VolumeId(*volumeId)

	scanner := &VolumeFileScanner4SeeDat{}
	err := storage.ScanVolumeFile(*volumePath, *volumeCollection, vid, storage.NeedleMapInMemory, scanner)
	if err != nil {
		glog.Fatalf("Reading Volume File [ERROR] %s\n", err)
	}
}
