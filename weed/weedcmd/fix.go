package weedcmd

import (
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func init() {
	cmdFix.Run = runFix // break init cycle
}

var cmdFix = &Command{
	UsageLine: "fix -dir=/tmp -volumeId=234",
	Short:     "run weed tool fix on index file if corrupted",
	Long: `Fix runs the SeaweedFS fix command to re-create the index .idx file.

  `,
}

var (
	fixVolumePath       = cmdFix.Flag.String("dir", ".", "data directory to store files")
	fixVolumeCollection = cmdFix.Flag.String("collection", "", "the volume collection name")
	fixVolumeId         = cmdFix.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func runFix(cmd *Command, args []string) bool {

	if *fixVolumeId == -1 {
		return false
	}

	fileName := strconv.Itoa(*fixVolumeId)
	if *fixVolumeCollection != "" {
		fileName = *fixVolumeCollection + "_" + fileName
	}
	indexFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".idx"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	nm := storage.NewNeedleMap(indexFile)
	defer nm.Close()

	vid := storage.VolumeId(*fixVolumeId)
	err = storage.ScanVolumeFile(*fixVolumePath, *fixVolumeCollection, vid,
		storage.NeedleMapInMemory,
		func(superBlock storage.SuperBlock) error {
			return nil
		}, false, func(n *storage.Needle, offset int64) error {
			glog.V(2).Infof("key %d offset %d size %d disk_size %d gzip %v", n.Id, offset, n.Size, n.DiskSize(), n.IsGzipped())
			if n.Size > 0 {
				pe := nm.Put(n.Id, uint32(offset/storage.NeedlePaddingSize), n.Size)
				glog.V(2).Infof("saved %d with error %v", n.Size, pe)
			} else {
				glog.V(2).Infof("skipping deleted file ...")
				return nm.Delete(n.Id)
			}
			return nil
		})
	if err != nil {
		glog.Fatalf("Export Volume File [ERROR] %s\n", err)
	}

	return true
}
