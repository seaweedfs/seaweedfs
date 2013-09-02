package main

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"os"
	"path"
	"strconv"
)

func init() {
	cmdFix.Run = runFix // break init cycle
	cmdFix.IsDebug = cmdFix.Flag.Bool("debug", false, "enable debug mode")
}

var cmdFix = &Command{
	UsageLine: "fix -dir=/tmp -volumeId=234",
	Short:     "run weed tool fix on index file if corrupted",
	Long: `Fix runs the WeedFS fix command to re-create the index .idx file.

  `,
}

var (
	fixVolumePath = cmdFix.Flag.String("dir", "/tmp", "data directory to store files")
	fixVolumeId   = cmdFix.Flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func runFix(cmd *Command, args []string) bool {

	if *fixVolumeId == -1 {
		return false
	}

	fileName := strconv.Itoa(*fixVolumeId)
	indexFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".idx"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		glog.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	nm := storage.NewNeedleMap(indexFile)
	defer nm.Close()

	vid := storage.VolumeId(*fixVolumeId)
	err = storage.ScanVolumeFile(*fixVolumePath, vid, func(superBlock storage.SuperBlock) error {
		return nil
	}, func(n *storage.Needle, offset uint32) error {
		debug("key", n.Id, "offset", offset, "size", n.Size, "disk_size", n.DiskSize(), "gzip", n.IsGzipped())
		if n.Size > 0 {
			count, pe := nm.Put(n.Id, offset/storage.NeedlePaddingSize, n.Size)
			debug("saved", count, "with error", pe)
		} else {
			debug("skipping deleted file ...")
			return nm.Delete(n.Id)
		}
		return nil
	})
	if err != nil {
		glog.Fatalf("Export Volume File [ERROR] %s\n", err)
	}

	return true
}
