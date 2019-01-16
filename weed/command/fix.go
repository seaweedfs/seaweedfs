package command

import (
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
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

type VolumeFileScanner4Fix struct {
	version storage.Version
	nm      *storage.NeedleMap
}

func (scanner *VolumeFileScanner4Fix) VisitSuperBlock(superBlock storage.SuperBlock) error {
	scanner.version = superBlock.Version()
	return nil

}
func (scanner *VolumeFileScanner4Fix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4Fix) VisitNeedle(n *storage.Needle, offset int64) error {
	glog.V(2).Infof("key %d offset %d size %d disk_size %d gzip %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsGzipped())
	if n.Size > 0 {
		pe := scanner.nm.Put(n.Id, types.Offset(offset/types.NeedlePaddingSize), n.Size)
		glog.V(2).Infof("saved %d with error %v", n.Size, pe)
	} else {
		glog.V(2).Infof("skipping deleted file ...")
		return scanner.nm.Delete(n.Id, types.Offset(offset/types.NeedlePaddingSize))
	}
	return nil
}

func runFix(cmd *Command, args []string) bool {

	if *fixVolumeId == -1 {
		return false
	}

	baseFileName := strconv.Itoa(*fixVolumeId)
	if *fixVolumeCollection != "" {
		baseFileName = *fixVolumeCollection + "_" + baseFileName
	}
	indexFileName := path.Join(*fixVolumePath, baseFileName+".idx")
	indexFile, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		glog.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	nm := storage.NewBtreeNeedleMap(indexFile)
	defer nm.Close()

	vid := storage.VolumeId(*fixVolumeId)
	scanner := &VolumeFileScanner4Fix{
		nm: nm,
	}

	err = storage.ScanVolumeFile(*fixVolumePath, *fixVolumeCollection, vid, storage.NeedleMapInMemory, scanner)
	if err != nil {
		glog.Fatalf("Export Volume File [ERROR] %s\n", err)
		os.Remove(indexFileName)
	}

	return true
}
