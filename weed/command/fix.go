package command

import (
	"os"
	"path"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/needle_map"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	fixIncludeDeleted   = cmdFix.Flag.Bool("includeDeleted", false, "include deleted entries in the index file")
)

type VolumeFileScanner4Fix struct {
	version needle.Version
	nm      *needle_map.MemDb
}

func (scanner *VolumeFileScanner4Fix) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil

}
func (scanner *VolumeFileScanner4Fix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4Fix) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	glog.V(2).Infof("key %d offset %d size %d disk_size %d compressed %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed())
	if n.Size.IsValid() {
		pe := scanner.nm.Set(n.Id, types.ToOffset(offset), n.Size)
		glog.V(2).Infof("saved %s %d bytes with error %v", n.Id.String(), n.Size, pe)
	} else {
		if val, found := scanner.nm.Get(n.Id); *fixIncludeDeleted && found && val.Size > 0 {
			pe := scanner.nm.Set(n.Id, val.Offset, -val.Size)
			glog.V(2).Infof("update deleted %s %d bytes with error %v", n.Id.String(), -val.Size, pe)
			return nil
		}
		glog.V(1).Infof("skipping deleted file %s size %d ...", n.Id.String(), n.Size)
		return scanner.nm.Delete(n.Id)
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
	indexFileName := path.Join(util.ResolvePath(*fixVolumePath), baseFileName+".idx")

	nm := needle_map.NewMemDb()
	defer nm.Close()

	vid := needle.VolumeId(*fixVolumeId)
	scanner := &VolumeFileScanner4Fix{
		nm: nm,
	}

	if err := storage.ScanVolumeFile(util.ResolvePath(*fixVolumePath), *fixVolumeCollection, vid, storage.NeedleMapInMemory, scanner); err != nil {
		glog.Fatalf("scan .dat File: %v", err)
		os.Remove(indexFileName)
	}

	if err := nm.SaveToIdx(indexFileName, *fixIncludeDeleted); err != nil {
		glog.Fatalf("save to .idx File: %v", err)
		os.Remove(indexFileName)
	}

	return true
}
