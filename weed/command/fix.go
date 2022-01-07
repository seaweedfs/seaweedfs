package command

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

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
	fixVolumeId         = cmdFix.Flag.Int("volumeId", 0, "an optional volume id.")
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
		glog.V(2).Infof("saved %d with error %v", n.Size, pe)
	} else {
		glog.V(2).Infof("skipping deleted file ...")
		return scanner.nm.Delete(n.Id)
	}
	return nil
}

func runFix(cmd *Command, args []string) bool {

	dir := util.ResolvePath(*fixVolumePath)
	if *fixVolumeId != 0 {
		doFixOneVolume(dir, *fixVolumeCollection, needle.VolumeId(*fixVolumeId))
		return true
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
		return false
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".dat") {
			continue
		}
		if *fixVolumeCollection != "" {
			if !strings.HasPrefix(file.Name(), *fixVolumeCollection+"_") {
				continue
			}
		}
		baseFileName := file.Name()[:len(file.Name())-4]
		collection, volumeIdStr := "", baseFileName
		if sepIndex := strings.LastIndex(baseFileName, "_"); sepIndex > 0 {
			collection = baseFileName[:sepIndex]
			volumeIdStr = baseFileName[sepIndex+1:]
		}
		volumeId, parseErr := strconv.ParseInt(volumeIdStr, 10, 64)
		if parseErr != nil {
			fmt.Printf("Failed to parse volume id from %s: %v\n", baseFileName, parseErr)
			return false
		}
		doFixOneVolume(dir, collection, needle.VolumeId(volumeId))
	}

	return true
}

func doFixOneVolume(dir, collection string, volumeId needle.VolumeId) {

	baseFileName := strconv.Itoa(int(volumeId))
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}

	indexFileName := path.Join(dir, baseFileName+".idx")

	nm := needle_map.NewMemDb()
	defer nm.Close()

	vid := needle.VolumeId(*fixVolumeId)
	scanner := &VolumeFileScanner4Fix{
		nm: nm,
	}

	if err := storage.ScanVolumeFile(dir, collection, vid, storage.NeedleMapInMemory, scanner); err != nil {
		glog.Fatalf("scan .dat File: %v", err)
		os.Remove(indexFileName)
	}

	if err := nm.SaveToIdx(indexFileName); err != nil {
		glog.Fatalf("save to .idx File: %v", err)
		os.Remove(indexFileName)
	}
}
