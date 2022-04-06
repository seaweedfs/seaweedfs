package command

import (
	"fmt"
	"io/fs"
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
	UsageLine: "fix [-volumeId=234] [-collection=bigData] /tmp",
	Short:     "run weed tool fix on files or whole folders to recreate index file(s) if corrupted",
	Long: `Fix runs the SeaweedFS fix command on dat files or whole folders to re-create the index .idx file.
  `,
}

var (
	fixVolumeCollection = cmdFix.Flag.String("collection", "", "an optional volume collection name, if specified only it will be processed")
	fixVolumeId         = cmdFix.Flag.Int64("volumeId", 0, "an optional volume id, if not 0 (default) only it will be processed")
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
	for _, arg := range args {
		basePath, f := path.Split(util.ResolvePath(arg))

		files := []fs.DirEntry{}
		if f == "" {
			fileInfo, err := os.ReadDir(basePath)
			if err != nil {
				fmt.Println(err)
				return false
			}
			files = fileInfo
		} else {
			fileInfo, err := os.Stat(basePath + f)
			if err != nil {
				fmt.Println(err)
				return false
			}
			files = []fs.DirEntry{fs.FileInfoToDirEntry(fileInfo)}
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
			if *fixVolumeId != 0 && *fixVolumeId != volumeId {
				continue
			}
			doFixOneVolume(basePath, baseFileName, collection, volumeId)
		}
	}
	return true
}

func doFixOneVolume(basepath string, baseFileName string, collection string, volumeId int64) {

	indexFileName := path.Join(basepath, baseFileName+".idx")

	nm := needle_map.NewMemDb()
	defer nm.Close()

	vid := needle.VolumeId(volumeId)
	scanner := &VolumeFileScanner4Fix{
		nm: nm,
	}

	if err := storage.ScanVolumeFile(basepath, collection, vid, storage.NeedleMapInMemory, scanner); err != nil {
		glog.Fatalf("scan .dat File: %v", err)
		os.Remove(indexFileName)
	}

	if err := nm.SaveToIdx(indexFileName); err != nil {
		glog.Fatalf("save to .idx File: %v", err)
		os.Remove(indexFileName)
	}
}
