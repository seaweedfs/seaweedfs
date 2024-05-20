package command

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	cmdFix.Run = runFix // break init cycle
}

var cmdFix = &Command{
	UsageLine: "fix [-volumeId=234] [-collection=bigData] /tmp",
	Short:     "run weed tool fix on files or whole folders to recreate index file(s) if corrupted",
	Long: `Fix runs the SeaweedFS fix command on dat files or whole folders to re-create the index .idx file.
  You Need to stop the volume server when running this command.
`,
}

var (
	fixVolumeCollection = cmdFix.Flag.String("collection", "", "an optional volume collection name, if specified only it will be processed")
	fixVolumeId         = cmdFix.Flag.Int64("volumeId", 0, "an optional volume id, if not 0 (default) only it will be processed")
	fixIncludeDeleted   = cmdFix.Flag.Bool("includeDeleted", true, "include deleted entries in the index file")
	fixIgnoreError      = cmdFix.Flag.Bool("ignoreError", false, "an optional, if true will be processed despite errors")
)

type VolumeFileScanner4Fix struct {
	version        needle.Version
	nm             *needle_map.MemDb
	nmDeleted      *needle_map.MemDb
	includeDeleted bool
}

func (scanner *VolumeFileScanner4Fix) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil
}

func (scanner *VolumeFileScanner4Fix) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4Fix) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	glog.V(2).Infof("key %v offset %d size %d disk_size %d compressed %v", n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed())
	if n.Size.IsValid() {
		if pe := scanner.nm.Set(n.Id, types.ToOffset(offset), n.Size); pe != nil {
			return fmt.Errorf("saved %d with error %v", n.Size, pe)
		}
	} else {
		if scanner.includeDeleted {
			if pe := scanner.nmDeleted.Set(n.Id, types.ToOffset(offset), types.TombstoneFileSize); pe != nil {
				return fmt.Errorf("saved deleted %d with error %v", n.Size, pe)
			}
		} else {
			glog.V(2).Infof("skipping deleted file ...")
			return scanner.nm.Delete(n.Id)
		}
	}
	return nil
}

func runFix(cmd *Command, args []string) bool {
	for _, arg := range args {
		basePath, f := path.Split(util.ResolvePath(arg))
		if util.FolderExists(arg) {
			basePath = arg
			f = ""
		}

		files := []fs.DirEntry{}
		if f == "" {
			fileInfo, err := os.ReadDir(basePath)
			if err != nil {
				fmt.Println(err)
				return false
			}
			files = fileInfo
		} else {
			fileInfo, err := os.Stat(arg)
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
			doFixOneVolume(basePath, baseFileName, collection, volumeId, *fixIncludeDeleted)
		}
	}
	return true
}

func SaveToIdx(scaner *VolumeFileScanner4Fix, idxName string) (ret error) {
	idxFile, err := os.OpenFile(idxName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	defer func() {
		idxFile.Close()
	}()

	return scaner.nm.AscendingVisit(func(value needle_map.NeedleValue) error {
		_, err := idxFile.Write(value.ToBytes())
		if scaner.includeDeleted && err == nil {
			if deleted, ok := scaner.nmDeleted.Get(value.Key); ok {
				_, err = idxFile.Write(deleted.ToBytes())
			}
		}
		return err
	})
}

func doFixOneVolume(basepath string, baseFileName string, collection string, volumeId int64, fixIncludeDeleted bool) {
	indexFileName := path.Join(basepath, baseFileName+".idx")

	nm := needle_map.NewMemDb()
	nmDeleted := needle_map.NewMemDb()
	defer nm.Close()
	defer nmDeleted.Close()

	vid := needle.VolumeId(volumeId)
	scanner := &VolumeFileScanner4Fix{
		nm:             nm,
		nmDeleted:      nmDeleted,
		includeDeleted: fixIncludeDeleted,
	}

	if err := storage.ScanVolumeFile(basepath, collection, vid, storage.NeedleMapInMemory, scanner); err != nil {
		err := fmt.Errorf("scan .dat File: %v", err)
		if *fixIgnoreError {
			glog.Error(err)
		} else {
			glog.Fatal(err)
		}
	}

	if err := SaveToIdx(scanner, indexFileName); err != nil {
		err := fmt.Errorf("save to .idx File: %v", err)
		if *fixIgnoreError {
			glog.Error(err)
		} else {
			os.Remove(indexFileName)
			glog.Fatal(err)
		}
	}
}
