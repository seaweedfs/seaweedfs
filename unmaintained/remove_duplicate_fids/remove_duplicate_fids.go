package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	volumePath       = flag.String("dir", "/tmp", "data directory to store files")
	volumeCollection = flag.String("collection", "", "the volume collection name")
	volumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

func Checksum(n *needle.Needle) string {
	return fmt.Sprintf("%s%x", n.Id, n.Cookie)
}

type VolumeFileScanner4SeeDat struct {
	version needle.Version
	block   super_block.SuperBlock

	dir        string
	hashes     map[string]bool
	dat        *os.File
	datBackend backend.BackendStorageFile
}

func (scanner *VolumeFileScanner4SeeDat) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	scanner.block = superBlock
	return nil

}
func (scanner *VolumeFileScanner4SeeDat) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4SeeDat) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {

	if scanner.datBackend == nil {
		newFileName := filepath.Join(*volumePath, "dat_fixed")
		newDatFile, err := os.Create(newFileName)
		if err != nil {
			glog.Fatalf("Write New Volume Data %v", err)
		}
		scanner.datBackend = backend.NewDiskFile(newDatFile)
		scanner.datBackend.WriteAt(scanner.block.Bytes(), 0)
	}

	checksum := Checksum(n)

	if scanner.hashes[checksum] {
		glog.V(0).Infof("duplicate checksum:%s fid:%d,%s%x @ offset:%d", checksum, *volumeId, n.Id, n.Cookie, offset)
		return nil
	}
	scanner.hashes[checksum] = true

	_, s, _, e := n.Append(scanner.datBackend, scanner.version)
	fmt.Printf("size %d error %v\n", s, e)

	return nil
}

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	vid := needle.VolumeId(*volumeId)

	outpath, _ := filepath.Abs(filepath.Dir(os.Args[0]))

	scanner := &VolumeFileScanner4SeeDat{
		dir:    filepath.Join(outpath, "out"),
		hashes: map[string]bool{},
	}

	if _, err := os.Stat(scanner.dir); err != nil {
		if err := os.MkdirAll(scanner.dir, os.ModePerm); err != nil {
			glog.Fatalf("could not create output dir : %s", err)
		}
	}

	err := storage.ScanVolumeFile(*volumePath, *volumeCollection, vid, storage.NeedleMapInMemory, scanner)
	if err != nil {
		glog.Fatalf("Reading Volume File [ERROR] %s\n", err)
	}

}
