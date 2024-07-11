package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	fixVolumePath       = flag.String("dir", "/tmp", "data directory to store files")
	fixVolumeCollection = flag.String("collection", "", "the volume collection name")
	fixVolumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

/*
This is to see content in .idx files.

	see_idx -v=4 -volumeId=9 -dir=/Users/chrislu/Downloads
*/
func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	fileName := strconv.Itoa(*fixVolumeId)
	if *fixVolumeCollection != "" {
		fileName = *fixVolumeCollection + "_" + fileName
	}
	indexFile, err := os.OpenFile(path.Join(*fixVolumePath, fileName+".idx"), os.O_RDONLY, 0644)
	if err != nil {
		glog.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	idx.WalkIndexFile(indexFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		fmt.Printf("key:%v offset:%v size:%v(%v)\n", key, offset, size, util.BytesToHumanReadable(uint64(size)))
		return nil
	})

}
