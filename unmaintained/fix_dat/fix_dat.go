package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	fixVolumePath       = flag.String("dir", "/tmp", "data directory to store files")
	fixVolumeCollection = flag.String("collection", "", "the volume collection name")
	fixVolumeId         = flag.Int("volumeId", -1, "a volume id. The volume should already exist in the dir. The volume index file should not exist.")
)

/*
This is to resolve an one-time issue that caused inconsistency with .dat and .idx files.
In this case, the .dat file contains all data, but some deletion caused incorrect offset.
The .idx has all correct offsets.

1. fix the .dat file, a new .dat_fixed file will be generated.
	go run fix_dat.go -volumeId=9 -dir=/Users/chrislu/Downloads
2. move the original .dat and .idx files to some backup folder, and rename .dat_fixed to .dat file
    mv 9.dat_fixed 9.dat
3. fix the .idx file with the "weed fix"
	weed fix -volumeId=9 -dir=/Users/chrislu/Downloads
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
		glog.Fatalf("Read Volume Index %v", err)
	}
	defer indexFile.Close()
	datFileName := path.Join(*fixVolumePath, fileName+".dat")
	datFile, err := os.OpenFile(datFileName, os.O_RDONLY, 0644)
	if err != nil {
		glog.Fatalf("Read Volume Data %v", err)
	}
	datBackend := backend.NewDiskFile(datFile)
	defer datBackend.Close()

	newDatFile, err := os.Create(path.Join(*fixVolumePath, fileName+".dat_fixed"))
	if err != nil {
		glog.Fatalf("Write New Volume Data %v", err)
	}
	defer newDatFile.Close()

	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		glog.Fatalf("Read Volume Data superblock %v", err)
	}
	newDatFile.Write(superBlock.Bytes())

	iterateEntries(datBackend, indexFile, func(n *needle.Needle, offset int64) {
		fmt.Printf("needle id=%v name=%s size=%d dataSize=%d\n", n.Id, string(n.Name), n.Size, n.DataSize)
		_, s, _, e := n.Append(datBackend, superBlock.Version)
		fmt.Printf("size %d error %v\n", s, e)
	})

}

func iterateEntries(datBackend backend.BackendStorageFile, idxFile *os.File, visitNeedle func(n *needle.Needle, offset int64)) {
	// start to read index file
	var readerOffset int64
	bytes := make([]byte, 16)
	count, _ := idxFile.ReadAt(bytes, readerOffset)
	readerOffset += int64(count)

	// start to read dat file
	superBlock, err := super_block.ReadSuperBlock(datBackend)
	if err != nil {
		fmt.Printf("cannot read dat file super block: %v", err)
		return
	}
	offset := int64(superBlock.BlockSize())
	version := superBlock.Version
	n, _, rest, err := needle.ReadNeedleHeader(datBackend, version, offset)
	if err != nil {
		fmt.Printf("cannot read needle header: %v", err)
		return
	}
	fmt.Printf("Needle %+v, rest %d\n", n, rest)
	for n != nil && count > 0 {
		// parse index file entry
		key := util.BytesToUint64(bytes[0:8])
		offsetFromIndex := util.BytesToUint32(bytes[8:12])
		sizeFromIndex := types.BytesToSize(bytes[12:16])
		count, _ = idxFile.ReadAt(bytes, readerOffset)
		readerOffset += int64(count)

		if offsetFromIndex != 0 && offset != int64(offsetFromIndex)*8 {
			//t := offset
			offset = int64(offsetFromIndex) * 8
			//fmt.Printf("Offset change %d => %d\n", t, offset)
		}

		fmt.Printf("key: %d offsetFromIndex %d n.Size %d sizeFromIndex:%d\n", key, offsetFromIndex, n.Size, sizeFromIndex)

		rest = needle.NeedleBodyLength(sizeFromIndex, version)

		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered in f", r)
				}
			}()
			if _, err = n.ReadNeedleBody(datBackend, version, offset+int64(types.NeedleHeaderSize), rest); err != nil {
				fmt.Printf("cannot read needle body: offset %d body %d %v\n", offset, rest, err)
			}
		}()

		if n.Size <= types.Size(n.DataSize) {
			continue
		}
		visitNeedle(n, offset)

		offset += types.NeedleHeaderSize + rest
		//fmt.Printf("==> new entry offset %d\n", offset)
		if n, _, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				return
			}

			fmt.Printf("cannot read needle header: %v\n", err)
			return
		}
		//fmt.Printf("new entry needle size:%d rest:%d\n", n.Size, rest)
	}

}
