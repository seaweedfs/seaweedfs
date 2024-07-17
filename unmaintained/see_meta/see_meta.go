package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	metaFile = flag.String("meta", "", "meta file generated via fs.meta.save")
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	dst, err := os.OpenFile(*metaFile, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open %s: %v", *metaFile, err)
	}
	defer dst.Close()

	err = walkMetaFile(dst)
	if err != nil {
		log.Fatalf("failed to visit %s: %v", *metaFile, err)
	}

}

func walkMetaFile(dst *os.File) error {

	sizeBuf := make([]byte, 4)

	for {
		if n, err := dst.Read(sizeBuf); n != 4 {
			if err == io.EOF {
				return nil
			}
			return err
		}

		size := util.BytesToUint32(sizeBuf)

		data := make([]byte, int(size))

		if n, err := dst.Read(data); n != len(data) {
			return err
		}

		fullEntry := &filer_pb.FullEntry{}
		if err := proto.Unmarshal(data, fullEntry); err != nil {
			return err
		}

		fmt.Fprintf(os.Stdout, "file %s %v\n", util.FullPath(fullEntry.Dir).Child(fullEntry.Entry.Name), fullEntry.Entry.Attributes.String())
		for i, chunk := range fullEntry.Entry.GetChunks() {
			fmt.Fprintf(os.Stdout, "  chunk: %d %v %d,%x%08x\n", i+1, chunk, chunk.Fid.VolumeId, chunk.Fid.FileKey, chunk.Fid.Cookie)
		}

	}

}
