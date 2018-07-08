package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

var (
	indexFileName = flag.String("file", "", ".idx file to analyze")
)

func main() {
	flag.Parse()
	indexFile, err := os.OpenFile(*indexFileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	storage.WalkIndexFile(indexFile, func(key types.NeedleId, offset types.Offset, size uint32) error {
		fmt.Printf("key %d, offset %d, size %d, nextOffset %d\n", key, offset*8, size, int64(offset)*types.NeedlePaddingSize+int64(size))
		return nil
	})
}
