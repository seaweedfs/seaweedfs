package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage"
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

	storage.WalkIndexFile(indexFile, func(key uint64, offset, size uint32) error {
		fmt.Printf("key %d, offset %d, size %d, nextOffset %d\n", key, offset*8, size, offset*8+size)
		return nil
	})
}
