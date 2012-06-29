package main

import (
	"pkg/storage"
	"flag"
	"log"
	"os"
	"path"
	"strconv"
)

var (
	dir      = flag.String("dir", "/tmp", "data directory to store files")
	volumeId = flag.Int("volumeId", -1, "a non-negative volume id. The volume should already exist in the dir. The volume index file should not exist.")
	IsDebug  = flag.Bool("debug", false, "enable debug mode")

	store *storage.Store
)

func main() {
	flag.Parse()

	if *volumeId == -1 {
		flag.Usage()
		return
	}

	fileName := strconv.Itoa(*volumeId)
	dataFile, e := os.OpenFile(path.Join(*dir, fileName+".dat"), os.O_RDONLY, 0644)
	if e != nil {
		log.Fatalf("Read Volume [ERROR] %s\n", e)
	}
	defer dataFile.Close()
	indexFile, ie := os.OpenFile(path.Join(*dir, fileName+".idx"), os.O_WRONLY|os.O_CREATE, 0644)
	if ie != nil {
		log.Fatalf("Create Volume Index [ERROR] %s\n", ie)
	}
	defer indexFile.Close()

	//skip the volume super block
	dataFile.Seek(storage.SuperBlockSize, 0)

	n, length := storage.ReadNeedle(dataFile)
	nm := storage.NewNeedleMap(indexFile)
	offset := uint32(storage.SuperBlockSize)
	for n != nil {
		if *IsDebug {
			log.Println("key", n.Key, "volume offset", offset, "data_size", n.Size, "length", length)
		}
		if n.Size > 0 {
			count, pe := nm.Put(n.Key, offset/8, n.Size)
			if *IsDebug {
				log.Println("saved", count, "with error", pe)
			}
		}
		offset += length
		n, length = storage.ReadNeedle(dataFile)
	}
}
