package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
)

var (
	minSize   = flag.Int("minSize", 1024, "min file size")
	maxSize   = flag.Int("maxSize", 10*1024*1024, "max file size")
	fileCount = flag.Int("fileCount", 1, "number of files to write")
	blockSize = flag.Int("blockSizeKB", 4, "write block size")
	toDir     = flag.String("dir", ".", "destination directory")
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {

	flag.Parse()

	block := make([]byte, *blockSize)

	for i := 0; i < *fileCount; i++ {

		f, err := os.Create(fmt.Sprintf("%s/file%05d", *toDir, i))
		check(err)

		fileSize := *minSize + rand.Intn(*maxSize-*minSize)

		for x := 0; x < fileSize; {
			rand.Read(block)
			_, err = f.Write(block)
			check(err)
			x += *blockSize
		}

		err = f.Close()
		check(err)

	}

}
