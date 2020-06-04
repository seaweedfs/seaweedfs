package main

import (
	"flag"
	"fmt"
	"runtime"
	"syscall"
)

var (
	dir = flag.String("dir", ".", "the directory which uses a disk")
)

func main() {
	flag.Parse()

	fillInDiskStatus(*dir)

	fmt.Printf("OS: %v\n", runtime.GOOS)
	fmt.Printf("Arch: %v\n", runtime.GOARCH)

}

func fillInDiskStatus(dir string) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(dir, &fs)
	if err != nil {
		fmt.Printf("failed to statfs on %s: %v\n", dir, err)
		return
	}
	fmt.Printf("statfs: %+v\n", fs)
	fmt.Println()

	total := fs.Blocks * uint64(fs.Bsize)
	free := fs.Bfree * uint64(fs.Bsize)
	fmt.Printf("Total: %d blocks x %d block size = %d bytes\n", fs.Blocks, uint64(fs.Bsize), total)
	fmt.Printf("Free : %d blocks x %d block size = %d bytes\n", fs.Bfree, uint64(fs.Bsize), free)
	fmt.Printf("Used : %d blocks x %d block size = %d bytes\n", fs.Blocks-fs.Bfree, uint64(fs.Bsize), total-free)
	fmt.Printf("Free Percentage : %.2f%%\n", float32((float64(free)/float64(total))*100))
	fmt.Printf("Used Percentage : %.2f%%\n", float32((float64(total-free)/float64(total))*100))
	return
}
