package main

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file.idx>")
		os.Exit(1)
	}

	fileName := os.Args[1]
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	fmt.Println("Key,Offset,Size")
	err = idx.WalkIndexFile(file, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		fmt.Printf("%d,%d,%d\n", key, offset.ToActualOffset(), size)
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking index: %v\n", err)
		os.Exit(1)
	}
}
