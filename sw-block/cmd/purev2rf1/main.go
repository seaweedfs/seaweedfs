package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/purev2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "bootstrap":
		if err := runBootstrap(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "status":
		if err := runStatus(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runBootstrap(args []string) error {
	fs := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	path := fs.String("path", "", "block volume path")
	sizeBytes := fs.Uint64("size-bytes", 1*1024*1024, "logical volume size in bytes")
	blockSize := fs.Uint("block-size", 4096, "block size in bytes")
	walSize := fs.Uint64("wal-size", 256*1024, "wal size in bytes")
	epoch := fs.Uint64("epoch", 1, "assignment epoch")
	leaseMs := fs.Int("lease-ms", 30000, "lease ttl in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("bootstrap: --path is required")
	}

	rt := purev2.New(purev2.Config{})
	defer rt.Close()

	opts := blockvol.CreateOptions{
		VolumeSize: *sizeBytes,
		BlockSize:  uint32(*blockSize),
		WALSize:    *walSize,
	}
	if err := rt.BootstrapPrimary(*path, opts, *epoch, time.Duration(*leaseMs)*time.Millisecond); err != nil {
		return err
	}
	snap, err := rt.Snapshot(*path)
	if err != nil {
		return err
	}
	return printJSON(snap)
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	path := fs.String("path", "", "block volume path")
	epoch := fs.Uint64("epoch", 0, "optional epoch to rebuild core projection")
	leaseMs := fs.Int("lease-ms", 30000, "lease ttl in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" {
		return fmt.Errorf("status: --path is required")
	}

	rt := purev2.New(purev2.Config{})
	defer rt.Close()

	if err := rt.OpenVolume(*path); err != nil {
		return err
	}
	if *epoch > 0 {
		if err := rt.ApplyPrimaryAssignment(*path, *epoch, time.Duration(*leaseMs)*time.Millisecond); err != nil {
			return err
		}
	}
	snap, err := rt.Snapshot(*path)
	if err != nil {
		return err
	}
	return printJSON(snap)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  purev2rf1 bootstrap --path <file> [--size-bytes N --block-size N --wal-size N --epoch N]")
	fmt.Fprintln(os.Stderr, "  purev2rf1 status --path <file> [--epoch N]")
}
