// iscsi-target is a standalone iSCSI target backed by a BlockVol file.
// Usage:
//
//	iscsi-target -vol /path/to/volume.blk -addr :3260 -iqn iqn.2024.com.seaweedfs:vol1
//	iscsi-target -create -size 1G -vol /path/to/volume.blk -addr :3260
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

func main() {
	volPath := flag.String("vol", "", "path to BlockVol file")
	addr := flag.String("addr", ":3260", "listen address")
	iqn := flag.String("iqn", "iqn.2024.com.seaweedfs:vol1", "target IQN")
	create := flag.Bool("create", false, "create a new volume file")
	size := flag.String("size", "1G", "volume size (e.g., 1G, 100M) — used with -create")
	flag.Parse()

	if *volPath == "" {
		fmt.Fprintln(os.Stderr, "error: -vol is required")
		flag.Usage()
		os.Exit(1)
	}

	logger := log.New(os.Stdout, "[iscsi] ", log.LstdFlags)

	var vol *blockvol.BlockVol
	var err error

	if *create {
		volSize, parseErr := parseSize(*size)
		if parseErr != nil {
			log.Fatalf("invalid size %q: %v", *size, parseErr)
		}
		vol, err = blockvol.CreateBlockVol(*volPath, blockvol.CreateOptions{
			VolumeSize: volSize,
			BlockSize:  4096,
			WALSize:    64 * 1024 * 1024,
		})
		if err != nil {
			log.Fatalf("create volume: %v", err)
		}
		logger.Printf("created volume: %s (%s)", *volPath, *size)
	} else {
		vol, err = blockvol.OpenBlockVol(*volPath)
		if err != nil {
			log.Fatalf("open volume: %v", err)
		}
		logger.Printf("opened volume: %s", *volPath)
	}
	defer vol.Close()

	info := vol.Info()
	logger.Printf("volume: %d bytes, block=%d, healthy=%v",
		info.VolumeSize, info.BlockSize, info.Healthy)

	// Create adapter
	adapter := &blockVolAdapter{vol: vol}

	// Create target server
	config := iscsi.DefaultTargetConfig()
	config.TargetName = *iqn
	config.TargetAlias = "SeaweedFS BlockVol"
	ts := iscsi.NewTargetServer(*addr, config, logger)
	ts.AddVolume(*iqn, adapter)

	// Graceful shutdown on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("received %v, shutting down...", sig)
		ts.Close()
	}()

	logger.Printf("starting iSCSI target: %s on %s", *iqn, *addr)
	if err := ts.ListenAndServe(); err != nil {
		log.Fatalf("target server: %v", err)
	}
	logger.Println("target stopped")
}

// blockVolAdapter wraps BlockVol to implement iscsi.BlockDevice.
type blockVolAdapter struct {
	vol *blockvol.BlockVol
}

func (a *blockVolAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return a.vol.ReadLBA(lba, length)
}
func (a *blockVolAdapter) WriteAt(lba uint64, data []byte) error {
	return a.vol.WriteLBA(lba, data)
}
func (a *blockVolAdapter) Trim(lba uint64, length uint32) error {
	return a.vol.Trim(lba, length)
}
func (a *blockVolAdapter) SyncCache() error  { return a.vol.SyncCache() }
func (a *blockVolAdapter) BlockSize() uint32  { return a.vol.Info().BlockSize }
func (a *blockVolAdapter) VolumeSize() uint64 { return a.vol.Info().VolumeSize }
func (a *blockVolAdapter) IsHealthy() bool    { return a.vol.Info().Healthy }

func parseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := uint64(1)
	suffix := s[len(s)-1]
	switch suffix {
	case 'K', 'k':
		multiplier = 1024
		s = s[:len(s)-1]
	case 'M', 'm':
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * multiplier, nil
}
