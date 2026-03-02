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
	portal := flag.String("portal", "", "advertised address for discovery (e.g. 10.0.0.1:3260,1)")
	iqn := flag.String("iqn", "iqn.2024.com.seaweedfs:vol1", "target IQN")
	create := flag.Bool("create", false, "create a new volume file")
	size := flag.String("size", "1G", "volume size (e.g., 1G, 100M) -- used with -create")
	adminAddr := flag.String("admin", "", "HTTP admin listen address (e.g. 127.0.0.1:8080; empty = disabled)")
	adminToken := flag.String("admin-token", "", "optional admin auth token (empty = no auth)")
	replicaData := flag.String("replica-data", "", "replica receiver data listen address (e.g. :9001; empty = disabled)")
	replicaCtrl := flag.String("replica-ctrl", "", "replica receiver ctrl listen address (e.g. :9002; empty = disabled)")
	rebuildListen := flag.String("rebuild-listen", "", "rebuild server listen address (e.g. :9003; empty = disabled)")
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

	// Start replica receiver if configured (replica nodes listen for WAL entries)
	if *replicaData != "" && *replicaCtrl != "" {
		if err := vol.StartReplicaReceiver(*replicaData, *replicaCtrl); err != nil {
			log.Fatalf("start replica receiver: %v", err)
		}
		logger.Printf("replica receiver: data=%s ctrl=%s", *replicaData, *replicaCtrl)
	}

	// Start rebuild server if configured
	if *rebuildListen != "" {
		if err := vol.StartRebuildServer(*rebuildListen); err != nil {
			log.Fatalf("start rebuild server: %v", err)
		}
		logger.Printf("rebuild server: %s", *rebuildListen)
	}

	// Start admin HTTP server if configured
	if *adminAddr != "" {
		adm := newAdminServer(vol, *adminToken, logger)
		ln, err := startAdminServer(*adminAddr, adm)
		if err != nil {
			log.Fatalf("start admin server: %v", err)
		}
		defer ln.Close()
		logger.Printf("admin server: %s", ln.Addr())
	}

	// Create adapter
	adapter := &blockVolAdapter{vol: vol}

	// Create target server
	config := iscsi.DefaultTargetConfig()
	config.TargetName = *iqn
	config.TargetAlias = "SeaweedFS BlockVol"
	ts := iscsi.NewTargetServer(*addr, config, logger)
	if *portal != "" {
		ts.SetPortalAddr(*portal)
	}
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
func (a *blockVolAdapter) SyncCache() error {
	return a.vol.SyncCache()
}
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
