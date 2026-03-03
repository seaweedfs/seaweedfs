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
	"sync/atomic"
	"syscall"
	"time"

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
	tpgID := flag.Int("tpg-id", 1, "target port group ID for ALUA (1-65535)")
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
	if *tpgID < 1 || *tpgID > 65535 {
		log.Fatalf("invalid -tpg-id %d: must be 1-65535", *tpgID)
	}

	logger := log.New(os.Stdout, "[iscsi] ", log.LstdFlags)

	var vol *blockvol.BlockVol
	var err error

	if *create {
		volSize, parseErr := parseSize(*size)
		if parseErr != nil {
			log.Fatalf("invalid size %q: %v", *size, parseErr)
		}
		if _, statErr := os.Stat(*volPath); statErr == nil {
			// File exists -- open it instead of failing
			vol, err = blockvol.OpenBlockVol(*volPath)
			if err != nil {
				log.Fatalf("open existing volume: %v", err)
			}
			logger.Printf("opened existing volume: %s", *volPath)
		} else {
			vol, err = blockvol.CreateBlockVol(*volPath, blockvol.CreateOptions{
				VolumeSize: volSize,
				BlockSize:  4096,
				WALSize:    64 * 1024 * 1024,
			})
			if err != nil {
				log.Fatalf("create volume: %v", err)
			}
			logger.Printf("created volume: %s (%s)", *volPath, *size)
		}
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

	// Create adapter with ALUA support and latency instrumentation
	adapter := &instrumentedAdapter{
		inner:  &blockVolAdapter{vol: vol, tpgID: uint16(*tpgID)},
		logger: logger,
	}

	// Create target server
	config := iscsi.DefaultTargetConfig()
	config.TargetName = *iqn
	config.TargetAlias = "SeaweedFS BlockVol"
	if *portal != "" {
		// Parse portal group tag from "addr:port,tpgt" format
		if idx := strings.LastIndex(*portal, ","); idx >= 0 {
			if tpgt, err := strconv.Atoi((*portal)[idx+1:]); err == nil {
				config.TargetPortalGroupTag = tpgt
			}
		}
	}
	ts := iscsi.NewTargetServer(*addr, config, logger)
	if *portal != "" {
		ts.SetPortalAddr(*portal)
	}
	ts.AddVolume(*iqn, adapter)

	// Start periodic performance stats logging (every 5 seconds).
	adapter.StartStatsLogger(5 * time.Second)

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

// blockVolAdapter wraps BlockVol to implement iscsi.BlockDevice and iscsi.ALUAProvider.
type blockVolAdapter struct {
	vol   *blockvol.BlockVol
	tpgID uint16
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

// ALUAProvider implementation.
func (a *blockVolAdapter) ALUAState() uint8    { return roleToALUA(a.vol.Role()) }
func (a *blockVolAdapter) TPGroupID() uint16   { return a.tpgID }
func (a *blockVolAdapter) DeviceNAA() [8]byte  { return uuidToNAA(a.vol.Info().UUID) }

// roleToALUA maps a BlockVol Role to an ALUA asymmetric access state.
// RoleNone maps to Active/Optimized so standalone single-node targets
// (no assignment from master) can accept writes.
func roleToALUA(r blockvol.Role) uint8 {
	switch r {
	case blockvol.RolePrimary, blockvol.RoleNone:
		return iscsi.ALUAActiveOptimized
	case blockvol.RoleReplica:
		return iscsi.ALUAStandby
	case blockvol.RoleStale:
		return iscsi.ALUAUnavailable
	case blockvol.RoleRebuilding, blockvol.RoleDraining:
		return iscsi.ALUATransitioning
	default:
		return iscsi.ALUAStandby
	}
}

// uuidToNAA converts a 16-byte UUID to an 8-byte NAA-6 identifier.
// NAA-6 format: nibble 6 (NAA=6) followed by 60 bits from the UUID.
func uuidToNAA(uuid [16]byte) [8]byte {
	var naa [8]byte
	// Set NAA=6 in the high nibble of the first byte.
	naa[0] = 0x60 | (uuid[0] & 0x0F)
	copy(naa[1:], uuid[1:8])
	return naa
}

// instrumentedAdapter wraps a BlockDevice and logs latency stats periodically.
type instrumentedAdapter struct {
	inner  iscsi.BlockDevice
	logger *log.Logger

	// Counters (atomic, lock-free)
	writeOps   atomic.Int64
	readOps    atomic.Int64
	syncOps    atomic.Int64
	writeTotUs atomic.Int64 // total microseconds
	readTotUs  atomic.Int64
	syncTotUs  atomic.Int64
	writeMaxUs atomic.Int64
	readMaxUs  atomic.Int64
	syncMaxUs  atomic.Int64
	writeBytes atomic.Int64
	readBytes  atomic.Int64
}

func (a *instrumentedAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	start := time.Now()
	data, err := a.inner.ReadAt(lba, length)
	us := time.Since(start).Microseconds()
	a.readOps.Add(1)
	a.readTotUs.Add(us)
	a.readBytes.Add(int64(length))
	atomicMax(&a.readMaxUs, us)
	return data, err
}

func (a *instrumentedAdapter) WriteAt(lba uint64, data []byte) error {
	start := time.Now()
	err := a.inner.WriteAt(lba, data)
	us := time.Since(start).Microseconds()
	a.writeOps.Add(1)
	a.writeTotUs.Add(us)
	a.writeBytes.Add(int64(len(data)))
	atomicMax(&a.writeMaxUs, us)
	return err
}

func (a *instrumentedAdapter) Trim(lba uint64, length uint32) error {
	return a.inner.Trim(lba, length)
}

func (a *instrumentedAdapter) SyncCache() error {
	start := time.Now()
	err := a.inner.SyncCache()
	us := time.Since(start).Microseconds()
	a.syncOps.Add(1)
	a.syncTotUs.Add(us)
	atomicMax(&a.syncMaxUs, us)
	return err
}

func (a *instrumentedAdapter) BlockSize() uint32  { return a.inner.BlockSize() }
func (a *instrumentedAdapter) VolumeSize() uint64 { return a.inner.VolumeSize() }
func (a *instrumentedAdapter) IsHealthy() bool    { return a.inner.IsHealthy() }

// ALUAProvider proxy: delegate to inner device if it implements ALUAProvider.
func (a *instrumentedAdapter) ALUAState() uint8 {
	if p, ok := a.inner.(iscsi.ALUAProvider); ok {
		return p.ALUAState()
	}
	return iscsi.ALUAStandby
}
func (a *instrumentedAdapter) TPGroupID() uint16 {
	if p, ok := a.inner.(iscsi.ALUAProvider); ok {
		return p.TPGroupID()
	}
	return 1
}
func (a *instrumentedAdapter) DeviceNAA() [8]byte {
	if p, ok := a.inner.(iscsi.ALUAProvider); ok {
		return p.DeviceNAA()
	}
	return [8]byte{}
}

// StartStatsLogger runs a goroutine that logs performance stats every interval.
func (a *instrumentedAdapter) StartStatsLogger(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			wr := a.writeOps.Swap(0)
			rd := a.readOps.Swap(0)
			sy := a.syncOps.Swap(0)
			wrUs := a.writeTotUs.Swap(0)
			rdUs := a.readTotUs.Swap(0)
			syUs := a.syncTotUs.Swap(0)
			wrMax := a.writeMaxUs.Swap(0)
			rdMax := a.readMaxUs.Swap(0)
			syMax := a.syncMaxUs.Swap(0)
			wrBytes := a.writeBytes.Swap(0)
			rdBytes := a.readBytes.Swap(0)

			if wr+rd+sy == 0 {
				continue // quiet when idle
			}

			wrAvg, rdAvg, syAvg := int64(0), int64(0), int64(0)
			if wr > 0 {
				wrAvg = wrUs / wr
			}
			if rd > 0 {
				rdAvg = rdUs / rd
			}
			if sy > 0 {
				syAvg = syUs / sy
			}
			a.logger.Printf("PERF[%ds] wr=%d(%.1fMB avg=%dus max=%dus) rd=%d(%.1fMB avg=%dus max=%dus) sync=%d(avg=%dus max=%dus)",
				int(interval.Seconds()),
				wr, float64(wrBytes)/(1024*1024), wrAvg, wrMax,
				rd, float64(rdBytes)/(1024*1024), rdAvg, rdMax,
				sy, syAvg, syMax,
			)
		}
	}()
}

func atomicMax(addr *atomic.Int64, val int64) {
	for {
		old := addr.Load()
		if val <= old {
			return
		}
		if addr.CompareAndSwap(old, val) {
			return
		}
	}
}

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
