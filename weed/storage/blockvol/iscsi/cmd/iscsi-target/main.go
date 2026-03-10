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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/nvme"
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
	walSize := flag.String("wal-size", "64M", "WAL size (e.g., 64M, 128M) -- used with -create")
	chapUser := flag.String("chap-user", "", "CHAP username (empty = CHAP disabled)")
	chapSecret := flag.String("chap-secret", "", "CHAP shared secret")
	nvmeAddr := flag.String("nvme-addr", "", "NVMe/TCP listen address (e.g. :4420; empty = disabled)")
	nqn := flag.String("nqn", "", "NVMe NQN (defaults to nqn.2024-01.com.seaweedfs:vol.<sanitized iqn suffix>)")
	walMaxCW := flag.Int("wal-max-concurrent-writes", 0, "max concurrent writers in WAL append path (0 = use default 16)")
	nvmeIOQueues := flag.Int("nvme-io-queues", 0, "max NVMe IO queues (0 = use default 4)")
	ioBackend := flag.String("io-backend", "standard", "flusher I/O backend: standard, auto, io_uring")
	flag.Parse()

	if *volPath == "" {
		fmt.Fprintln(os.Stderr, "error: -vol is required")
		flag.Usage()
		os.Exit(1)
	}
	if *tpgID < 1 || *tpgID > 65535 {
		log.Fatalf("invalid -tpg-id %d: must be 1-65535", *tpgID)
	}
	if *chapUser != "" && *chapSecret == "" {
		log.Fatalf("-chap-secret is required when -chap-user is set")
	}

	logger := log.New(os.Stdout, "[iscsi] ", log.LstdFlags)

	// Build config.
	cfg := blockvol.DefaultConfig()
	cfg.IOBackend = blockvol.IOBackendMode(*ioBackend)
	if *walMaxCW > 0 {
		cfg.WALMaxConcurrentWrites = *walMaxCW
		logger.Printf("WALMaxConcurrentWrites = %d", *walMaxCW)
	}
	cfgs := []blockvol.BlockVolConfig{cfg}

	var vol *blockvol.BlockVol
	var err error

	if *create {
		volSize, parseErr := parseSize(*size)
		if parseErr != nil {
			log.Fatalf("invalid size %q: %v", *size, parseErr)
		}
		walBytes, parseErr := parseSize(*walSize)
		if parseErr != nil {
			log.Fatalf("invalid wal-size %q: %v", *walSize, parseErr)
		}
		if _, statErr := os.Stat(*volPath); statErr == nil {
			// File exists -- open it instead of failing
			vol, err = blockvol.OpenBlockVol(*volPath, cfgs...)
			if err != nil {
				log.Fatalf("open existing volume: %v", err)
			}
			logger.Printf("opened existing volume: %s", *volPath)
		} else {
			vol, err = blockvol.CreateBlockVol(*volPath, blockvol.CreateOptions{
				VolumeSize: volSize,
				BlockSize:  4096,
				WALSize:    walBytes,
			}, cfgs...)
			if err != nil {
				log.Fatalf("create volume: %v", err)
			}
			logger.Printf("created volume: %s (%s)", *volPath, *size)
		}
	} else {
		vol, err = blockvol.OpenBlockVol(*volPath, cfgs...)
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

	// Create Prometheus registry and metrics adapter.
	promReg := prometheus.NewRegistry()
	instrumented := &instrumentedAdapter{
		inner:  &blockvol.BlockVolAdapter{Vol: vol, TPGID: uint16(*tpgID)},
		logger: logger,
	}
	adapter := newMetricsAdapter(instrumented, vol, promReg)

	// Start admin HTTP server if configured
	if *adminAddr != "" {
		adm := newAdminServer(vol, *adminToken, logger)
		adm.metricsRegistry = promReg
		ln, err := startAdminServer(*adminAddr, adm)
		if err != nil {
			log.Fatalf("start admin server: %v", err)
		}
		defer ln.Close()
		logger.Printf("admin server: %s", ln.Addr())
	}

	// Create target server
	config := iscsi.DefaultTargetConfig()
	config.TargetName = *iqn
	config.TargetAlias = "SeaweedFS BlockVol"
	if *chapUser != "" && *chapSecret != "" {
		config.CHAPConfig = iscsi.CHAPConfig{
			Enabled:  true,
			Username: *chapUser,
			Secret:   *chapSecret,
		}
		logger.Printf("CHAP authentication enabled for user %q", *chapUser)
	}
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

	// Start NVMe/TCP target if configured.
	var nvmeSrv *nvme.Server
	if *nvmeAddr != "" {
		nvmeNQN := *nqn
		if nvmeNQN == "" {
			// Derive NQN from IQN: extract suffix after last ':'
			iqnParts := strings.SplitN(*iqn, ":", 2)
			suffix := *iqn
			if len(iqnParts) == 2 {
				suffix = iqnParts[1]
			}
			nvmeNQN = blockvol.BuildNQN("nqn.2024-01.com.seaweedfs:vol.", suffix)
		}

		nvmeCfg := nvme.DefaultConfig()
		nvmeCfg.ListenAddr = *nvmeAddr
		nvmeCfg.Enabled = true
		if *nvmeIOQueues > 0 {
			nvmeCfg.MaxIOQueues = uint16(*nvmeIOQueues)
			logger.Printf("NVMe MaxIOQueues = %d", *nvmeIOQueues)
		}

		nvmeSrv = nvme.NewServer(nvmeCfg)
		nvmeSrv.AddVolume(nvmeNQN, adapter, [16]byte{}) // NGUID zero = auto
		if err := nvmeSrv.ListenAndServe(); err != nil {
			log.Fatalf("nvme target: %v", err)
		}
		logger.Printf("NVMe/TCP target: %s on %s", nvmeNQN, *nvmeAddr)
	}

	// Start periodic performance stats logging (every 5 seconds).
	instrumented.StartStatsLogger(5 * time.Second)

	// Graceful shutdown on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("received %v, shutting down...", sig)
		if nvmeSrv != nil {
			nvmeSrv.Close()
		}
		ts.Close()
	}()

	logger.Printf("starting iSCSI target: %s on %s", *iqn, *addr)
	if err := ts.ListenAndServe(); err != nil {
		log.Fatalf("target server: %v", err)
	}
	logger.Println("target stopped")
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
