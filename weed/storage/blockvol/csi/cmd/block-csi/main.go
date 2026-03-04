// block-csi is the SeaweedFS BlockVol CSI driver.
// It embeds a BlockVol engine and iSCSI target in-process, serving
// CSI Identity, Controller, and Node services on a Unix socket.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	blockcsi "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/csi"
)

func main() {
	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint (unix socket)")
	dataDir := flag.String("data-dir", "/var/lib/sw-block", "volume data directory")
	iscsiAddr := flag.String("iscsi-addr", "127.0.0.1:3260", "local iSCSI target listen address")
	iqnPrefix := flag.String("iqn-prefix", "iqn.2024.com.seaweedfs", "IQN prefix for volumes")
	nodeID := flag.String("node-id", "", "node identifier (required)")
	masterAddr := flag.String("master", "", "master address for control-plane mode (e.g. master:9333)")
	mode := flag.String("mode", "all", "driver mode: controller, node, or all")
	flag.Parse()

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "error: -node-id is required")
		flag.Usage()
		os.Exit(1)
	}
	if *mode == "controller" && *masterAddr == "" {
		fmt.Fprintln(os.Stderr, "error: -master is required in controller mode")
		flag.Usage()
		os.Exit(1)
	}

	logger := log.New(os.Stdout, "[block-csi] ", log.LstdFlags)

	driver, err := blockcsi.NewCSIDriver(blockcsi.DriverConfig{
		Endpoint:   *endpoint,
		DataDir:    *dataDir,
		ISCSIAddr:  *iscsiAddr,
		IQNPrefix:  *iqnPrefix,
		NodeID:     *nodeID,
		MasterAddr: *masterAddr,
		Mode:       *mode,
		Logger:     logger,
	})
	if err != nil {
		log.Fatalf("create CSI driver: %v", err)
	}

	// Graceful shutdown on signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("received %v, shutting down...", sig)
		driver.Stop()
	}()

	logger.Printf("starting block-csi driver: node=%s endpoint=%s", *nodeID, *endpoint)
	if err := driver.Run(); err != nil {
		log.Fatalf("CSI driver: %v", err)
	}
	logger.Println("block-csi driver stopped")
}
