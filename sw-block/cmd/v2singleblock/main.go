package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/volumev2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

type smokeResult struct {
	VolumeName string `json:"volume_name"`
	Path       string `json:"path"`
	NodeID     string `json:"node_id"`
	Epoch      uint64 `json:"epoch"`
	Role       string `json:"role"`
	Mode       string `json:"mode"`
	Reason     string `json:"reason"`
	Readback   string `json:"readback_hex"`
}

type restartSmokeResult struct {
	VolumeName   string `json:"volume_name"`
	Path         string `json:"path"`
	NodeID       string `json:"node_id"`
	Epoch        uint64 `json:"epoch"`
	Role         string `json:"role"`
	Mode         string `json:"mode"`
	Reason       string `json:"reason"`
	InitialWrite string `json:"initial_write_hex"`
	PostRestart  string `json:"post_restart_hex"`
}

type iscsiSmokeResult struct {
	VolumeName string `json:"volume_name"`
	Path       string `json:"path"`
	NodeID     string `json:"node_id"`
	Epoch      uint64 `json:"epoch"`
	Role       string `json:"role"`
	Mode       string `json:"mode"`
	Reason     string `json:"reason"`
	IQN        string `json:"iqn"`
	Address    string `json:"address"`
}

type commonFlags struct {
	name      string
	path      string
	nodeID    string
	writeText string
	sizeBytes uint64
	blockSize uint
	walSize   uint64
}

type singleNodeEnv struct {
	master       *masterv2.Master
	node         *volumev2.Node
	orchestrator *volumev2.Orchestrator
}

var commandOutput io.Writer = os.Stdout

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "smoke":
		if err := runSmoke(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "restart-smoke":
		if err := runRestartSmoke(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "iscsi-smoke":
		if err := runISCSISmoke(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func runSmoke(args []string) error {
	cfg, err := parseCommonFlags("smoke", args)
	if err != nil {
		return err
	}
	if cfg.path == "" {
		return fmt.Errorf("smoke: --path is required")
	}

	env, err := bootstrapSingleNode(cfg)
	if err != nil {
		return err
	}
	defer env.close()

	payload := paddedPayload([]byte(cfg.writeText), uint32(cfg.blockSize))
	if err := env.node.WriteLBA(cfg.name, 0, payload); err != nil {
		return err
	}
	if err := env.node.SyncCache(cfg.name); err != nil {
		return err
	}
	readBack, err := env.node.ReadLBA(cfg.name, 0, uint32(len(payload)))
	if err != nil {
		return err
	}
	snap, err := env.node.Snapshot(cfg.name)
	if err != nil {
		return err
	}

	result := smokeResult{
		VolumeName: cfg.name,
		Path:       cfg.path,
		NodeID:     cfg.nodeID,
		Epoch:      snap.Status.Epoch,
		Role:       snap.Status.Role.String(),
		Readback:   hex.EncodeToString(readBack[:len(payload)]),
	}
	if snap.HasProjection {
		result.Mode = string(snap.Projection.Mode.Name)
		result.Reason = snap.Projection.Publication.Reason
	}
	return printJSON(result)
}

func runRestartSmoke(args []string) error {
	cfg, err := parseCommonFlags("restart-smoke", args)
	if err != nil {
		return err
	}
	if cfg.path == "" {
		return fmt.Errorf("restart-smoke: --path is required")
	}

	payload := paddedPayload([]byte(cfg.writeText), uint32(cfg.blockSize))
	master := masterv2.New(masterv2.Config{LeaseTTL: 30 * time.Second})
	if err := declarePrimary(master, cfg); err != nil {
		return err
	}

	func() {
		node, nodeErr := volumev2.New(volumev2.Config{NodeID: cfg.nodeID})
		if nodeErr != nil {
			err = nodeErr
			return
		}
		defer node.Close()
		session, nodeErr := volumev2.NewInProcessSession(master)
		if nodeErr != nil {
			err = nodeErr
			return
		}
		orchestrator, nodeErr := volumev2.NewOrchestrator(node, session)
		if nodeErr != nil {
			err = nodeErr
			return
		}
		if nodeErr = syncTwice(orchestrator); nodeErr != nil {
			err = nodeErr
			return
		}
		if nodeErr = node.WriteLBA(cfg.name, 0, payload); nodeErr != nil {
			err = nodeErr
			return
		}
		if nodeErr = node.SyncCache(cfg.name); nodeErr != nil {
			err = nodeErr
			return
		}
	}()
	if err != nil {
		return err
	}

	node, err := volumev2.New(volumev2.Config{NodeID: cfg.nodeID})
	if err != nil {
		return err
	}
	defer node.Close()
	session, err := volumev2.NewInProcessSession(master)
	if err != nil {
		return err
	}
	orchestrator, err := volumev2.NewOrchestrator(node, session)
	if err != nil {
		return err
	}
	if err := syncTwice(orchestrator); err != nil {
		return err
	}
	readBack, err := node.ReadLBA(cfg.name, 0, uint32(len(payload)))
	if err != nil {
		return err
	}
	snap, err := node.Snapshot(cfg.name)
	if err != nil {
		return err
	}

	result := restartSmokeResult{
		VolumeName:   cfg.name,
		Path:         cfg.path,
		NodeID:       cfg.nodeID,
		Epoch:        snap.Status.Epoch,
		Role:         snap.Status.Role.String(),
		InitialWrite: hex.EncodeToString(payload),
		PostRestart:  hex.EncodeToString(readBack[:len(payload)]),
	}
	if snap.HasProjection {
		result.Mode = string(snap.Projection.Mode.Name)
		result.Reason = snap.Projection.Publication.Reason
	}
	return printJSON(result)
}

func runISCSISmoke(args []string) error {
	fs := flag.NewFlagSet("iscsi-smoke", flag.ContinueOnError)
	name := fs.String("name", "single-node-vol", "logical volume name")
	path := fs.String("path", "", "block volume file path")
	nodeID := fs.String("node", "node-a", "volumev2 node id")
	writeText := fs.String("write-text", "v2-single-node-smoke", "payload written at LBA 0")
	sizeBytes := fs.Uint64("size-bytes", 1*1024*1024, "logical volume size in bytes")
	blockSize := fs.Uint("block-size", 4096, "block size in bytes")
	walSize := fs.Uint64("wal-size", 256*1024, "wal size in bytes")
	listenAddr := fs.String("listen-addr", "127.0.0.1:0", "iSCSI target listen address")
	iqn := fs.String("iqn", "", "target iqn")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg := commonFlags{
		name:      *name,
		path:      *path,
		nodeID:    *nodeID,
		writeText: *writeText,
		sizeBytes: *sizeBytes,
		blockSize: *blockSize,
		walSize:   *walSize,
	}
	if cfg.path == "" {
		return fmt.Errorf("iscsi-smoke: --path is required")
	}

	env, err := bootstrapSingleNode(cfg)
	if err != nil {
		return err
	}
	defer env.close()

	export, err := env.node.ExportISCSI(cfg.name, *listenAddr, *iqn)
	if err != nil {
		return err
	}
	defer export.Close()

	if err := performISCSILogin(export.Address(), export.IQN()); err != nil {
		return err
	}
	snap, err := env.node.Snapshot(cfg.name)
	if err != nil {
		return err
	}

	result := iscsiSmokeResult{
		VolumeName: cfg.name,
		Path:       cfg.path,
		NodeID:     cfg.nodeID,
		Epoch:      snap.Status.Epoch,
		Role:       snap.Status.Role.String(),
		IQN:        export.IQN(),
		Address:    export.Address(),
	}
	if snap.HasProjection {
		result.Mode = string(snap.Projection.Mode.Name)
		result.Reason = snap.Projection.Publication.Reason
	}
	return printJSON(result)
}

func parseCommonFlags(name string, args []string) (commonFlags, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	cfg := commonFlags{}
	namePtr := fs.String("name", "single-node-vol", "logical volume name")
	pathPtr := fs.String("path", "", "block volume file path")
	nodePtr := fs.String("node", "node-a", "volumev2 node id")
	writePtr := fs.String("write-text", "v2-single-node-smoke", "payload written at LBA 0")
	sizePtr := fs.Uint64("size-bytes", 1*1024*1024, "logical volume size in bytes")
	blockPtr := fs.Uint("block-size", 4096, "block size in bytes")
	walPtr := fs.Uint64("wal-size", 256*1024, "wal size in bytes")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.name = *namePtr
	cfg.path = *pathPtr
	cfg.nodeID = *nodePtr
	cfg.writeText = *writePtr
	cfg.sizeBytes = *sizePtr
	cfg.blockSize = *blockPtr
	cfg.walSize = *walPtr
	return cfg, nil
}

func bootstrapSingleNode(cfg commonFlags) (*singleNodeEnv, error) {
	master := masterv2.New(masterv2.Config{LeaseTTL: 30 * time.Second})
	if err := declarePrimary(master, cfg); err != nil {
		return nil, err
	}
	node, err := volumev2.New(volumev2.Config{NodeID: cfg.nodeID})
	if err != nil {
		return nil, err
	}
	session, err := volumev2.NewInProcessSession(master)
	if err != nil {
		node.Close()
		return nil, err
	}
	orchestrator, err := volumev2.NewOrchestrator(node, session)
	if err != nil {
		node.Close()
		return nil, err
	}
	if err := syncTwice(orchestrator); err != nil {
		node.Close()
		return nil, err
	}
	return &singleNodeEnv{
		master:       master,
		node:         node,
		orchestrator: orchestrator,
	}, nil
}

func declarePrimary(master *masterv2.Master, cfg commonFlags) error {
	return master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          cfg.name,
		Path:          cfg.path,
		PrimaryNodeID: cfg.nodeID,
		CreateOptions: blockvol.CreateOptions{
			VolumeSize: cfg.sizeBytes,
			BlockSize:  uint32(cfg.blockSize),
			WALSize:    cfg.walSize,
		},
	})
}

func (env *singleNodeEnv) close() {
	if env == nil || env.node == nil {
		return
	}
	env.node.Close()
}

func syncTwice(orchestrator *volumev2.Orchestrator) error {
	if err := orchestrator.SyncOnce(); err != nil {
		return err
	}
	return orchestrator.SyncOnce()
}

func performISCSILogin(addr, targetIQN string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dial iscsi target %s: %w", addr, err)
	}
	defer conn.Close()

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.2026-04.com.seaweedfs:cli.initiator")
	params.Set("TargetName", targetIQN)
	params.Set("SessionType", "Normal")

	loginReq := &iscsi.PDU{}
	loginReq.SetOpcode(iscsi.OpLoginReq)
	loginReq.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageFullFeature)
	loginReq.SetLoginTransit(true)
	loginReq.SetISID([6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x02})
	loginReq.SetCmdSN(1)
	loginReq.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, loginReq); err != nil {
		return fmt.Errorf("write iscsi login: %w", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		return fmt.Errorf("read iscsi login: %w", err)
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		return fmt.Errorf("iscsi login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	return nil
}

func paddedPayload(in []byte, blockSize uint32) []byte {
	size := int(blockSize)
	if size <= 0 {
		size = len(in)
	}
	if size < len(in) {
		size = len(in)
	}
	out := make([]byte, size)
	copy(out, in)
	return out
}

func printJSON(v any) error {
	enc := json.NewEncoder(commandOutput)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  v2singleblock smoke --path <file> [--name N --node N --write-text TEXT]")
	fmt.Fprintln(os.Stderr, "  v2singleblock restart-smoke --path <file> [--name N --node N --write-text TEXT]")
	fmt.Fprintln(os.Stderr, "  v2singleblock iscsi-smoke --path <file> [--name N --node N --iqn IQN]")
}
