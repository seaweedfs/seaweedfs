package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 11 P3: Front-end publication rebinding
//
// Proofs:
//   1. Create/Lookup coherence: iSCSI + NVMe fields match
//   2. Failover: publication truth switches to new primary
//   3. Heartbeat reconstruction: fields survive restart
//   4. No-NVMe fallback: explicit and coherent
//
// V1 reuse roles:
//   master_grpc_server_block.go: reuse as bounded adapter
//   master_block_registry.go: reuse as bounded truth carrier
//   volume_server_block.go: reuse as publication source only
// ============================================================

func newPublicationMaster(t *testing.T, nvmeEnabled bool) *MasterServer {
	t.Helper()
	dir := t.TempDir()

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		os.MkdirAll(serverDir, 0755)
		volPath := filepath.Join(serverDir, fmt.Sprintf("%s.blk", name))
		vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
			VolumeSize: 1 * 1024 * 1024,
			BlockSize:  4096,
			WALSize:    256 * 1024,
		})
		if err != nil {
			return nil, err
		}
		vol.Close()

		host := server
		if idx := strings.LastIndex(server, ":"); idx >= 0 {
			host = server[:idx]
		}

		result := &blockAllocResult{
			Path:              volPath,
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         host + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}
		if nvmeEnabled {
			result.NvmeAddr = host + ":4420"
			result.NQN = fmt.Sprintf("nqn.2024-01.com.seaweedfs:vol.%s", name)
		}
		return result, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error { return nil }

	return ms
}

// --- 1. Create/Lookup coherence ---

func TestP11P3_CreateLookup_PublicationCoherence(t *testing.T) {
	ms := newPublicationMaster(t, true)
	ctx := context.Background()

	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "pub-vol-1", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Create response has iSCSI + NVMe fields.
	if createResp.IscsiAddr == "" || createResp.Iqn == "" {
		t.Fatal("create missing iSCSI fields")
	}
	if createResp.NvmeAddr == "" || createResp.Nqn == "" {
		t.Fatal("create missing NVMe fields")
	}

	// Lookup must return the SAME publication fields.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pub-vol-1"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	if lookupResp.IscsiAddr != createResp.IscsiAddr {
		t.Fatalf("iSCSI addr mismatch: create=%q lookup=%q", createResp.IscsiAddr, lookupResp.IscsiAddr)
	}
	if lookupResp.Iqn != createResp.Iqn {
		t.Fatalf("IQN mismatch: create=%q lookup=%q", createResp.Iqn, lookupResp.Iqn)
	}
	if lookupResp.NvmeAddr != createResp.NvmeAddr {
		t.Fatalf("NVMe addr mismatch: create=%q lookup=%q", createResp.NvmeAddr, lookupResp.NvmeAddr)
	}
	if lookupResp.Nqn != createResp.Nqn {
		t.Fatalf("NQN mismatch: create=%q lookup=%q", createResp.Nqn, lookupResp.Nqn)
	}

	// Verify: registry truth matches.
	entry, _ := ms.blockRegistry.Lookup("pub-vol-1")
	if entry.ISCSIAddr != createResp.IscsiAddr {
		t.Fatalf("registry iSCSI=%q != create=%q", entry.ISCSIAddr, createResp.IscsiAddr)
	}
	if entry.NvmeAddr != createResp.NvmeAddr {
		t.Fatalf("registry NVMe=%q != create=%q", entry.NvmeAddr, createResp.NvmeAddr)
	}

	t.Logf("P11P3 coherence: create=lookup=registry — iSCSI=%s NVMe=%s IQN=%s NQN=%s",
		createResp.IscsiAddr, createResp.NvmeAddr, createResp.Iqn, createResp.Nqn)
}

// --- 2. Failover: publication truth switches ---

func TestP11P3_Failover_PublicationSwitches(t *testing.T) {
	ms := newPublicationMaster(t, true)
	ctx := context.Background()

	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "pub-vol-2", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	oldPrimary := createResp.VolumeServer
	oldISCSI := createResp.IscsiAddr
	oldNVMe := createResp.NvmeAddr

	t.Logf("before failover: primary=%s iSCSI=%s NVMe=%s", oldPrimary, oldISCSI, oldNVMe)

	// Expire lease and failover.
	ms.blockRegistry.UpdateEntry("pub-vol-2", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes(oldPrimary)

	// Lookup after failover: publication fields should reflect NEW primary.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pub-vol-2"})
	if err != nil {
		t.Fatalf("Lookup after failover: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pub-vol-2")
	if entry.VolumeServer == oldPrimary {
		t.Fatalf("primary should have changed, still %s", oldPrimary)
	}

	// New primary's publication fields should differ from old primary's.
	if lookupResp.IscsiAddr == oldISCSI {
		t.Fatalf("iSCSI addr should change after failover: still %q", lookupResp.IscsiAddr)
	}
	if lookupResp.NvmeAddr == oldNVMe {
		t.Fatalf("NVMe addr should change after failover: still %q", lookupResp.NvmeAddr)
	}

	// Registry and lookup must still agree.
	if lookupResp.IscsiAddr != entry.ISCSIAddr {
		t.Fatalf("post-failover: lookup iSCSI=%q != registry=%q", lookupResp.IscsiAddr, entry.ISCSIAddr)
	}
	if lookupResp.NvmeAddr != entry.NvmeAddr {
		t.Fatalf("post-failover: lookup NVMe=%q != registry=%q", lookupResp.NvmeAddr, entry.NvmeAddr)
	}

	t.Logf("P11P3 failover: old=%s→new=%s iSCSI=%s NVMe=%s (switched)",
		oldPrimary, entry.VolumeServer, lookupResp.IscsiAddr, lookupResp.NvmeAddr)
}

// --- 3. Heartbeat reconstruction ---

func TestP11P3_HeartbeatReconstruction(t *testing.T) {
	ms := newPublicationMaster(t, true)
	ctx := context.Background()

	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "pub-vol-3", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pub-vol-3")
	originalNVMe := entry.NvmeAddr
	originalNQN := entry.NQN
	primaryServer := entry.VolumeServer

	// Clear NVMe fields to simulate master restart state loss.
	ms.blockRegistry.UpdateEntry("pub-vol-3", func(e *BlockVolumeEntry) {
		e.NvmeAddr = ""
		e.NQN = ""
	})

	cleared, _ := ms.blockRegistry.Lookup("pub-vol-3")
	if cleared.NvmeAddr != "" {
		t.Fatal("NvmeAddr should be cleared")
	}

	// Reconstruct via REAL heartbeat path: UpdateFullHeartbeat with proto info.
	// This is the same code path as master_grpc_server.go:280.
	heartbeatInfo := &master_pb.BlockVolumeInfoMessage{
		Path:           entry.Path,
		VolumeSize:     entry.SizeBytes,
		BlockSize:      4096,
		Epoch:          entry.Epoch,
		Role:           entry.Role,
		NvmeAddr:       originalNVMe,
		Nqn:            originalNQN,
	}

	// Derive server-level NVMe addr (same as what the VS heartbeat carries).
	host := primaryServer
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		host = host[:idx]
	}
	serverNvmeAddr := host + ":4420"

	ms.blockRegistry.UpdateFullHeartbeat(
		primaryServer,
		[]*master_pb.BlockVolumeInfoMessage{heartbeatInfo},
		serverNvmeAddr,
	)

	// Verify: lookup reflects heartbeat-reconstructed NVMe truth.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pub-vol-3"})
	if err != nil {
		t.Fatalf("Lookup after reconstruction: %v", err)
	}
	if lookupResp.NvmeAddr != originalNVMe {
		t.Fatalf("reconstructed NvmeAddr=%q, want %q", lookupResp.NvmeAddr, originalNVMe)
	}
	if lookupResp.Nqn != originalNQN {
		t.Fatalf("reconstructed NQN=%q, want %q", lookupResp.Nqn, originalNQN)
	}

	_ = createResp // used for initial creation

	t.Logf("P11P3 reconstruction: cleared → UpdateFullHeartbeat → lookup restored NVMe=%s NQN=%s",
		lookupResp.NvmeAddr, lookupResp.Nqn)
}

// --- 4. No-NVMe fallback ---

func TestP11P3_NoNVMe_Fallback(t *testing.T) {
	ms := newPublicationMaster(t, false) // NVMe disabled
	ctx := context.Background()

	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "pub-vol-4", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// iSCSI must be present.
	if createResp.IscsiAddr == "" || createResp.Iqn == "" {
		t.Fatal("iSCSI fields must be present even without NVMe")
	}

	// NVMe must be explicitly empty (not fabricated).
	if createResp.NvmeAddr != "" {
		t.Fatalf("NvmeAddr=%q should be empty when NVMe disabled", createResp.NvmeAddr)
	}
	if createResp.Nqn != "" {
		t.Fatalf("Nqn=%q should be empty when NVMe disabled", createResp.Nqn)
	}

	// Lookup must also show empty NVMe.
	lookupResp, _ := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pub-vol-4"})
	if lookupResp.NvmeAddr != "" || lookupResp.Nqn != "" {
		t.Fatalf("lookup: NVMe should be empty, got addr=%q nqn=%q", lookupResp.NvmeAddr, lookupResp.Nqn)
	}

	// Registry must also show empty NVMe.
	entry, _ := ms.blockRegistry.Lookup("pub-vol-4")
	if entry.NvmeAddr != "" || entry.NQN != "" {
		t.Fatalf("registry: NVMe should be empty, got addr=%q nqn=%q", entry.NvmeAddr, entry.NQN)
	}

	t.Logf("P11P3 no-NVMe: iSCSI=%s IQN=%s, NVMe explicitly empty across create/lookup/registry",
		createResp.IscsiAddr, createResp.Iqn)
}
