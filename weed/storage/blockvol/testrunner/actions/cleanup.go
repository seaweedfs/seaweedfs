package actions

import (
	"context"
	"fmt"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterCleanupActions registers environment cleanup and device discovery actions.
func RegisterCleanupActions(r *tr.Registry) {
	r.RegisterFunc("pre_run_cleanup", tr.TierCore, preRunCleanup)
	r.RegisterFunc("nvme_connect_direct", tr.TierBlock, nvmeConnectDirect)
	r.RegisterFunc("nvme_disconnect_all", tr.TierBlock, nvmeDisconnectAll)
}

// preRunCleanup kills stale processes, unmounts filesystems, disconnects
// NVMe/iSCSI sessions, and verifies ports are free. Runs on a specified node.
//
// Params:
//   - kill_patterns: comma-separated process names to kill (default: "weed,iscsi-target,postgres")
//   - unmount: comma-separated mount points to unmount
//   - nvme_disconnect: "true" to disconnect all NVMe sessions
//   - iscsi_logout_prefix: IQN prefix to logout (e.g., "iqn.2024-01.com.seaweedfs")
//   - check_ports: comma-separated ports that must be free after cleanup
//
// Always succeeds (ignore_error semantics built in) — logs warnings but doesn't fail the scenario.
func preRunCleanup(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("pre_run_cleanup: %w", err)
	}

	var cleaned []string

	// Kill stale processes.
	patterns := act.Params["kill_patterns"]
	if patterns == "" {
		patterns = "weed,iscsi-target,postgres"
	}
	for _, p := range strings.Split(patterns, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		node.RunRoot(ctx, fmt.Sprintf("pkill -9 %s 2>/dev/null || true", p))
		cleaned = append(cleaned, "kill:"+p)
	}

	// Unmount filesystems.
	if mounts := act.Params["unmount"]; mounts != "" {
		for _, m := range strings.Split(mounts, ",") {
			m = strings.TrimSpace(m)
			if m == "" {
				continue
			}
			node.RunRoot(ctx, fmt.Sprintf("umount -l %s 2>/dev/null || true", m))
			cleaned = append(cleaned, "umount:"+m)
		}
	}

	// Disconnect NVMe.
	if act.Params["nvme_disconnect"] == "true" {
		node.RunRoot(ctx, "nvme disconnect-all 2>/dev/null || true")
		cleaned = append(cleaned, "nvme:disconnect-all")
	}

	// Logout iSCSI sessions.
	if prefix := act.Params["iscsi_logout_prefix"]; prefix != "" {
		node.RunRoot(ctx, fmt.Sprintf(
			"iscsiadm -m session 2>/dev/null | grep '%s' | awk '{print $4}' | while read iqn; do "+
				"iscsiadm -m node -T $iqn --logout 2>/dev/null; "+
				"iscsiadm -m node -T $iqn -o delete 2>/dev/null; done || true", prefix))
		cleaned = append(cleaned, "iscsi:"+prefix)
	}

	// Check ports are free.
	if ports := act.Params["check_ports"]; ports != "" {
		for _, p := range strings.Split(ports, ",") {
			p = strings.TrimSpace(p)
			stdout, _, _, _ := node.RunRoot(ctx, fmt.Sprintf("ss -tlnp | grep ':%s ' | head -1", p))
			if strings.TrimSpace(stdout) != "" {
				actx.Log("  WARNING: port %s still in use after cleanup: %s", p, strings.TrimSpace(stdout))
			}
		}
	}

	actx.Log("  cleanup: %s", strings.Join(cleaned, ", "))
	return map[string]string{"value": strings.Join(cleaned, ",")}, nil
}

// nvmeConnect connects to an NVMe-oF target and returns the discovered device path.
// Handles modprobe, disconnect stale sessions, connect, and device discovery.
//
// Params:
//   - target_addr: NVMe target IP (required)
//   - target_port: NVMe target port (default: "4420")
//   - nqn: NVMe subsystem NQN (required)
//   - transport: "tcp" or "rdma" (default: "tcp")
//   - expected_size: expected device size for discovery (e.g., "2G") (optional)
//
// Returns: value = device path (e.g., "/dev/nvme1n1")
func nvmeConnectDirect(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_connect: %w", err)
	}

	addr := act.Params["target_addr"]
	if addr == "" {
		return nil, fmt.Errorf("nvme_connect: target_addr required")
	}
	port := paramDefault(act.Params, "target_port", "4420")
	nqn := act.Params["nqn"]
	if nqn == "" {
		return nil, fmt.Errorf("nvme_connect: nqn required")
	}
	transport := paramDefault(act.Params, "transport", "tcp")

	// Ensure NVMe-TCP kernel module is loaded.
	node.RunRoot(ctx, fmt.Sprintf("modprobe nvme_%s 2>/dev/null || true", transport))

	// Connect.
	cmd := fmt.Sprintf("nvme connect -t %s -a %s -s %s -n %s 2>&1", transport, addr, port, nqn)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("nvme_connect: code=%d stdout=%s stderr=%s err=%v", code, stdout, stderr, err)
	}

	// Wait for device to appear.
	node.Run(ctx, "sleep 2")

	// Discover the device. Strategy: find NVMe namespace matching expected size.
	expectedSize := act.Params["expected_size"]
	var devCmd string
	if expectedSize != "" {
		devCmd = fmt.Sprintf("lsblk -dpno NAME,SIZE | grep '%s' | head -1 | awk '{print $1}'", expectedSize)
	} else {
		// Fall back to newest NVMe device (not nvme0 which is the boot disk).
		devCmd = "lsblk -dpno NAME | grep nvme | grep -v nvme0 | tail -1"
	}

	devOut, _, _, _ := node.RunRoot(ctx, devCmd)
	device := strings.TrimSpace(devOut)
	if device == "" {
		return nil, fmt.Errorf("nvme_connect: connected but no device found (expected_size=%s)", expectedSize)
	}

	actx.Log("  nvme connected: %s → %s", nqn, device)
	return map[string]string{"value": device}, nil
}

// nvmeDisconnectAll disconnects all NVMe-oF sessions on the node.
func nvmeDisconnectAll(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_disconnect_all: %w", err)
	}
	node.RunRoot(ctx, "nvme disconnect-all 2>/dev/null || true")
	return nil, nil
}
