package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterNVMeActions registers NVMe/TCP client actions.
func RegisterNVMeActions(r *tr.Registry) {
	r.RegisterFunc("nvme_connect", tr.TierBlock, nvmeConnect)
	r.RegisterFunc("nvme_disconnect", tr.TierBlock, nvmeDisconnect)
	r.RegisterFunc("nvme_get_device", tr.TierBlock, nvmeGetDevice)
	r.RegisterFunc("nvme_cleanup", tr.TierBlock, nvmeCleanup)
}

// nvmeConnect connects to an NVMe/TCP target.
// Params: target (required). Uses TargetSpec.NvmePort and NQN().
// Returns: value = NQN (for subsequent disconnect).
func nvmeConnect(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("nvme_connect: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("nvme_connect: target %q not in scenario", targetName)
	}

	host, err := GetTargetHost(actx, targetName)
	if err != nil {
		return nil, err
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_connect: %w", err)
	}

	nqn := spec.NQN()
	port := spec.NvmePort
	if port == 0 {
		port = 4420
	}

	actx.Log("  nvme connect %s -> %s:%d nqn=%s", targetName, host, port, nqn)
	cmd := fmt.Sprintf("nvme connect -t tcp -n %s -a %s -s %d", nqn, host, port)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		// Treat "already connected" as success.
		if strings.Contains(stdout+stderr, "already connected") {
			actx.Log("  already connected")
			return map[string]string{"value": nqn}, nil
		}
		return nil, fmt.Errorf("nvme_connect: code=%d stdout=%s stderr=%s err=%v", code, stdout, stderr, err)
	}

	return map[string]string{"value": nqn}, nil
}

// nvmeDisconnect disconnects from an NVMe/TCP target.
// Params: target (required).
func nvmeDisconnect(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("nvme_disconnect: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("nvme_disconnect: target %q not in scenario", targetName)
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_disconnect: %w", err)
	}

	nqn := spec.NQN()
	actx.Log("  nvme disconnect nqn=%s", nqn)
	cmd := fmt.Sprintf("nvme disconnect -n %s", nqn)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		outStr := stdout + stderr
		// Treat "not connected" / "no subsystem" as success (idempotent).
		if strings.Contains(outStr, "not connected") || strings.Contains(outStr, "No subsystemtype") || strings.Contains(outStr, "Invalid argument") {
			actx.Log("  already disconnected")
			return nil, nil
		}
		return nil, fmt.Errorf("nvme_disconnect: code=%d output=%s err=%v", code, outStr, err)
	}

	return nil, nil
}

// nvmeGetDevice finds the block device path for an NVMe/TCP connection.
// Params: target (required). Polls nvme list-subsys until device appears.
// Returns: value = /dev/nvmeXn1
func nvmeGetDevice(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("nvme_get_device: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("nvme_get_device: target %q not in scenario", targetName)
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_get_device: %w", err)
	}

	nqn := spec.NQN()
	actx.Log("  waiting for NVMe device for nqn=%s ...", nqn)

	// Poll for up to 10 seconds.
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, fmt.Errorf("nvme_get_device: timeout waiting for device (nqn=%s)", nqn)
		case <-ticker.C:
			dev, findErr := findNVMeDevice(ctx, node, nqn)
			if findErr != nil {
				continue // retry
			}
			if dev != "" {
				actx.Log("  found device: %s", dev)
				return map[string]string{"value": dev}, nil
			}
		}
	}
}

// nvmeCleanup disconnects all NVMe/TCP subsystems matching our prefix.
func nvmeCleanup(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("nvme_cleanup: %w", err)
	}

	cmd := "nvme disconnect-all 2>/dev/null || true"
	node.RunRoot(ctx, cmd)
	actx.Log("  nvme disconnect-all complete")
	return nil, nil
}

// findNVMeDevice parses `nvme list-subsys -o json` to find the device for a NQN.
func findNVMeDevice(ctx context.Context, node *infra.Node, nqn string) (string, error) {
	cmd := "nvme list-subsys -o json 2>/dev/null"
	stdout, _, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return "", fmt.Errorf("nvme list-subsys failed: code=%d err=%v", code, err)
	}

	// nvme list-subsys returns a JSON array of host entries, each with a Subsystems array.
	var hosts []nvmeSubsysOutput
	if err := json.Unmarshal([]byte(stdout), &hosts); err != nil {
		// Fallback: try parsing as a single object (older nvme-cli versions).
		var single nvmeSubsysOutput
		if err2 := json.Unmarshal([]byte(stdout), &single); err2 != nil {
			return "", fmt.Errorf("nvme list-subsys parse: %w", err)
		}
		hosts = []nvmeSubsysOutput{single}
	}

	for _, h := range hosts {
	for _, ss := range h.Subsystems {
		if ss.NQN != nqn {
			continue
		}
		for _, p := range ss.Paths {
			if p.Name == "" {
				continue
			}
			if strings.EqualFold(p.Transport, "tcp") && strings.EqualFold(p.State, "live") {
				return "/dev/" + p.Name + "n1", nil
			}
		}
		// Fallback: any path with a name.
		for _, p := range ss.Paths {
			if p.Name != "" {
				return "/dev/" + p.Name + "n1", nil
			}
		}
	}
	}
	return "", nil // not found yet
}

// JSON structures for nvme list-subsys output.
type nvmeSubsysOutput struct {
	Subsystems []nvmeSubsysEntry `json:"Subsystems"`
}

type nvmeSubsysEntry struct {
	NQN   string          `json:"NQN"`
	Paths []nvmePathEntry `json:"Paths"`
}

type nvmePathEntry struct {
	Name      string `json:"Name"`
	Transport string `json:"Transport"`
	State     string `json:"State"`
}
