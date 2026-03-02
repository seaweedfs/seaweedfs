//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ISCSIClient wraps iscsiadm commands on a node.
type ISCSIClient struct {
	node       *Node
	targetHost string // set after Discover, used to fix wildcard portals
	targetPort int
}

// NewISCSIClient creates an iSCSI client bound to a node.
func NewISCSIClient(node *Node) *ISCSIClient {
	return &ISCSIClient{node: node}
}

// Discover runs iSCSI SendTargets discovery and returns discovered IQNs.
// Remembers the target host for subsequent Login calls.
func (c *ISCSIClient) Discover(ctx context.Context, host string, port int) ([]string, error) {
	c.targetHost = host
	c.targetPort = port

	cmd := fmt.Sprintf("iscsiadm -m discovery -t sendtargets -p %s:%d", host, port)
	stdout, stderr, code, err := c.node.RunRoot(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("discovery error: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("discovery failed (code %d): %s", code, stderr)
	}

	var iqns []string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Format: "10.0.0.1:3260,1 iqn.2024.com.seaweedfs:vol1"
		// or:     "[::]:3260,-1 iqn.2024.com.seaweedfs:vol1"
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			iqns = append(iqns, parts[1])
		}
	}

	// Fix wildcard portals: target may advertise [::]:3260 but remote
	// initiators need the real IP. Delete wildcard records and re-create
	// with the correct portal address.
	for _, iqn := range iqns {
		c.fixNodePortal(ctx, iqn, host, port)
	}

	return iqns, nil
}

// fixNodePortal ensures the node record for iqn uses the actual target
// host address, not a wildcard like [::] or 0.0.0.0.
func (c *ISCSIClient) fixNodePortal(ctx context.Context, iqn, host string, port int) {
	// List node records for this IQN
	stdout, _, _, _ := c.node.RunRoot(ctx,
		fmt.Sprintf("iscsiadm -m node -T %s 2>/dev/null", iqn))

	// Check if any record has a wildcard address
	hasWildcard := false
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, "node.conn[0].address") {
			if strings.Contains(line, "::") || strings.Contains(line, "0.0.0.0") {
				hasWildcard = true
			}
		}
	}
	if !hasWildcard {
		return
	}

	// Delete ALL node records for this IQN (wildcard ones)
	c.node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -o delete 2>/dev/null", iqn))

	// Create a new node record with the correct portal
	portal := fmt.Sprintf("%s:%d", host, port)
	c.node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -p %s -o new 2>/dev/null", iqn, portal))
}

// Login connects to the target and returns the device path (e.g. /dev/sda).
// Uses explicit portal from Discover when available to avoid wildcard issues.
func (c *ISCSIClient) Login(ctx context.Context, iqn string) (string, error) {
	var cmd string
	if c.targetHost != "" && c.targetHost != "127.0.0.1" && c.targetHost != "localhost" {
		// Remote mode: use explicit portal to avoid wildcard [::] issue
		portal := fmt.Sprintf("%s:%d", c.targetHost, c.targetPort)
		cmd = fmt.Sprintf("iscsiadm -m node -T %s -p %s --login", iqn, portal)
	} else {
		// Local/WSL2 mode: IQN-only works fine
		cmd = fmt.Sprintf("iscsiadm -m node -T %s --login", iqn)
	}
	_, stderr, code, err := c.node.RunRoot(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("login error: %w", err)
	}
	if code != 0 {
		return "", fmt.Errorf("login failed (code %d): %s", code, stderr)
	}

	// Poll for device to appear (kernel creates /dev/sdX asynchronously)
	return c.waitForDevice(ctx, iqn)
}

// Logout disconnects from the target.
func (c *ISCSIClient) Logout(ctx context.Context, iqn string) error {
	cmd := fmt.Sprintf("iscsiadm -m node -T %s --logout", iqn)
	_, stderr, code, err := c.node.RunRoot(ctx, cmd)
	if err != nil {
		return fmt.Errorf("logout error: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("logout failed (code %d): %s", code, stderr)
	}
	return nil
}

// GetDevice returns the device path for an active session.
func (c *ISCSIClient) GetDevice(ctx context.Context, iqn string) (string, error) {
	return c.waitForDevice(ctx, iqn)
}

func (c *ISCSIClient) waitForDevice(ctx context.Context, iqn string) (string, error) {
	deadline := time.Now().Add(30 * time.Second)
	rescanned := false
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		// Parse session details to find the attached disk
		stdout, _, code, _ := c.node.RunRoot(ctx, "iscsiadm -m session -P3")
		if code == 0 {
			dev := parseDeviceFromSession(stdout, iqn)
			if dev != "" {
				return dev, nil
			}
		}

		// After 5s without device, force a LUN rescan (WSL2 needs this)
		if !rescanned && time.Until(deadline) < 25*time.Second {
			c.node.RunRoot(ctx, "iscsiadm -m session -R")
			rescanned = true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("device for %s did not appear within 30s", iqn)
}

// parseDeviceFromSession extracts /dev/sdX from iscsiadm -m session -P3 output.
func parseDeviceFromSession(output, iqn string) string {
	lines := strings.Split(output, "\n")
	inTarget := false
	for _, line := range lines {
		if strings.Contains(line, "Target: "+iqn) {
			inTarget = true
			continue
		}
		if inTarget && strings.Contains(line, "Target: ") {
			break // next target
		}
		if inTarget && strings.Contains(line, "Attached scsi disk") {
			// "Attached scsi disk sda          State: running"
			fields := strings.Fields(line)
			for i, f := range fields {
				if f == "disk" && i+1 < len(fields) {
					return "/dev/" + fields[i+1]
				}
			}
		}
	}
	return ""
}

// WaitForSession polls until a session for the given IQN is in LOGGED_IN state.
// Used after Kill9+Restart to wait for iSCSI session recovery.
func (c *ISCSIClient) WaitForSession(ctx context.Context, iqn string) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("session %s did not recover: %w", iqn, ctx.Err())
		default:
		}

		stdout, _, code, _ := c.node.RunRoot(ctx, "iscsiadm -m session")
		if code == 0 && strings.Contains(stdout, iqn) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// CleanupAll force-logouts sessions matching the IQN prefix only.
// Does not touch other iSCSI sessions on the node.
func (c *ISCSIClient) CleanupAll(ctx context.Context, iqnPrefix string) error {
	stdout, _, _, _ := c.node.RunRoot(ctx, "iscsiadm -m session 2>&1")
	if stdout == "" || strings.Contains(stdout, "No active sessions") {
		return nil
	}

	// Parse session lines: "tcp: [N] 10.0.0.1:3260,1 iqn.2024.com.seaweedfs:test-..."
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, iqnPrefix) {
			continue
		}
		// Extract IQN from the line
		fields := strings.Fields(line)
		for _, f := range fields {
			if strings.HasPrefix(f, iqnPrefix) {
				c.node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s --logout 2>/dev/null", f))
				c.node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -o delete 2>/dev/null", f))
			}
		}
	}
	return nil
}
