package infra

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ISCSIClient wraps iscsiadm commands on a node.
type ISCSIClient struct {
	Node       *Node
	TargetHost string
	TargetPort int
}

// NewISCSIClient creates an iSCSI client bound to a node.
func NewISCSIClient(node *Node) *ISCSIClient {
	return &ISCSIClient{Node: node}
}

// Discover runs iSCSI SendTargets discovery and returns discovered IQNs.
func (c *ISCSIClient) Discover(ctx context.Context, host string, port int) ([]string, error) {
	c.TargetHost = host
	c.TargetPort = port

	cmd := fmt.Sprintf("iscsiadm -m discovery -t sendtargets -p %s:%d", host, port)
	stdout, stderr, code, err := c.Node.RunRoot(ctx, cmd)
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
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			iqns = append(iqns, parts[1])
		}
	}

	for _, iqn := range iqns {
		c.fixNodePortal(ctx, iqn, host, port)
	}

	return iqns, nil
}

func (c *ISCSIClient) fixNodePortal(ctx context.Context, iqn, host string, port int) {
	stdout, _, _, _ := c.Node.RunRoot(ctx,
		fmt.Sprintf("iscsiadm -m node -T %s 2>/dev/null", iqn))

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

	c.Node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -o delete 2>/dev/null", iqn))
	portal := fmt.Sprintf("%s:%d", host, port)
	c.Node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -p %s -o new 2>/dev/null", iqn, portal))
}

// Login connects to the target and returns the device path (e.g. /dev/sda).
func (c *ISCSIClient) Login(ctx context.Context, iqn string) (string, error) {
	var cmd string
	if c.TargetHost != "" && c.TargetHost != "127.0.0.1" && c.TargetHost != "localhost" {
		portal := fmt.Sprintf("%s:%d", c.TargetHost, c.TargetPort)
		cmd = fmt.Sprintf("iscsiadm -m node -T %s -p %s --login", iqn, portal)
	} else {
		cmd = fmt.Sprintf("iscsiadm -m node -T %s --login", iqn)
	}
	_, stderr, code, err := c.Node.RunRoot(ctx, cmd)
	if err != nil {
		return "", fmt.Errorf("login error: %w", err)
	}
	if code != 0 {
		return "", fmt.Errorf("login failed (code %d): %s", code, stderr)
	}

	return c.waitForDevice(ctx, iqn)
}

// Logout disconnects from the target.
func (c *ISCSIClient) Logout(ctx context.Context, iqn string) error {
	cmd := fmt.Sprintf("iscsiadm -m node -T %s --logout", iqn)
	_, stderr, code, err := c.Node.RunRoot(ctx, cmd)
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

		stdout, _, code, _ := c.Node.RunRoot(ctx, "iscsiadm -m session -P3")
		if code == 0 {
			dev := ParseDeviceFromSession(stdout, iqn)
			if dev != "" {
				return dev, nil
			}
		}

		if !rescanned && time.Until(deadline) < 25*time.Second {
			c.Node.RunRoot(ctx, "iscsiadm -m session -R")
			rescanned = true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("device for %s did not appear within 30s", iqn)
}

// ParseDeviceFromSession extracts /dev/sdX from iscsiadm -m session -P3 output.
func ParseDeviceFromSession(output, iqn string) string {
	lines := strings.Split(output, "\n")
	inTarget := false
	for _, line := range lines {
		if strings.Contains(line, "Target: "+iqn) {
			inTarget = true
			continue
		}
		if inTarget && strings.Contains(line, "Target: ") {
			break
		}
		if inTarget && strings.Contains(line, "Attached scsi disk") {
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
func (c *ISCSIClient) WaitForSession(ctx context.Context, iqn string) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("session %s did not recover: %w", iqn, ctx.Err())
		default:
		}

		stdout, _, code, _ := c.Node.RunRoot(ctx, "iscsiadm -m session")
		if code == 0 && strings.Contains(stdout, iqn) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// CleanupAll force-logouts sessions matching the IQN prefix only.
func (c *ISCSIClient) CleanupAll(ctx context.Context, iqnPrefix string) error {
	stdout, _, _, _ := c.Node.RunRoot(ctx, "iscsiadm -m session 2>&1")
	if stdout == "" || strings.Contains(stdout, "No active sessions") {
		return nil
	}

	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, iqnPrefix) {
			continue
		}
		fields := strings.Fields(line)
		for _, f := range fields {
			if strings.HasPrefix(f, iqnPrefix) {
				c.Node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s --logout 2>/dev/null", f))
				c.Node.RunRoot(ctx, fmt.Sprintf("iscsiadm -m node -T %s -o delete 2>/dev/null", f))
			}
		}
	}
	return nil
}
