package infra

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// InjectNetem adds a netem delay on the node's outbound traffic to targetIP.
// Returns the cleanup command string (for storage in vars).
func InjectNetem(ctx context.Context, node *Node, targetIP string, delayMs int) (cleanupCmd string, err error) {
	iface, _, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"ip route get %s | head -1 | awk '{for(i=1;i<=NF;i++) if($i==\"dev\") print $(i+1)}'", targetIP))
	iface = strings.TrimSpace(iface)
	if err != nil || code != 0 || iface == "" {
		return "", fmt.Errorf("find interface for %s: iface=%q code=%d err=%v", targetIP, iface, code, err)
	}

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"tc qdisc add dev %s root netem delay %dms", iface, delayMs))
	if err != nil || code != 0 {
		return "", fmt.Errorf("tc qdisc add: code=%d stderr=%s err=%v", code, stderr, err)
	}

	cleanupCmd = fmt.Sprintf("tc qdisc del dev %s root 2>/dev/null", iface)
	return cleanupCmd, nil
}

// InjectIptablesDrop blocks outbound TCP traffic from node to targetIP on the given ports.
// Returns the cleanup command string.
func InjectIptablesDrop(ctx context.Context, node *Node, targetIP string, ports []int) (cleanupCmd string, err error) {
	for i, port := range ports {
		_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
			"iptables -A OUTPUT -d %s -p tcp --dport %d -j DROP", targetIP, port))
		if err != nil || code != 0 {
			// Rollback already-added rules
			for j := 0; j < i; j++ {
				node.RunRoot(ctx, fmt.Sprintf(
					"iptables -D OUTPUT -d %s -p tcp --dport %d -j DROP 2>/dev/null", targetIP, ports[j]))
			}
			return "", fmt.Errorf("iptables add port %d: code=%d stderr=%s err=%v", port, code, stderr, err)
		}
	}

	// Build cleanup command that removes all rules.
	// Use "|| true" so each removal succeeds even if the rule is already gone,
	// and ";" so all ports are attempted even if one fails.
	var cmds []string
	for _, port := range ports {
		cmds = append(cmds, fmt.Sprintf(
			"iptables -D OUTPUT -d %s -p tcp --dport %d -j DROP 2>/dev/null || true", targetIP, port))
	}
	cleanupCmd = strings.Join(cmds, " ; ")
	return cleanupCmd, nil
}

// FillDisk fills the filesystem at dir, leaving ~4MB free.
// Returns the cleanup command string.
func FillDisk(ctx context.Context, node *Node, dir string) (cleanupCmd string, err error) {
	stdout, _, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"df -BM --output=avail %s | tail -1 | tr -d ' M'", dir))
	if err != nil || code != 0 {
		return "", fmt.Errorf("df: code=%d err=%v", code, err)
	}
	availMB := 0
	fmt.Sscanf(strings.TrimSpace(stdout), "%d", &availMB)
	if availMB < 8 {
		return "", fmt.Errorf("not enough space to fill: %dMB available", availMB)
	}
	fillMB := availMB - 4

	// Use fallocate (instant) instead of dd (linear time).
	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"fallocate -l %dM %s/fillfile", fillMB, dir))
	if err != nil || code != 0 {
		// Fallback to dd if fallocate not available.
		_, stderr, code, err = node.RunRoot(ctx, fmt.Sprintf(
			"dd if=/dev/zero of=%s/fillfile bs=1M count=%d 2>/dev/null", dir, fillMB))
		if err != nil || code != 0 {
			stdout2, _, _, _ := node.RunRoot(ctx, fmt.Sprintf("test -f %s/fillfile && echo ok", dir))
			if !strings.Contains(stdout2, "ok") {
				return "", fmt.Errorf("fillDisk: code=%d stderr=%s err=%v", code, stderr, err)
			}
		}
	}

	cleanupCmd = fmt.Sprintf("rm -f %s/fillfile", dir)
	return cleanupCmd, nil
}

// CorruptWALRegion overwrites nBytes within the WAL section of the volume file.
func CorruptWALRegion(ctx context.Context, node *Node, volPath string, nBytes int) error {
	const walOffset = 4096 // SuperblockSize

	stdout, _, code, err := node.RunRoot(ctx, fmt.Sprintf("stat -c %%s %s", volPath))
	if err != nil || code != 0 {
		return fmt.Errorf("stat %s: code=%d err=%v", volPath, code, err)
	}
	fileSize := 0
	fmt.Sscanf(strings.TrimSpace(stdout), "%d", &fileSize)

	walEnd := walOffset + 64*1024*1024
	if walEnd > fileSize {
		walEnd = fileSize
	}
	walUsable := walEnd - walOffset
	if walUsable < nBytes*2 {
		return fmt.Errorf("WAL region too small: %d", walUsable)
	}
	seekPos := walOffset + walUsable/3

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"python3 -c \"import sys; sys.stdout.buffer.write(b'\\xff'*%d)\" | dd of=%s bs=1 seek=%d conv=notrunc 2>/dev/null",
		nBytes, volPath, seekPos))
	if err != nil || code != 0 {
		return fmt.Errorf("corrupt WAL region: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return nil
}

// ClearFault executes a cleanup command stored in vars.
func ClearFault(ctx context.Context, node *Node, cleanupCmd string) error {
	if cleanupCmd == "" {
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, stderr, code, err := node.RunRoot(cctx, cleanupCmd)
	if err != nil || code != 0 {
		return fmt.Errorf("clear fault: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return nil
}
