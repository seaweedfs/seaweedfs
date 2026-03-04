//go:build integration

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// requireCmd skips the test if cmd is not available on clientNode.
func requireCmd(t *testing.T, cmd string) {
	t.Helper()
	if !clientNode.HasCommand(cmd) {
		t.Skipf("%s not available", cmd)
	}
}

// injectNetem adds a netem delay on the node's outbound traffic to targetIP.
// Returns a cleanup function that removes the qdisc.
// Requires tc (iproute2) and root access.
func injectNetem(ctx context.Context, node *Node, targetIP string, delayMs int) (cleanup func(), err error) {
	// Find the interface routing to targetIP
	iface, _, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"ip route get %s | head -1 | awk '{for(i=1;i<=NF;i++) if($i==\"dev\") print $(i+1)}'", targetIP))
	iface = strings.TrimSpace(iface)
	if err != nil || code != 0 || iface == "" {
		return nil, fmt.Errorf("find interface for %s: iface=%q code=%d err=%v", targetIP, iface, code, err)
	}

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"tc qdisc add dev %s root netem delay %dms", iface, delayMs))
	if err != nil || code != 0 {
		return nil, fmt.Errorf("tc qdisc add: code=%d stderr=%s err=%v", code, stderr, err)
	}

	cleanup = func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		node.RunRoot(cctx, fmt.Sprintf("tc qdisc del dev %s root 2>/dev/null", iface))
	}
	return cleanup, nil
}

// injectIptablesDrop blocks outbound TCP traffic from node to targetIP on the given ports.
// Returns a cleanup function that removes the iptables rules.
func injectIptablesDrop(ctx context.Context, node *Node, targetIP string, ports []int) (cleanup func(), err error) {
	for _, port := range ports {
		_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
			"iptables -A OUTPUT -d %s -p tcp --dport %d -j DROP", targetIP, port))
		if err != nil || code != 0 {
			// Rollback already-added rules
			for _, p2 := range ports {
				if p2 == port {
					break
				}
				node.RunRoot(ctx, fmt.Sprintf(
					"iptables -D OUTPUT -d %s -p tcp --dport %d -j DROP 2>/dev/null", targetIP, p2))
			}
			return nil, fmt.Errorf("iptables add port %d: code=%d stderr=%s err=%v", port, code, stderr, err)
		}
	}

	cleanup = func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, port := range ports {
			node.RunRoot(cctx, fmt.Sprintf(
				"iptables -D OUTPUT -d %s -p tcp --dport %d -j DROP 2>/dev/null", targetIP, port))
		}
	}
	return cleanup, nil
}

// fillDisk fills the filesystem at dir, leaving ~4MB free.
// Returns a cleanup function that removes the fill file.
func fillDisk(ctx context.Context, node *Node, dir string) (cleanup func(), err error) {
	// Get available space in MB
	stdout, _, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"df -BM --output=avail %s | tail -1 | tr -d ' M'", dir))
	if err != nil || code != 0 {
		return nil, fmt.Errorf("df: code=%d err=%v", code, err)
	}
	availMB := 0
	fmt.Sscanf(strings.TrimSpace(stdout), "%d", &availMB)
	if availMB < 8 {
		return nil, fmt.Errorf("not enough space to fill: %dMB available", availMB)
	}
	fillMB := availMB - 4 // leave 4MB

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"dd if=/dev/zero of=%s/fillfile bs=1M count=%d 2>/dev/null", dir, fillMB))
	if err != nil || code != 0 {
		// dd may return non-zero on ENOSPC which is expected; check if file was created
		stdout2, _, _, _ := node.RunRoot(ctx, fmt.Sprintf("test -f %s/fillfile && echo ok", dir))
		if !strings.Contains(stdout2, "ok") {
			return nil, fmt.Errorf("fillDisk dd: code=%d stderr=%s err=%v", code, stderr, err)
		}
	}

	cleanup = func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		node.RunRoot(cctx, fmt.Sprintf("rm -f %s/fillfile", dir))
	}
	return cleanup, nil
}

// corruptWALRegion overwrites nBytes within the WAL section of the volume file.
// The WAL is embedded in the volume file starting at offset 4096 (SuperblockSize).
// We corrupt near the end of the WAL region to simulate torn writes.
func corruptWALRegion(ctx context.Context, node *Node, volPath string, nBytes int) error {
	const walOffset = 4096 // SuperblockSize — WAL starts here

	// Get file size to determine WAL region extent
	stdout, _, code, err := node.RunRoot(ctx, fmt.Sprintf("stat -c %%s %s", volPath))
	if err != nil || code != 0 {
		return fmt.Errorf("stat %s: code=%d err=%v", volPath, code, err)
	}
	fileSize := 0
	fmt.Sscanf(strings.TrimSpace(stdout), "%d", &fileSize)

	// WAL region is from walOffset to walOffset + walSize.
	// For a 50M vol with default 64M WAL, WAL extends from 4096 to ~67M.
	// Corrupt nBytes at a position 1/3 into the WAL region (where recent writes live).
	walEnd := walOffset + 64*1024*1024 // default 64MB WAL
	if walEnd > fileSize {
		walEnd = fileSize
	}
	walUsable := walEnd - walOffset
	if walUsable < nBytes*2 {
		return fmt.Errorf("WAL region too small: %d", walUsable)
	}
	// Corrupt near the middle of the WAL region
	seekPos := walOffset + walUsable/3

	_, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf(
		"python3 -c \"import sys; sys.stdout.buffer.write(b'\\xff'*%d)\" | dd of=%s bs=1 seek=%d conv=notrunc 2>/dev/null",
		nBytes, volPath, seekPos))
	if err != nil || code != 0 {
		return fmt.Errorf("corrupt WAL region: code=%d stderr=%s err=%v", code, stderr, err)
	}
	return nil
}

