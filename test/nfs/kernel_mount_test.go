//go:build linux

package nfs

// End-to-end mount tests that drive the real Linux NFS client (mount.nfs +
// in-tree kernel) against a running `weed nfs` subprocess. These exist to
// catch regressions that the existing framework can't see, because the
// framework drives the server with willscott/go-nfs-client — the same RPC
// library the server uses internally — so any bug shared between the two
// (XDR layout, version dispatch, RPC framing) round-trips invisibly.
//
// Two real bugs hit recently were exactly that shape:
//   1. NFSv4 mis-routed to the v3 SETATTR handler (#9262). The client
//      library never sends NFSv4, so the test suite never noticed; the
//      Linux kernel mount path did notice, with EIO.
//   2. UDP MOUNT v3 missing. Only TCP MOUNT was advertised; the kernel
//      defaults mountproto=udp in many setups, so the in-tree client
//      surfaced EPROTONOSUPPORT during MOUNT setup.
//
// These tests mount over the actual loopback interface using mount.nfs and
// shell out to /bin/mount and /bin/umount. They require root (mount(2) is
// privileged) and Linux (the in-tree NFS client is what's being exercised);
// they t.Skip cleanly when either prerequisite is missing.
//
// Run locally with:
//
//	cd test/nfs
//	sudo go test -v -run TestKernelMount ./...
//
// CI runs them via .github/workflows/nfs-tests.yml after installing
// nfs-common (mount.nfs + helpers).

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// kernelMountSkipIfUnsupported skips the test when the host can't run a
// real NFS mount. The combined check belongs in one place so the three
// kernel-mount tests stay focused on what they're actually verifying.
func kernelMountSkipIfUnsupported(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("kernel mount test requires root; mount(2) is privileged")
	}
	if _, err := exec.LookPath("mount.nfs"); err != nil {
		t.Skipf("mount.nfs not installed: %v (CI installs the nfs-common package)", err)
	}
}

// kernelMount runs /bin/mount with the given options against the framework's
// running NFS server, returns the mountpoint and an unmount closure. We pass
// explicit port=/mountport= options so the kernel never queries portmap.
// That keeps the harness honest about what it's testing — the NFS / MOUNT
// wire protocol — and avoids colliding with a system rpcbind on shared CI
// runners (port 111 is privileged and frequently in use already).
func kernelMount(t *testing.T, fw *NfsTestFramework, optsTemplate string) (string, func()) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(fw.NfsAddr())
	if err != nil {
		t.Fatalf("split nfs addr %q: %v", fw.NfsAddr(), err)
	}
	mountpoint, err := os.MkdirTemp("", "weed-nfs-kmount-")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	opts := strings.ReplaceAll(optsTemplate, "{port}", portStr)
	target := fmt.Sprintf("%s:%s", host, fw.ExportRoot())
	cmd := exec.Command("mount", "-t", "nfs", "-o", opts, target, mountpoint)
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(mountpoint)
		t.Fatalf("mount %s -o %s failed: %v\nmount output:\n%s", target, opts, err, out)
	}
	teardown := func() {
		// -f to bail out faster if the server's already gone.
		_ = exec.Command("umount", "-f", mountpoint).Run()
		_ = os.RemoveAll(mountpoint)
	}
	return mountpoint, teardown
}

func newKernelMountFramework(t *testing.T) *NfsTestFramework {
	t.Helper()
	cfg := DefaultTestConfig()
	fw := NewNfsTestFramework(t, cfg)
	if err := fw.Setup(cfg); err != nil {
		fw.Cleanup()
		t.Fatalf("framework setup: %v", err)
	}
	t.Cleanup(fw.Cleanup)
	return fw
}

// TestKernelMountV3TCP exercises the most common mount form: NFSv3 + MOUNT
// v3, both over TCP. This is what the existing go-nfs-client tests cover at
// the protocol layer, but running it through mount.nfs and the kernel
// confirms that the wire format we emit decodes cleanly under a different
// XDR/RPC parser.
func TestKernelMountV3TCP(t *testing.T) {
	kernelMountSkipIfUnsupported(t)
	fw := newKernelMountFramework(t)

	mountpoint, undo := kernelMount(t, fw,
		"nfsvers=3,nolock,port={port},mountport={port},proto=tcp,mountproto=tcp")
	defer undo()

	if _, err := os.Stat(mountpoint); err != nil {
		t.Errorf("stat mountpoint: %v", err)
	}
	if _, err := os.ReadDir(mountpoint); err != nil {
		t.Errorf("readdir mountpoint: %v", err)
	}
}

// TestKernelMountV3MountProtoUDP is the regression test for the UDP MOUNT
// v3 responder. mountproto=udp forces the kernel to call MOUNT over UDP
// only; before the responder existed the kernel hit nothing (MOUNT was
// advertised TCP-only) and surfaced EPROTONOSUPPORT during mount setup.
func TestKernelMountV3MountProtoUDP(t *testing.T) {
	kernelMountSkipIfUnsupported(t)
	fw := newKernelMountFramework(t)

	mountpoint, undo := kernelMount(t, fw,
		"nfsvers=3,nolock,port={port},mountport={port},proto=tcp,mountproto=udp")
	defer undo()

	if _, err := os.Stat(mountpoint); err != nil {
		t.Errorf("stat mountpoint: %v", err)
	}
}

// TestKernelMountV4RejectsCleanly is the regression test for the NFSv4
// PROG_MISMATCH path (#9262). The server only speaks NFSv3, but the
// previous behaviour was to mis-route v4 COMPOUND to the v3 SETATTR
// handler and write garbage; the kernel surfaced EIO instead of a
// version-mismatch error and (depending on distro) didn't fall back to
// v3. The version filter now answers PROG_MISMATCH so the kernel sees
// "v4 not supported" cleanly.
//
// The test asserts:
//   1. mount.nfs exits non-zero (no silent success against a v3 server);
//   2. the failure message mentions protocol/version/io, which is what the
//      kernel surfaces when it gets PROG_MISMATCH instead of garbage. A
//      pre-fix server returns "mount system call failed" with no further
//      context, so a regression collapses the assertion onto that branch.
func TestKernelMountV4RejectsCleanly(t *testing.T) {
	kernelMountSkipIfUnsupported(t)
	fw := newKernelMountFramework(t)

	host, portStr, err := net.SplitHostPort(fw.NfsAddr())
	if err != nil {
		t.Fatalf("split nfs addr: %v", err)
	}
	mountpoint, err := os.MkdirTemp("", "weed-nfs-kmount-v4-")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	defer os.RemoveAll(mountpoint)

	target := fmt.Sprintf("%s:%s", host, fw.ExportRoot())
	cmd := exec.Command("mount", "-t", "nfs", "-o",
		fmt.Sprintf("vers=4,port=%s", portStr),
		target, mountpoint)
	out, err := cmd.CombinedOutput()
	defer exec.Command("umount", "-f", mountpoint).Run()

	if err == nil {
		t.Fatalf("v4 mount unexpectedly succeeded against v3-only server\nmount output:\n%s", out)
	}
	// Don't pin the exact error string — different distros print slightly
	// different things — but require some hint that the kernel saw a
	// protocol-level failure rather than a generic "mount system call
	// failed". Without the version filter, mount.nfs prints the latter
	// alone; with it, the former.
	lower := strings.ToLower(string(out))
	if !strings.Contains(lower, "protocol") &&
		!strings.Contains(lower, "version") &&
		!strings.Contains(lower, "i/o") {
		t.Errorf("v4 mount failure didn't mention protocol/version/io; output:\n%s", out)
	}
	// Also require a non-zero exit so a future change that makes mount(2)
	// silently succeed (e.g. by relaxing the version filter) shows up
	// here even if the message phrasing changes.
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Errorf("expected mount to exit non-zero with ExitError, got %v", err)
	}
}
