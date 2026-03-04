package csi

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ISCSIUtil provides iSCSI initiator operations.
type ISCSIUtil interface {
	Discovery(ctx context.Context, portal string) error
	Login(ctx context.Context, iqn, portal string) error
	Logout(ctx context.Context, iqn string) error
	GetDeviceByIQN(ctx context.Context, iqn string) (string, error)
	IsLoggedIn(ctx context.Context, iqn string) (bool, error)
}

// MountUtil provides filesystem mount operations.
type MountUtil interface {
	FormatAndMount(ctx context.Context, device, target, fsType string) error
	Mount(ctx context.Context, source, target, fsType string, readOnly bool) error
	BindMount(ctx context.Context, source, target string, readOnly bool) error
	Unmount(ctx context.Context, target string) error
	IsFormatted(ctx context.Context, device string) (bool, error)
	IsMounted(ctx context.Context, target string) (bool, error)
}

// realISCSIUtil uses iscsiadm CLI.
type realISCSIUtil struct{}

func (r *realISCSIUtil) Discovery(ctx context.Context, portal string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "discovery", "-t", "sendtargets", "-p", portal)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iscsiadm discovery: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) Login(ctx context.Context, iqn, portal string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "-p", portal, "--login")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iscsiadm login: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) Logout(ctx context.Context, iqn string) error {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "node", "-T", iqn, "--logout")
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Treat "not logged in" as success.
		if strings.Contains(string(out), "No matching sessions") {
			return nil
		}
		return fmt.Errorf("iscsiadm logout: %s: %w", string(out), err)
	}
	return nil
}

func (r *realISCSIUtil) GetDeviceByIQN(ctx context.Context, iqn string) (string, error) {
	// Poll for device to appear (iSCSI login is async).
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for device for IQN %s", iqn)
		case <-ticker.C:
			// Look for block device symlinks under /dev/disk/by-path/
			pattern := fmt.Sprintf("/dev/disk/by-path/*%s*", iqn)
			matches, err := filepath.Glob(pattern)
			if err != nil {
				continue
			}
			for _, m := range matches {
				// Skip partitions.
				if strings.Contains(m, "-part") {
					continue
				}
				// Resolve symlink to actual device.
				dev, err := filepath.EvalSymlinks(m)
				if err != nil {
					continue
				}
				return dev, nil
			}
		}
	}
}

func (r *realISCSIUtil) IsLoggedIn(ctx context.Context, iqn string) (bool, error) {
	cmd := exec.CommandContext(ctx, "iscsiadm", "-m", "session")
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Exit code 21 = no sessions, not an error.
		outStr := string(out)
		if strings.Contains(outStr, "No active sessions") {
			return false, nil
		}
		// Also handle exit code 21 directly (nsenter may suppress output).
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 21 {
			return false, nil
		}
		return false, fmt.Errorf("iscsiadm session: %s: %w", outStr, err)
	}
	return strings.Contains(string(out), iqn), nil
}

// realMountUtil uses mount/umount/mkfs CLI.
type realMountUtil struct{}

func (r *realMountUtil) FormatAndMount(ctx context.Context, device, target, fsType string) error {
	formatted, err := r.IsFormatted(ctx, device)
	if err != nil {
		return err
	}
	if !formatted {
		cmd := exec.CommandContext(ctx, "mkfs."+fsType, device)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("mkfs.%s: %s: %w", fsType, string(out), err)
		}
	}
	return r.Mount(ctx, device, target, fsType, false)
}

func (r *realMountUtil) Mount(ctx context.Context, source, target, fsType string, readOnly bool) error {
	args := []string{"-t", fsType}
	if readOnly {
		args = append(args, "-o", "ro")
	}
	args = append(args, source, target)
	cmd := exec.CommandContext(ctx, "mount", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount: %s: %w", string(out), err)
	}
	return nil
}

func (r *realMountUtil) BindMount(ctx context.Context, source, target string, readOnly bool) error {
	args := []string{"--bind", source, target}
	cmd := exec.CommandContext(ctx, "mount", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bind mount: %s: %w", string(out), err)
	}
	if readOnly {
		cmd = exec.CommandContext(ctx, "mount", "-o", "remount,bind,ro", target)
		out, err = cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("remount ro: %s: %w", string(out), err)
		}
	}
	return nil
}

func (r *realMountUtil) Unmount(ctx context.Context, target string) error {
	cmd := exec.CommandContext(ctx, "umount", target)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Treat "not mounted" as success.
		if strings.Contains(string(out), "not mounted") {
			return nil
		}
		return fmt.Errorf("umount: %s: %w", string(out), err)
	}
	return nil
}

func (r *realMountUtil) IsFormatted(ctx context.Context, device string) (bool, error) {
	cmd := exec.CommandContext(ctx, "blkid", "-p", device)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Exit code 2 = no filesystem found.
		if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == 2 {
			return false, nil
		}
		return false, fmt.Errorf("blkid: %s: %w", string(out), err)
	}
	return strings.Contains(string(out), "TYPE="), nil
}

func (r *realMountUtil) IsMounted(ctx context.Context, target string) (bool, error) {
	cmd := exec.CommandContext(ctx, "mountpoint", "-q", target)
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	// Non-zero exit = not a mount point.
	return false, nil
}

// mockISCSIUtil is a test double for ISCSIUtil.
type mockISCSIUtil struct {
	discoveryErr    error
	loginErr        error
	logoutErr       error
	getDeviceResult string
	getDeviceErr    error
	loggedIn        map[string]bool
	calls           []string
}

func newMockISCSIUtil() *mockISCSIUtil {
	return &mockISCSIUtil{loggedIn: make(map[string]bool)}
}

func (m *mockISCSIUtil) Discovery(_ context.Context, portal string) error {
	m.calls = append(m.calls, "discovery:"+portal)
	return m.discoveryErr
}

func (m *mockISCSIUtil) Login(_ context.Context, iqn, portal string) error {
	m.calls = append(m.calls, "login:"+iqn)
	if m.loginErr != nil {
		return m.loginErr
	}
	m.loggedIn[iqn] = true
	return nil
}

func (m *mockISCSIUtil) Logout(_ context.Context, iqn string) error {
	m.calls = append(m.calls, "logout:"+iqn)
	if m.logoutErr != nil {
		return m.logoutErr
	}
	delete(m.loggedIn, iqn)
	return nil
}

func (m *mockISCSIUtil) GetDeviceByIQN(_ context.Context, iqn string) (string, error) {
	m.calls = append(m.calls, "getdevice:"+iqn)
	return m.getDeviceResult, m.getDeviceErr
}

func (m *mockISCSIUtil) IsLoggedIn(_ context.Context, iqn string) (bool, error) {
	return m.loggedIn[iqn], nil
}

// mockMountUtil is a test double for MountUtil.
type mockMountUtil struct {
	formatAndMountErr error
	mountErr          error
	unmountErr        error
	isFormattedResult bool
	isMountedTargets  map[string]bool
	calls             []string
}

func newMockMountUtil() *mockMountUtil {
	return &mockMountUtil{isMountedTargets: make(map[string]bool)}
}

func (m *mockMountUtil) FormatAndMount(_ context.Context, device, target, fsType string) error {
	m.calls = append(m.calls, "formatandmount:"+device+":"+target)
	if m.formatAndMountErr != nil {
		return m.formatAndMountErr
	}
	m.isMountedTargets[target] = true
	return nil
}

func (m *mockMountUtil) Mount(_ context.Context, source, target, fsType string, readOnly bool) error {
	m.calls = append(m.calls, "mount:"+source+":"+target)
	if m.mountErr != nil {
		return m.mountErr
	}
	m.isMountedTargets[target] = true
	return nil
}

func (m *mockMountUtil) BindMount(_ context.Context, source, target string, readOnly bool) error {
	m.calls = append(m.calls, "bindmount:"+source+":"+target)
	if m.mountErr != nil {
		return m.mountErr
	}
	m.isMountedTargets[target] = true
	return nil
}

func (m *mockMountUtil) Unmount(_ context.Context, target string) error {
	m.calls = append(m.calls, "unmount:"+target)
	if m.unmountErr != nil {
		return m.unmountErr
	}
	delete(m.isMountedTargets, target)
	return nil
}

func (m *mockMountUtil) IsFormatted(_ context.Context, device string) (bool, error) {
	return m.isFormattedResult, nil
}

func (m *mockMountUtil) IsMounted(_ context.Context, target string) (bool, error) {
	return m.isMountedTargets[target], nil
}
