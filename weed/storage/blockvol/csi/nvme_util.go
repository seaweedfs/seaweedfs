package csi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// NVMeUtil provides NVMe/TCP initiator operations.
type NVMeUtil interface {
	Connect(ctx context.Context, nqn, addr string) error
	Disconnect(ctx context.Context, nqn string) error
	IsConnected(ctx context.Context, nqn string) (bool, error)
	GetDeviceByNQN(ctx context.Context, nqn string) (string, error)
	GetControllerByNQN(ctx context.Context, nqn string) (string, error)
	Rescan(ctx context.Context, nqn string) error
	IsNVMeTCPAvailable() bool
}

// realNVMeUtil uses nvme-cli commands.
type realNVMeUtil struct{}

func (r *realNVMeUtil) Connect(ctx context.Context, nqn, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("nvme connect: invalid addr %q: %w", addr, err)
	}
	cmd := exec.CommandContext(ctx, "nvme", "connect", "-t", "tcp", "-n", nqn, "-a", host, "-s", port)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Treat "already connected" as success (idempotent).
		if strings.Contains(string(out), "already connected") {
			return nil
		}
		return fmt.Errorf("nvme connect: %s: %w", string(out), err)
	}
	return nil
}

func (r *realNVMeUtil) Disconnect(ctx context.Context, nqn string) error {
	cmd := exec.CommandContext(ctx, "nvme", "disconnect", "-n", nqn)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Treat "not connected" / "no subsystem" as success (idempotent).
		outStr := string(out)
		if strings.Contains(outStr, "not connected") || strings.Contains(outStr, "No subsystemtype") || strings.Contains(outStr, "Invalid argument") {
			return nil
		}
		return fmt.Errorf("nvme disconnect: %s: %w", outStr, err)
	}
	return nil
}

func (r *realNVMeUtil) IsConnected(ctx context.Context, nqn string) (bool, error) {
	_, _, err := r.findSubsys(ctx, nqn)
	if err != nil {
		if errors.Is(err, errNQNNotFound) {
			return false, nil // NQN not present = not connected
		}
		return false, err // command/parse failure — propagate
	}
	return true, nil
}

// errNQNNotFound is returned by findSubsys when the NQN is not in the subsystem list.
// Callers use errors.Is to distinguish "not found" from command/parse errors.
var errNQNNotFound = errors.New("nvme: NQN not found")

func (r *realNVMeUtil) GetDeviceByNQN(ctx context.Context, nqn string) (string, error) {
	// Poll for device to appear (NVMe connect + device enumeration is async).
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline:
			return "", fmt.Errorf("timeout waiting for NVMe device for NQN %s", nqn)
		case <-ticker.C:
			_, dev, err := r.findSubsys(ctx, nqn)
			if err != nil {
				continue
			}
			if dev != "" {
				return dev, nil
			}
		}
	}
}

func (r *realNVMeUtil) GetControllerByNQN(ctx context.Context, nqn string) (string, error) {
	ctrl, _, err := r.findSubsys(ctx, nqn)
	if err != nil {
		return "", err
	}
	if ctrl == "" {
		return "", fmt.Errorf("no controller found for NQN %s", nqn)
	}
	return ctrl, nil
}

func (r *realNVMeUtil) Rescan(ctx context.Context, nqn string) error {
	ctrl, err := r.GetControllerByNQN(ctx, nqn)
	if err != nil {
		return fmt.Errorf("nvme rescan: find controller: %w", err)
	}
	cmd := exec.CommandContext(ctx, "nvme", "ns-rescan", ctrl)
	out, errCmd := cmd.CombinedOutput()
	if errCmd != nil {
		return fmt.Errorf("nvme ns-rescan %s: %s: %w", ctrl, string(out), errCmd)
	}
	return nil
}

// IsNVMeTCPAvailable checks if the nvme_tcp kernel module is loaded (read-only).
func (r *realNVMeUtil) IsNVMeTCPAvailable() bool {
	_, err := os.Stat("/sys/module/nvme_tcp")
	return err == nil
}

// nvmeListSubsysOutput represents the JSON output from `nvme list-subsys -o json`.
type nvmeListSubsysOutput struct {
	Subsystems []nvmeSubsys `json:"Subsystems"`
}

type nvmeSubsys struct {
	NQN   string      `json:"NQN"`
	Paths []nvmePath  `json:"Paths"`
	// Some nvme-cli versions use "Namespaces" instead.
}

type nvmePath struct {
	Name      string `json:"Name"`       // controller name, e.g. "nvme0"
	Transport string `json:"Transport"`
	State     string `json:"State"`
}

// findSubsys parses `nvme list-subsys -o json` to find controller and namespace device
// for a given NQN. Returns (controller path, namespace device path, error).
// Returns errNQNNotFound (sentinel) when the NQN is absent from the subsystem list.
// Returns a non-sentinel error for command execution or JSON parse failures.
func (r *realNVMeUtil) findSubsys(ctx context.Context, nqn string) (string, string, error) {
	cmd := exec.CommandContext(ctx, "nvme", "list-subsys", "-o", "json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("nvme list-subsys: %s: %w", string(out), err)
	}

	var parsed nvmeListSubsysOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		return "", "", fmt.Errorf("nvme list-subsys: parse json: %w", err)
	}

	for _, ss := range parsed.Subsystems {
		if ss.NQN != nqn {
			continue
		}
		// Prefer a live TCP path. Fall back to any path with a name.
		var fallbackCtrl string
		for _, p := range ss.Paths {
			if p.Name == "" {
				continue
			}
			ctrl := "/dev/" + p.Name
			dev := ctrl + "n1"
			// Prefer Transport=tcp + State=live.
			if strings.EqualFold(p.Transport, "tcp") && strings.EqualFold(p.State, "live") {
				return ctrl, dev, nil
			}
			if fallbackCtrl == "" {
				fallbackCtrl = ctrl
			}
		}
		if fallbackCtrl != "" {
			return fallbackCtrl, fallbackCtrl + "n1", nil
		}
		return "", "", fmt.Errorf("NQN %s found but no controller paths", nqn)
	}
	return "", "", errNQNNotFound
}

// mockNVMeUtil is a test double for NVMeUtil.
type mockNVMeUtil struct {
	connectErr         error
	disconnectErr      error
	getDeviceResult    string
	getDeviceErr       error
	getControllerResult string
	getControllerErr   error
	rescanErr          error
	nvmeTCPAvailable   bool
	connected          map[string]bool
	calls              []string
}

func newMockNVMeUtil() *mockNVMeUtil {
	return &mockNVMeUtil{connected: make(map[string]bool)}
}

func (m *mockNVMeUtil) Connect(_ context.Context, nqn, addr string) error {
	m.calls = append(m.calls, "connect:"+nqn+":"+addr)
	if m.connectErr != nil {
		return m.connectErr
	}
	m.connected[nqn] = true
	return nil
}

func (m *mockNVMeUtil) Disconnect(_ context.Context, nqn string) error {
	m.calls = append(m.calls, "disconnect:"+nqn)
	if m.disconnectErr != nil {
		return m.disconnectErr
	}
	delete(m.connected, nqn)
	return nil
}

func (m *mockNVMeUtil) IsConnected(_ context.Context, nqn string) (bool, error) {
	return m.connected[nqn], nil
}

func (m *mockNVMeUtil) GetDeviceByNQN(_ context.Context, nqn string) (string, error) {
	m.calls = append(m.calls, "getdevice:"+nqn)
	return m.getDeviceResult, m.getDeviceErr
}

func (m *mockNVMeUtil) GetControllerByNQN(_ context.Context, nqn string) (string, error) {
	m.calls = append(m.calls, "getcontroller:"+nqn)
	return m.getControllerResult, m.getControllerErr
}

func (m *mockNVMeUtil) Rescan(_ context.Context, nqn string) error {
	m.calls = append(m.calls, "rescan:"+nqn)
	return m.rescanErr
}

func (m *mockNVMeUtil) IsNVMeTCPAvailable() bool {
	return m.nvmeTCPAvailable
}
