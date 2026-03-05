//go:build integration

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// HATarget extends Target with HA-specific admin HTTP endpoints.
type HATarget struct {
	*Target
	AdminPort   int
	ReplicaData int // replica receiver data port
	ReplicaCtrl int // replica receiver ctrl port
	RebuildPort int
	TPGID       int // ALUA target port group ID (0 = omit flag)
}

// StatusResp matches the JSON returned by GET /status.
type StatusResp struct {
	Path          string `json:"path"`
	Epoch         uint64 `json:"epoch"`
	Role          string `json:"role"`
	WALHeadLSN    uint64 `json:"wal_head_lsn"`
	CheckpointLSN uint64 `json:"checkpoint_lsn"`
	HasLease      bool   `json:"has_lease"`
	Healthy       bool   `json:"healthy"`
}

// NewHATarget creates an HATarget with the given ports.
func NewHATarget(node *Node, cfg TargetConfig, adminPort, replicaData, replicaCtrl, rebuildPort int) *HATarget {
	return &HATarget{
		Target:      NewTarget(node, cfg),
		AdminPort:   adminPort,
		ReplicaData: replicaData,
		ReplicaCtrl: replicaCtrl,
		RebuildPort: rebuildPort,
	}
}

// Start overrides Target.Start to add HA-specific flags.
func (h *HATarget) Start(ctx context.Context, create bool) error {
	// Remove old log
	h.node.Run(ctx, fmt.Sprintf("rm -f %s", h.logFile))

	args := fmt.Sprintf("-vol %s -addr :%d -iqn %s",
		h.volFile, h.config.Port, h.config.IQN)

	if create {
		h.node.Run(ctx, fmt.Sprintf("rm -f %s %s.wal", h.volFile, h.volFile))
		args += fmt.Sprintf(" -create -size %s", h.config.VolSize)
	}

	if h.AdminPort > 0 {
		args += fmt.Sprintf(" -admin 0.0.0.0:%d", h.AdminPort)
	}
	if h.ReplicaData > 0 && h.ReplicaCtrl > 0 {
		args += fmt.Sprintf(" -replica-data :%d -replica-ctrl :%d", h.ReplicaData, h.ReplicaCtrl)
	}
	if h.RebuildPort > 0 {
		args += fmt.Sprintf(" -rebuild-listen :%d", h.RebuildPort)
	}
	if h.TPGID > 0 {
		args += fmt.Sprintf(" -tpg-id %d", h.TPGID)
	}

	cmd := fmt.Sprintf("setsid -f %s %s >%s 2>&1", h.binPath, args, h.logFile)
	_, stderr, code, err := h.node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("start ha target: code=%d stderr=%s err=%v", code, stderr, err)
	}

	if err := h.WaitForPort(ctx); err != nil {
		return err
	}

	// Also wait for admin port if configured
	if h.AdminPort > 0 {
		if err := h.waitForAdminPort(ctx); err != nil {
			return err
		}
	}

	// Discover PID by matching the unique volume file path (not binPath,
	// which is shared across primary/replica targets).
	stdout, _, _, _ := h.node.Run(ctx, fmt.Sprintf("ps -eo pid,args | grep '%s' | grep -v grep | awk '{print $1}'", h.volFile))
	pidStr := strings.TrimSpace(stdout)
	if idx := strings.IndexByte(pidStr, '\n'); idx > 0 {
		pidStr = pidStr[:idx]
	}
	pid := 0
	fmt.Sscanf(pidStr, "%d", &pid)
	if pid == 0 {
		return fmt.Errorf("find ha target PID: %q", pidStr)
	}
	h.pid = pid
	return nil
}

func (h *HATarget) waitForAdminPort(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for admin port %d: %w", h.AdminPort, ctx.Err())
		default:
		}
		stdout, _, code, _ := h.node.Run(ctx, fmt.Sprintf("ss -tln | grep :%d", h.AdminPort))
		if code == 0 && strings.Contains(stdout, fmt.Sprintf(":%d", h.AdminPort)) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// adminAddr returns host:port for admin requests.
func (h *HATarget) adminAddr() string {
	host := h.node.Host
	if h.node.IsLocal {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", host, h.AdminPort)
}

// curlPost executes a POST via curl on the node (works in WSL2 and remote).
// Returns HTTP status code, response body, and error.
func (h *HATarget) curlPost(ctx context.Context, path string, body interface{}) (int, string, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, "", err
	}
	cmd := fmt.Sprintf("curl -s -w '\\n%%{http_code}' -X POST -H 'Content-Type: application/json' -d '%s' http://127.0.0.1:%d%s 2>&1",
		string(data), h.AdminPort, path)
	stdout, _, code, err := h.node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return 0, "", fmt.Errorf("curl POST %s: code=%d err=%v stdout=%s", path, code, err, stdout)
	}
	return parseCurlOutput(stdout)
}

// curlGet executes a GET via curl on the node.
func (h *HATarget) curlGet(ctx context.Context, path string) (int, string, error) {
	cmd := fmt.Sprintf("curl -s -w '\\n%%{http_code}' http://127.0.0.1:%d%s 2>&1", h.AdminPort, path)
	stdout, _, code, err := h.node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return 0, "", fmt.Errorf("curl GET %s: code=%d err=%v stdout=%s", path, code, err, stdout)
	}
	return parseCurlOutput(stdout)
}

// parseCurlOutput splits curl -w '\n%{http_code}' output into body and status.
func parseCurlOutput(output string) (int, string, error) {
	output = strings.TrimSpace(output)
	idx := strings.LastIndex(output, "\n")
	if idx < 0 {
		// Single line = just status code, no body
		var code int
		if _, err := fmt.Sscanf(output, "%d", &code); err != nil {
			return 0, "", fmt.Errorf("parse curl status: %q", output)
		}
		return code, "", nil
	}
	body := output[:idx]
	var httpCode int
	if _, err := fmt.Sscanf(strings.TrimSpace(output[idx+1:]), "%d", &httpCode); err != nil {
		return 0, "", fmt.Errorf("parse curl status from %q", output[idx+1:])
	}
	return httpCode, body, nil
}

// Assign sends POST /assign to inject a role/epoch assignment.
func (h *HATarget) Assign(ctx context.Context, epoch uint64, role uint32, leaseTTLMs uint32) error {
	code, body, err := h.curlPost(ctx, "/assign", map[string]interface{}{
		"epoch":        epoch,
		"role":         role,
		"lease_ttl_ms": leaseTTLMs,
	})
	if err != nil {
		return fmt.Errorf("assign request: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("assign failed (HTTP %d): %s", code, body)
	}
	return nil
}

// AssignRaw sends POST /assign and returns HTTP status code and body.
func (h *HATarget) AssignRaw(ctx context.Context, body interface{}) (int, string, error) {
	return h.curlPost(ctx, "/assign", body)
}

// Status sends GET /status and returns the parsed response.
func (h *HATarget) Status(ctx context.Context) (*StatusResp, error) {
	code, body, err := h.curlGet(ctx, "/status")
	if err != nil {
		return nil, fmt.Errorf("status request: %w", err)
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("status failed (HTTP %d): %s", code, body)
	}
	var st StatusResp
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&st); err != nil {
		return nil, fmt.Errorf("decode status: %w", err)
	}
	return &st, nil
}

// SetReplica sends POST /replica to configure WAL shipping target.
func (h *HATarget) SetReplica(ctx context.Context, dataAddr, ctrlAddr string) error {
	code, body, err := h.curlPost(ctx, "/replica", map[string]string{
		"data_addr": dataAddr,
		"ctrl_addr": ctrlAddr,
	})
	if err != nil {
		return fmt.Errorf("replica request: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("replica failed (HTTP %d): %s", code, body)
	}
	return nil
}

// SetReplicaRaw sends POST /replica and returns HTTP status + body.
func (h *HATarget) SetReplicaRaw(ctx context.Context, body interface{}) (int, string, error) {
	return h.curlPost(ctx, "/replica", body)
}

// StartRebuildEndpoint sends POST /rebuild {action:"start"}.
func (h *HATarget) StartRebuildEndpoint(ctx context.Context, listenAddr string) error {
	code, body, err := h.curlPost(ctx, "/rebuild", map[string]string{
		"action":      "start",
		"listen_addr": listenAddr,
	})
	if err != nil {
		return fmt.Errorf("rebuild start: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("rebuild start failed (HTTP %d): %s", code, body)
	}
	return nil
}

// StartRebuildClient sends POST /rebuild {action:"connect"} to start the
// rebuild client. The client connects to the primary's rebuild server,
// streams WAL/extent data, and transitions from RoleRebuilding to RoleReplica.
// This is non-blocking on the target side; poll WaitForRole("replica") to
// check completion.
func (h *HATarget) StartRebuildClient(ctx context.Context, rebuildAddr string, epoch uint64) error {
	code, body, err := h.curlPost(ctx, "/rebuild", map[string]interface{}{
		"action":       "connect",
		"rebuild_addr": rebuildAddr,
		"epoch":        epoch,
	})
	if err != nil {
		return fmt.Errorf("rebuild connect: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("rebuild connect failed (HTTP %d): %s", code, body)
	}
	return nil
}

// StopRebuildEndpoint sends POST /rebuild {action:"stop"}.
func (h *HATarget) StopRebuildEndpoint(ctx context.Context) error {
	code, body, err := h.curlPost(ctx, "/rebuild", map[string]string{"action": "stop"})
	if err != nil {
		return fmt.Errorf("rebuild stop: %w", err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("rebuild stop failed (HTTP %d): %s", code, body)
	}
	return nil
}

// WaitForRole polls GET /status until the target reports the expected role.
func (h *HATarget) WaitForRole(ctx context.Context, expectedRole string) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for role %s: %w", expectedRole, ctx.Err())
		default:
		}
		st, err := h.Status(ctx)
		if err == nil && st.Role == expectedRole {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// WaitForLSN polls GET /status until wal_head_lsn >= minLSN.
func (h *HATarget) WaitForLSN(ctx context.Context, minLSN uint64) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for LSN >= %d: %w", minLSN, ctx.Err())
		default:
		}
		st, err := h.Status(ctx)
		if err == nil && st.WALHeadLSN >= minLSN {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
}
