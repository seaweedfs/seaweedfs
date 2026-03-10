package infra

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
	NvmePort             int // NVMe/TCP listen port (0 = disabled)
	NQN                  string // NVMe NQN (auto-derived from IQN if empty)
	MaxConcurrentWrites  int // WAL max concurrent writes (0 = default 16)
	NvmeIOQueues         int // NVMe max IO queues (0 = default 4)
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
	VolumeSize    uint64 `json:"volume_size"`
}

// SnapshotEntry matches the JSON returned by POST /snapshot {action:"list"}.
type SnapshotEntry struct {
	ID        uint32 `json:"id"`
	BaseLSN   uint64 `json:"base_lsn"`
	CreatedAt string `json:"created_at"`
	CoWBlocks uint64 `json:"cow_blocks"`
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

// HATargetSpec holds the parameters needed to create an HATarget from YAML config.
type HATargetSpec struct {
	VolSize         string
	WALSize         string
	IQN             string
	ISCSIPort       int
	AdminPort       int
	ReplicaDataPort int
	ReplicaCtrlPort int
	RebuildPort     int
	TPGID                int
	NvmePort             int
	NQN                  string
	MaxConcurrentWrites  int
	NvmeIOQueues         int
}

// NewHATargetFromSpec creates an HATarget from an HATargetSpec and Node.
func NewHATargetFromSpec(node *Node, name string, spec HATargetSpec) *HATarget {
	volSize := spec.VolSize
	if volSize == "" {
		volSize = "100M"
	}
	walSize := spec.WALSize
	if walSize == "" {
		walSize = "64M"
	}

	cfg := TargetConfig{
		VolSize: volSize,
		WALSize: walSize,
		IQN:     spec.IQN,
		Port:    spec.ISCSIPort,
	}

	ht := NewHATarget(node, cfg, spec.AdminPort, spec.ReplicaDataPort, spec.ReplicaCtrlPort, spec.RebuildPort)
	ht.TPGID = spec.TPGID
	ht.NvmePort = spec.NvmePort
	ht.NQN = spec.NQN
	ht.MaxConcurrentWrites = spec.MaxConcurrentWrites
	ht.NvmeIOQueues = spec.NvmeIOQueues

	// Use unique file paths per target name.
	ht.BinPath = "/tmp/iscsi-target-test"
	ht.VolFile = fmt.Sprintf("/tmp/blockvol-%s.blk", name)
	ht.LogFile = fmt.Sprintf("/tmp/iscsi-target-%s.log", name)
	return ht
}

// Start overrides Target.Start to add HA-specific flags.
func (h *HATarget) Start(ctx context.Context, create bool) error {
	// Pre-flight: check if ports are already in use by another process.
	if err := h.checkPortsFree(ctx); err != nil {
		return err
	}

	// Remove old log
	h.Node.Run(ctx, fmt.Sprintf("rm -f %s", h.LogFile))

	args := fmt.Sprintf("-vol %s -addr :%d -iqn %s",
		h.VolFile, h.Config.Port, h.Config.IQN)

	if create {
		if err := h.checkDiskSpace(ctx); err != nil {
			return err
		}
		h.Node.Run(ctx, fmt.Sprintf("rm -f %s %s.wal", h.VolFile, h.VolFile))
		args += fmt.Sprintf(" -create -size %s", h.Config.VolSize)
		if h.Config.WALSize != "" {
			args += fmt.Sprintf(" -wal-size %s", h.Config.WALSize)
		}
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
	if h.NvmePort > 0 {
		args += fmt.Sprintf(" -nvme-addr :%d", h.NvmePort)
		if h.NQN != "" {
			args += fmt.Sprintf(" -nqn %s", h.NQN)
		}
	}
	if h.MaxConcurrentWrites > 0 {
		args += fmt.Sprintf(" -wal-max-concurrent-writes %d", h.MaxConcurrentWrites)
	}
	if h.NvmeIOQueues > 0 {
		args += fmt.Sprintf(" -nvme-io-queues %d", h.NvmeIOQueues)
	}

	cmd := fmt.Sprintf("setsid -f %s %s >%s 2>&1", h.BinPath, args, h.LogFile)
	_, stderr, code, err := h.Node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("start ha target: code=%d stderr=%s err=%v", code, stderr, err)
	}

	if err := h.WaitForPort(ctx); err != nil {
		return err
	}

	// Discover PID early — needed for liveness check in waitForAdminPort.
	stdout, _, _, _ := h.Node.Run(ctx, fmt.Sprintf("ps -eo pid,args | grep '%s' | grep -v grep | awk '{print $1}'", h.VolFile))
	pidStr := strings.TrimSpace(stdout)
	if idx := strings.IndexByte(pidStr, '\n'); idx > 0 {
		pidStr = pidStr[:idx]
	}
	pid := 0
	fmt.Sscanf(pidStr, "%d", &pid)
	if pid == 0 {
		return fmt.Errorf("find ha target PID: %q", pidStr)
	}
	h.Pid = pid

	if h.AdminPort > 0 {
		if err := h.waitForAdminPort(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (h *HATarget) waitForAdminPort(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Collect last 20 lines of log for diagnostics.
			logTail, _, _, _ := h.Node.Run(context.Background(),
				fmt.Sprintf("tail -20 %s 2>/dev/null", h.LogFile))
			return fmt.Errorf("wait for admin port %d: %w\nlast log:\n%s", h.AdminPort, ctx.Err(), logTail)
		default:
		}

		// Check if our process is still alive — fail fast if it crashed.
		if h.Pid > 0 {
			_, _, code, _ := h.Node.Run(ctx, fmt.Sprintf("kill -0 %d 2>/dev/null", h.Pid))
			if code != 0 {
				logTail, _, _, _ := h.Node.Run(context.Background(),
					fmt.Sprintf("tail -20 %s 2>/dev/null", h.LogFile))
				return fmt.Errorf("target process %d died before admin port %d was ready\nlast log:\n%s",
					h.Pid, h.AdminPort, logTail)
			}
		}

		stdout, _, code, _ := h.Node.Run(ctx, fmt.Sprintf("ss -tln | grep :%d", h.AdminPort))
		if code == 0 && strings.Contains(stdout, fmt.Sprintf(":%d", h.AdminPort)) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// checkPortsFree verifies required ports are not already in use by another process.
func (h *HATarget) checkPortsFree(ctx context.Context) error {
	ports := []struct {
		port int
		name string
	}{
		{h.Config.Port, "iSCSI"},
	}
	if h.AdminPort > 0 {
		ports = append(ports, struct {
			port int
			name string
		}{h.AdminPort, "admin"})
	}
	if h.ReplicaData > 0 {
		ports = append(ports, struct {
			port int
			name string
		}{h.ReplicaData, "replica-data"})
	}
	if h.ReplicaCtrl > 0 {
		ports = append(ports, struct {
			port int
			name string
		}{h.ReplicaCtrl, "replica-ctrl"})
	}
	if h.RebuildPort > 0 {
		ports = append(ports, struct {
			port int
			name string
		}{h.RebuildPort, "rebuild"})
	}
	if h.NvmePort > 0 {
		ports = append(ports, struct {
			port int
			name string
		}{h.NvmePort, "nvme"})
	}

	for _, p := range ports {
		stdout, _, code, _ := h.Node.Run(ctx, fmt.Sprintf("ss -tln | grep ':%d '", p.port))
		if code == 0 && strings.TrimSpace(stdout) != "" {
			// Port is in use — find what owns it.
			owner, _, _, _ := h.Node.Run(ctx, fmt.Sprintf(
				"ss -tlnp | grep ':%d ' | head -1", p.port))
			return fmt.Errorf("port %d (%s) already in use on %s: %s",
				p.port, p.name, h.Node.Host, strings.TrimSpace(owner))
		}
	}
	return nil
}

// checkDiskSpace verifies the target node has enough disk space for the volume + WAL.
func (h *HATarget) checkDiskSpace(ctx context.Context) error {
	return CheckDiskSpace(ctx, h.Node, h.VolFile, h.Config.VolSize, h.Config.WALSize)
}

// curlPost executes a POST via curl on the node.
func (h *HATarget) curlPost(ctx context.Context, path string, body interface{}) (int, string, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return 0, "", err
	}
	cmd := fmt.Sprintf("curl -s -w '\\n%%{http_code}' -X POST -H 'Content-Type: application/json' -d '%s' http://127.0.0.1:%d%s 2>&1",
		string(data), h.AdminPort, path)
	stdout, _, code, err := h.Node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return 0, "", fmt.Errorf("curl POST %s: code=%d err=%v stdout=%s", path, code, err, stdout)
	}
	return parseCurlOutput(stdout)
}

// curlGet executes a GET via curl on the node.
func (h *HATarget) curlGet(ctx context.Context, path string) (int, string, error) {
	cmd := fmt.Sprintf("curl -s -w '\\n%%{http_code}' http://127.0.0.1:%d%s 2>&1", h.AdminPort, path)
	stdout, _, code, err := h.Node.Run(ctx, cmd)
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

// StartRebuildClient sends POST /rebuild {action:"connect"}.
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

// CreateSnapshot sends POST /snapshot {action:"create", id:N}.
func (h *HATarget) CreateSnapshot(ctx context.Context, id uint32) error {
	code, body, err := h.curlPost(ctx, "/snapshot", map[string]interface{}{
		"action": "create",
		"id":     id,
	})
	if err != nil {
		return fmt.Errorf("create snapshot %d: %w", id, err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("create snapshot %d failed (HTTP %d): %s", id, code, body)
	}
	return nil
}

// DeleteSnapshot sends POST /snapshot {action:"delete", id:N}.
func (h *HATarget) DeleteSnapshot(ctx context.Context, id uint32) error {
	code, body, err := h.curlPost(ctx, "/snapshot", map[string]interface{}{
		"action": "delete",
		"id":     id,
	})
	if err != nil {
		return fmt.Errorf("delete snapshot %d: %w", id, err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("delete snapshot %d failed (HTTP %d): %s", id, code, body)
	}
	return nil
}

// ListSnapshots sends POST /snapshot {action:"list"} and returns the entries.
func (h *HATarget) ListSnapshots(ctx context.Context) ([]SnapshotEntry, error) {
	code, body, err := h.curlPost(ctx, "/snapshot", map[string]string{
		"action": "list",
	})
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("list snapshots failed (HTTP %d): %s", code, body)
	}
	var resp struct {
		Snapshots []SnapshotEntry `json:"snapshots"`
	}
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode snapshots: %w", err)
	}
	return resp.Snapshots, nil
}

// Resize sends POST /resize {new_size_bytes:N}.
func (h *HATarget) Resize(ctx context.Context, newSizeBytes uint64) error {
	code, body, err := h.curlPost(ctx, "/resize", map[string]interface{}{
		"new_size_bytes": newSizeBytes,
	})
	if err != nil {
		return fmt.Errorf("resize to %d: %w", newSizeBytes, err)
	}
	if code != http.StatusOK {
		return fmt.Errorf("resize to %d failed (HTTP %d): %s", newSizeBytes, code, body)
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

// HostAddr returns the target's node host (for building replica addresses).
func (h *HATarget) HostAddr() string {
	if h.Node.IsLocal {
		return "127.0.0.1"
	}
	return h.Node.Host
}
