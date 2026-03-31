package testrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ClusterMode describes how the cluster was obtained.
type ClusterMode string

const (
	ClusterModeAttached ClusterMode = "attached"
	ClusterModeManaged  ClusterMode = "managed"
	ClusterModeNone     ClusterMode = "none" // no cluster spec
)

// ClusterState holds the result of cluster setup.
type ClusterState struct {
	Mode      ClusterMode
	MasterURL string
	Servers   int
	BlockCap  int
	Pids      []string // PIDs of managed processes (empty if attached)
	Dirs      []string // temp directories to clean up (managed only)
}

// ClusterManager handles attach-or-create lifecycle for test clusters.
type ClusterManager struct {
	spec          *ClusterSpec
	logFunc       func(string, ...interface{})
	state         ClusterState
	node          NodeRunner   // the node where managed processes run
	attachedNodes []NodeRunner // all nodes (for cleanup=destroy on attached clusters)
}

// NewClusterManager creates a manager for the given spec.
// If spec is nil, Setup is a no-op (backward compatible).
func NewClusterManager(spec *ClusterSpec, logFunc func(string, ...interface{})) *ClusterManager {
	return &ClusterManager{
		spec:    spec,
		logFunc: logFunc,
	}
}

// Setup tries to attach to an existing cluster, falls back to managed if needed.
// Sets master_url and cluster_* vars on the ActionContext.
func (cm *ClusterManager) Setup(ctx context.Context, actx *ActionContext) error {
	if cm.spec == nil {
		cm.state.Mode = ClusterModeNone
		return nil
	}

	masterURL := actx.Vars["master_url"]
	if masterURL == "" {
		masterURL = actx.Scenario.Env["master_url"]
	}

	fallback := cm.spec.Fallback
	if fallback == "" {
		fallback = "managed"
	}

	// Step 1: Try attach.
	if masterURL != "" {
		cm.logFunc("[cluster] trying attach to %s", masterURL)
		state, err := cm.tryAttach(ctx, masterURL)
		if err == nil && cm.meetsRequirements(state) {
			cm.state = state
			cm.state.Mode = ClusterModeAttached
			// Collect all nodes for potential cleanup=destroy.
			for _, node := range actx.Nodes {
				cm.attachedNodes = append(cm.attachedNodes, node)
			}
			cm.setVars(actx)
			cm.logFunc("[cluster] attached: servers=%d block_capable=%d", state.Servers, state.BlockCap)
			return nil
		}
		if err != nil {
			cm.logFunc("[cluster] attach failed: %v", err)
		} else {
			cm.logFunc("[cluster] attach succeeded but requirements not met: need servers>=%d block_capable>=%d, got servers=%d block_capable=%d",
				cm.spec.Require.Servers, cm.spec.Require.BlockCapable, state.Servers, state.BlockCap)
		}
	}

	// Step 2: Fallback.
	switch fallback {
	case "fail":
		return fmt.Errorf("cluster not available at %s and fallback=fail", masterURL)
	case "skip":
		cm.state.Mode = ClusterModeNone
		cm.logFunc("[cluster] skipped (fallback=skip)")
		return nil // caller should check cm.Skipped()
	case "managed":
		return cm.createManaged(ctx, actx)
	default:
		return fmt.Errorf("unknown cluster fallback %q", fallback)
	}
}

// Teardown stops managed cluster processes based on the cleanup policy.
//   - "auto" (default): tear down managed, leave attached alone.
//   - "keep": never tear down (cluster stays for next test).
//   - "destroy": always tear down (even attached — reset to clean).
func (cm *ClusterManager) Teardown(ctx context.Context) {
	cleanup := "auto"
	if cm.spec != nil && cm.spec.Cleanup != "" {
		cleanup = cm.spec.Cleanup
	}

	shouldTeardown := false
	switch cleanup {
	case "keep":
		cm.logFunc("[cluster] cleanup=keep: leaving cluster running")
		return
	case "destroy":
		shouldTeardown = true
	default: // "auto"
		shouldTeardown = (cm.state.Mode == ClusterModeManaged)
	}

	if !shouldTeardown {
		return
	}

	if len(cm.state.Pids) > 0 && cm.node != nil {
		// Managed cluster: kill tracked processes and remove dirs.
		cm.logFunc("[cluster] tearing down %s cluster (%d processes, %d dirs)", cm.state.Mode, len(cm.state.Pids), len(cm.state.Dirs))
		for _, pid := range cm.state.Pids {
			cm.node.RunRoot(ctx, fmt.Sprintf("kill -9 %s 2>/dev/null", pid))
		}
		time.Sleep(1 * time.Second)
		for _, dir := range cm.state.Dirs {
			cm.node.RunRoot(ctx, fmt.Sprintf("rm -rf %s 2>/dev/null", dir))
		}
	} else if cm.state.Mode == ClusterModeAttached && cleanup == "destroy" {
		// Attached cluster with cleanup=destroy: kill all weed processes on
		// every node in the topology. This is destructive — use only for
		// reset-to-clean scenarios.
		cm.logFunc("[cluster] cleanup=destroy on attached cluster: killing weed processes")
		for _, node := range cm.attachedNodes {
			node.RunRoot(ctx, "killall -9 weed 2>/dev/null")
		}
		time.Sleep(1 * time.Second)
	}
}

// State returns the cluster state after Setup.
func (cm *ClusterManager) State() ClusterState {
	return cm.state
}

// Skipped returns true if the cluster was skipped (fallback=skip + attach failed).
func (cm *ClusterManager) Skipped() bool {
	return cm.spec != nil && cm.state.Mode == ClusterModeNone
}

// tryAttach probes the master and discovers topology.
func (cm *ClusterManager) tryAttach(ctx context.Context, masterURL string) (ClusterState, error) {
	state := ClusterState{MasterURL: masterURL}

	// Check leader status.
	body, err := httpGet(ctx, masterURL+"/cluster/status")
	if err != nil {
		return state, fmt.Errorf("cluster/status: %w", err)
	}
	if !strings.Contains(body, `"IsLeader":true`) && !strings.Contains(body, `"isLeader":true`) {
		return state, fmt.Errorf("master is not leader: %s", body)
	}

	// Count volume servers.
	body, err = httpGet(ctx, masterURL+"/dir/status")
	if err == nil {
		var dirStatus struct {
			Topology struct {
				DataCenters []struct {
					Racks []struct {
						DataNodes []struct{} `json:"DataNodes"`
					} `json:"Racks"`
				} `json:"DataCenters"`
			} `json:"Topology"`
		}
		if json.Unmarshal([]byte(body), &dirStatus) == nil {
			for _, dc := range dirStatus.Topology.DataCenters {
				for _, rack := range dc.Racks {
					state.Servers += len(rack.DataNodes)
				}
			}
		}
	}

	// Count block-capable servers.
	body, err = httpGet(ctx, masterURL+"/block/servers")
	if err == nil {
		var servers []struct {
			BlockCapable bool `json:"block_capable"`
		}
		if json.Unmarshal([]byte(body), &servers) == nil {
			for _, s := range servers {
				if s.BlockCapable {
					state.BlockCap++
				}
			}
		}
	}
	// block/servers 404 is OK — means no block support, BlockCap stays 0.

	return state, nil
}

func (cm *ClusterManager) meetsRequirements(state ClusterState) bool {
	if cm.spec.Require.Servers > 0 && state.Servers < cm.spec.Require.Servers {
		return false
	}
	if cm.spec.Require.BlockCapable > 0 && state.BlockCap < cm.spec.Require.BlockCapable {
		return false
	}
	return true
}

// createManaged starts a weed master + volume servers on the specified node.
func (cm *ClusterManager) createManaged(ctx context.Context, actx *ActionContext) error {
	mc := cm.spec.Managed
	if mc.MasterPort == 0 {
		return fmt.Errorf("cluster.managed.master_port is required")
	}
	if mc.Node == "" {
		return fmt.Errorf("cluster.managed.node is required")
	}

	// Get the node runner.
	node, ok := actx.Nodes[mc.Node]
	if !ok {
		return fmt.Errorf("cluster.managed.node %q not found in topology", mc.Node)
	}
	cm.node = node

	// Determine IP.
	ip := mc.IP
	if ip == "" {
		if ns, ok := actx.Scenario.Topology.Nodes[mc.Node]; ok {
			ip = ns.Host
		}
	}
	if ip == "" {
		ip = "127.0.0.1"
	}

	cm.logFunc("[cluster] creating managed cluster: master=%d, %d volume servers on %s",
		mc.MasterPort, len(mc.Volumes), mc.Node)

	// Create master dir.
	masterDir := fmt.Sprintf("/tmp/sw-managed-master-%d", mc.MasterPort)
	node.RunRoot(ctx, fmt.Sprintf("rm -rf %s && mkdir -p %s", masterDir, masterDir))
	cm.state.Dirs = append(cm.state.Dirs, masterDir)

	// Start master.
	cmd := fmt.Sprintf("sh -c 'nohup %sweed master -port=%d -mdir=%s </dev/null >%s/master.log 2>&1 & echo $!'",
		UploadBasePath, mc.MasterPort, masterDir, masterDir)
	stdout, _, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("start master: code=%d err=%v", code, err)
	}
	masterPid := strings.TrimSpace(stdout)
	cm.state.Pids = append(cm.state.Pids, masterPid)
	cm.logFunc("[cluster] master started PID=%s port=%d", masterPid, mc.MasterPort)

	// Wait for master ready.
	masterURL := fmt.Sprintf("http://localhost:%d", mc.MasterPort)
	if err := cm.waitReady(ctx, node, masterURL, 30*time.Second); err != nil {
		return fmt.Errorf("master not ready: %w", err)
	}

	// Start volume servers.
	for i, vol := range mc.Volumes {
		vsDir := fmt.Sprintf("/tmp/sw-managed-vs%d-%d", i, vol.Port)
		node.RunRoot(ctx, fmt.Sprintf("rm -rf %s && mkdir -p %s", vsDir, vsDir))
		cm.state.Dirs = append(cm.state.Dirs, vsDir)

		args := fmt.Sprintf("-port=%d -mserver=localhost:%d -dir=%s -ip=%s",
			vol.Port, mc.MasterPort, vsDir, ip)
		if vol.BlockListen != "" {
			blockDir := vsDir + "/blocks"
			node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", blockDir))
			args += fmt.Sprintf(" -block.dir=%s -block.listen=%s", blockDir, vol.BlockListen)
		}
		if vol.ExtraArgs != "" {
			args += " " + vol.ExtraArgs
		}

		vsCmd := fmt.Sprintf("sh -c 'nohup %sweed volume %s </dev/null >%s/volume.log 2>&1 & echo $!'",
			UploadBasePath, args, vsDir)
		stdout, _, code, err := node.RunRoot(ctx, vsCmd)
		if err != nil || code != 0 {
			return fmt.Errorf("start volume server %d: code=%d err=%v", i, code, err)
		}
		vsPid := strings.TrimSpace(stdout)
		cm.state.Pids = append(cm.state.Pids, vsPid)
		cm.logFunc("[cluster] volume server %d started PID=%s port=%d", i, vsPid, vol.Port)
	}

	// Wait for volume servers to register.
	if err := cm.waitServers(ctx, masterURL); err != nil {
		return fmt.Errorf("servers not registered: %w", err)
	}

	// Count block-capable volumes and wait for block registration if needed.
	blockCount := 0
	for _, vol := range mc.Volumes {
		if vol.BlockListen != "" {
			blockCount++
		}
	}
	if blockCount > 0 {
		externalURL := fmt.Sprintf("http://%s:%d", ip, mc.MasterPort)
		if err := cm.waitBlockServers(ctx, externalURL, blockCount); err != nil {
			return fmt.Errorf("block servers not registered: %w", err)
		}
	}

	cm.state.Mode = ClusterModeManaged
	// Use external IP so other nodes (clients) can reach the master.
	cm.state.MasterURL = fmt.Sprintf("http://%s:%d", ip, mc.MasterPort)
	cm.state.Servers = len(mc.Volumes)
	cm.state.BlockCap = blockCount

	cm.setVars(actx)
	cm.logFunc("[cluster] managed cluster ready: master=%s servers=%d block_capable=%d",
		cm.state.MasterURL, cm.state.Servers, cm.state.BlockCap)
	return nil
}

func (cm *ClusterManager) waitReady(ctx context.Context, node NodeRunner, masterURL string, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout after %s", timeout)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			cmd := fmt.Sprintf("curl -s %s/cluster/status 2>/dev/null", masterURL)
			stdout, _, _, _ := node.Run(ctx, cmd)
			if strings.Contains(stdout, `"IsLeader":true`) || strings.Contains(stdout, `"isLeader":true`) {
				return nil
			}
		}
	}
}

func (cm *ClusterManager) waitServers(ctx context.Context, masterURL string) error {
	want := len(cm.spec.Managed.Volumes)
	if want == 0 {
		return nil
	}
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for %d servers", want)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			body, err := httpGet(ctx, masterURL+"/dir/status")
			if err != nil {
				continue
			}
			count := 0
			var dirStatus struct {
				Topology struct {
					DataCenters []struct {
						Racks []struct {
							DataNodes []struct{} `json:"DataNodes"`
						} `json:"Racks"`
					} `json:"DataCenters"`
				} `json:"Topology"`
			}
			if json.Unmarshal([]byte(body), &dirStatus) == nil {
				for _, dc := range dirStatus.Topology.DataCenters {
					for _, rack := range dc.Racks {
						count += len(rack.DataNodes)
					}
				}
			}
			if count >= want {
				return nil
			}
		}
	}
}

func (cm *ClusterManager) waitBlockServers(ctx context.Context, masterURL string, want int) error {
	cm.logFunc("[cluster] waiting for %d block-capable servers...", want)
	deadline := time.After(60 * time.Second)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for %d block-capable servers", want)
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			body, err := httpGet(ctx, masterURL+"/block/servers")
			if err != nil {
				continue
			}
			var servers []struct {
				BlockCapable bool `json:"block_capable"`
			}
			if json.Unmarshal([]byte(body), &servers) != nil {
				continue
			}
			capable := 0
			for _, s := range servers {
				if s.BlockCapable {
					capable++
				}
			}
			if capable >= want {
				cm.logFunc("[cluster] %d block-capable servers ready", capable)
				return nil
			}
		}
	}
}

func (cm *ClusterManager) setVars(actx *ActionContext) {
	actx.Vars["master_url"] = cm.state.MasterURL
	actx.Vars["cluster_mode"] = string(cm.state.Mode)
	actx.Vars["cluster_servers"] = fmt.Sprintf("%d", cm.state.Servers)
	actx.Vars["cluster_block_capable"] = fmt.Sprintf("%d", cm.state.BlockCap)
}

func httpGet(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return string(body), fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return string(body), nil
}
