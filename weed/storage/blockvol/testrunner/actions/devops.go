package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/internal/blockapi"
	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterDevOpsActions registers SeaweedFS cluster management actions.
func RegisterDevOpsActions(r *tr.Registry) {
	r.RegisterFunc("build_deploy_weed", tr.TierDevOps, buildDeployWeed)
	r.RegisterFunc("start_weed_master", tr.TierDevOps, startWeedMaster)
	r.RegisterFunc("start_weed_volume", tr.TierDevOps, startWeedVolume)
	r.RegisterFunc("stop_weed", tr.TierDevOps, stopWeed)
	r.RegisterFunc("wait_cluster_ready", tr.TierDevOps, waitClusterReady)
	r.RegisterFunc("create_block_volume", tr.TierDevOps, createBlockVolume)
	r.RegisterFunc("expand_block_volume", tr.TierDevOps, expandBlockVolume)
	r.RegisterFunc("lookup_block_volume", tr.TierDevOps, lookupBlockVolume)
	r.RegisterFunc("delete_block_volume", tr.TierDevOps, deleteBlockVolume)
	r.RegisterFunc("wait_block_servers", tr.TierDevOps, waitBlockServers)
	r.RegisterFunc("cluster_status", tr.TierDevOps, clusterStatus)
	r.RegisterFunc("wait_block_primary", tr.TierDevOps, waitBlockPrimary)
	r.RegisterFunc("assert_block_field", tr.TierDevOps, assertBlockField)
	r.RegisterFunc("block_status", tr.TierDevOps, blockStatus)
	r.RegisterFunc("block_promote", tr.TierDevOps, blockPromote)
	r.RegisterFunc("wait_volume_healthy", tr.TierDevOps, waitVolumeHealthy)
	r.RegisterFunc("discover_primary", tr.TierDevOps, discoverPrimary)
	r.RegisterFunc("collect_glog", tr.TierDevOps, collectGlog)
	r.RegisterFunc("collect_debug", tr.TierDevOps, collectDebug)
}

// setISCSIVars sets the save_as_iscsi_host/port/addr/iqn vars from a VolumeInfo.
// When the iSCSI addr has no host (e.g. ":3275"), falls back to the volume server's host.
func setISCSIVars(actx *tr.ActionContext, prefix string, info *blockapi.VolumeInfo) {
	actx.Vars[prefix+"_capacity"] = strconv.FormatUint(info.SizeBytes, 10)
	actx.Vars[prefix+"_iscsi_addr"] = info.ISCSIAddr
	actx.Vars[prefix+"_iqn"] = info.IQN
	if info.ISCSIAddr != "" {
		host, port, _ := net.SplitHostPort(info.ISCSIAddr)
		if host == "" && info.VolumeServer != "" {
			host, _, _ = net.SplitHostPort(info.VolumeServer)
		}
		actx.Vars[prefix+"_iscsi_host"] = host
		actx.Vars[prefix+"_iscsi_port"] = port
	}
}

// blockAPIClient builds a blockapi.Client from the master_url param or var.
func blockAPIClient(actx *tr.ActionContext, act tr.Action) (*blockapi.Client, error) {
	masterURL := act.Params["master_url"]
	if masterURL == "" {
		masterURL = actx.Vars["master_url"]
	}
	if masterURL == "" {
		return nil, fmt.Errorf("master_url param or var required")
	}
	return blockapi.NewClient(masterURL), nil
}

// buildDeployWeed cross-compiles the weed binary and uploads to all nodes.
func buildDeployWeed(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	repoDir := actx.Vars["repo_dir"]
	if repoDir == "" {
		return nil, fmt.Errorf("build_deploy_weed: repo_dir not set in env")
	}

	actx.Log("  cross-compiling weed binary...")
	localBin := repoDir + "/weed-linux"
	buildCmd := fmt.Sprintf("cd %s && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o weed-linux ./weed", repoDir)

	ln := tr.NewLocalNode("build-host")
	_, stderr, code, err := ln.Run(ctx, buildCmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("build_deploy_weed: code=%d stderr=%s err=%v", code, stderr, err)
	}

	// Upload to agents if coordinator mode.
	if actx.Coordinator != nil {
		for _, agentName := range actx.Coordinator.AgentNames() {
			actx.Log("  uploading weed to agent %s...", agentName)
			if err := actx.Coordinator.UploadToAgent(ctx, agentName, localBin, tr.UploadBasePath+"weed"); err != nil {
				return nil, fmt.Errorf("upload weed to %s: %w", agentName, err)
			}
		}
		return nil, nil
	}

	// SSH mode: deploy to all nodes.
	for nodeName, nodeRunner := range actx.Nodes {
		actx.Log("  deploying weed to node %s...", nodeName)
		nodeRunner.Run(ctx, fmt.Sprintf("mkdir -p %s", tr.UploadBasePath))
		if err := nodeRunner.Upload(localBin, tr.UploadBasePath+"weed"); err != nil {
			return nil, fmt.Errorf("deploy weed to %s: %w", nodeName, err)
		}
		nodeRunner.Run(ctx, fmt.Sprintf("chmod +x %sweed", tr.UploadBasePath))
	}

	return nil, nil
}

// logBinaryVersion logs the weed binary's md5, mtime, and size on the given node.
// Stores the md5 in __weed_md5_<node> var for cross-node consistency checks.
func logBinaryVersion(ctx context.Context, actx *tr.ActionContext, node tr.NodeRunner, nodeName string) {
	binPath := tr.UploadBasePath + "weed"
	stdout, _, _, err := node.Run(ctx, fmt.Sprintf(
		"md5sum %s 2>/dev/null | awk '{print $1}'; stat -c '%%Y %%s' %s 2>/dev/null",
		binPath, binPath))
	if err != nil {
		actx.Log("  [binary] %s: %s not found or error", nodeName, binPath)
		return
	}
	lines := strings.SplitN(strings.TrimSpace(stdout), "\n", 2)
	md5 := ""
	if len(lines) >= 1 {
		md5 = strings.TrimSpace(lines[0])
	}
	meta := ""
	if len(lines) >= 2 {
		meta = strings.TrimSpace(lines[1])
	}
	actx.Log("  [binary] %s: %s md5=%s %s", nodeName, binPath, md5, meta)
	if md5 != "" {
		varKey := "__weed_md5_" + nodeName
		if prev, ok := actx.Vars[varKey]; ok && prev != md5 {
			actx.Log("  [binary] WARNING: %s md5 changed %s → %s", nodeName, prev, md5)
		}
		actx.Vars[varKey] = md5
		// Cross-node check: if another node already set its md5, compare.
		for k, v := range actx.Vars {
			if strings.HasPrefix(k, "__weed_md5_") && k != varKey && v != md5 {
				actx.Log("  [binary] WARNING: md5 mismatch %s=%s vs %s=%s", nodeName, md5, strings.TrimPrefix(k, "__weed_md5_"), v)
			}
		}
	}
}

// startWeedMaster starts a weed master process on the given node.
func startWeedMaster(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("start_weed_master: %w", err)
	}

	port := act.Params["port"]
	if port == "" {
		port = "9333"
	}
	dir := act.Params["dir"]
	if dir == "" {
		dir = "/tmp/sw-weed-master"
	}
	extraArgs := act.Params["extra_args"]

	// Log binary version for traceability.
	logBinaryVersion(ctx, actx, node, act.Node)

	// Ensure directory exists.
	node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", dir))

	cmd := fmt.Sprintf("sh -c 'nohup %sweed master -port=%s -mdir=%s %s </dev/null >%s/master.log 2>&1 & echo $!'",
		tr.UploadBasePath, port, dir, extraArgs, dir)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("start_weed_master: code=%d stderr=%s err=%v", code, stderr, err)
	}

	pid := strings.TrimSpace(stdout)
	actx.Log("  weed master started on port %s (PID %s)", port, pid)
	return map[string]string{"value": pid}, nil
}

// startWeedVolume starts a weed volume process on the given node.
func startWeedVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("start_weed_volume: %w", err)
	}

	port := act.Params["port"]
	if port == "" {
		port = "8080"
	}
	master := act.Params["master"]
	if master == "" {
		return nil, fmt.Errorf("start_weed_volume: master param required")
	}
	dir := act.Params["dir"]
	if dir == "" {
		dir = "/tmp/sw-weed-volume"
	}
	extraArgs := act.Params["extra_args"]

	// Log binary version for traceability.
	logBinaryVersion(ctx, actx, node, act.Node)

	node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", dir))

	cmd := fmt.Sprintf("sh -c 'nohup %sweed volume -port=%s -mserver=%s -dir=%s %s </dev/null >%s/volume.log 2>&1 & echo $!'",
		tr.UploadBasePath, port, master, dir, extraArgs, dir)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("start_weed_volume: code=%d stderr=%s err=%v", code, stderr, err)
	}

	pid := strings.TrimSpace(stdout)
	actx.Log("  weed volume started on port %s (PID %s)", port, pid)
	return map[string]string{"value": pid}, nil
}

// stopWeed stops a weed process by PID.
func stopWeed(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("stop_weed: %w", err)
	}

	pid := act.Params["pid"]
	if pid == "" {
		return nil, fmt.Errorf("stop_weed: pid param required")
	}

	// Graceful kill first, then force after 5s.
	_, _, _, _ = node.RunRoot(ctx, fmt.Sprintf("kill %s", pid))

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			node.RunRoot(ctx, fmt.Sprintf("kill -9 %s", pid))
			actx.Log("  force-killed PID %s", pid)
			return nil, nil
		case <-ticker.C:
			_, _, code, _ := node.RunRoot(ctx, fmt.Sprintf("kill -0 %s 2>/dev/null", pid))
			if code != 0 {
				actx.Log("  PID %s exited gracefully", pid)
				return nil, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// waitClusterReady polls the master until IsLeader is true.
func waitClusterReady(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("wait_cluster_ready: %w", err)
	}

	masterURL := act.Params["master_url"]
	if masterURL == "" {
		return nil, fmt.Errorf("wait_cluster_ready: master_url param required")
	}

	timeout := 30 * time.Second
	if t, ok := act.Params["timeout"]; ok {
		if d, err := ParseDuration(t); err == nil {
			timeout = d
		}
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("wait_cluster_ready: timeout after %s", timeout)
		case <-ticker.C:
			cmd := fmt.Sprintf("curl -s %s/cluster/status 2>/dev/null", masterURL)
			stdout, _, code, err := node.Run(timeoutCtx, cmd)
			if err != nil || code != 0 {
				continue
			}
			if strings.Contains(stdout, `"IsLeader":true`) || strings.Contains(stdout, `"isLeader":true`) {
				actx.Log("  cluster ready at %s", masterURL)
				return map[string]string{"value": stdout}, nil
			}
		}
	}
}

// createBlockVolume creates a block volume via the master block API.
// Params: name, size (human e.g. "50M") or size_bytes, replica_factor (default 1).
// Sets save_as=JSON, save_as_capacity, save_as_iscsi_addr, save_as_iqn.
func createBlockVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("create_block_volume: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("create_block_volume: name param required")
	}

	var sizeBytes uint64
	if sb := act.Params["size_bytes"]; sb != "" {
		sizeBytes, err = strconv.ParseUint(sb, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("create_block_volume: invalid size_bytes: %w", err)
		}
	} else {
		size := act.Params["size"]
		if size == "" {
			size = "1G"
		}
		sizeBytes, err = ParseSizeBytes(size)
		if err != nil {
			return nil, fmt.Errorf("create_block_volume: %w", err)
		}
	}

	rf := ParseInt(act.Params["replica_factor"], 1)

	durMode := act.Params["durability_mode"]

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           name,
		SizeBytes:      sizeBytes,
		ReplicaFactor:  rf,
		DurabilityMode: durMode,
	})
	if err != nil {
		return nil, fmt.Errorf("create_block_volume: %w", err)
	}

	jsonBytes, _ := json.Marshal(info)
	actx.Log("  created block volume %s (size=%d, rf=%d)", name, info.SizeBytes, rf)

	// Set multi-var outputs.
	if act.SaveAs != "" {
		setISCSIVars(actx, act.SaveAs, info)
	}

	return map[string]string{"value": string(jsonBytes)}, nil
}

// expandBlockVolume expands a block volume via master block API.
// Params: name, new_size (human e.g. "100M") or new_size_bytes.
func expandBlockVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("expand_block_volume: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("expand_block_volume: name param required")
	}

	var newSizeBytes uint64
	if sb := act.Params["new_size_bytes"]; sb != "" {
		newSizeBytes, err = strconv.ParseUint(sb, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expand_block_volume: invalid new_size_bytes: %w", err)
		}
	} else {
		ns := act.Params["new_size"]
		if ns == "" {
			return nil, fmt.Errorf("expand_block_volume: new_size or new_size_bytes param required")
		}
		newSizeBytes, err = ParseSizeBytes(ns)
		if err != nil {
			return nil, fmt.Errorf("expand_block_volume: %w", err)
		}
	}

	capacity, err := client.ExpandVolume(ctx, name, newSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("expand_block_volume: %w", err)
	}

	actx.Log("  expanded block volume %s -> %d bytes", name, capacity)
	return map[string]string{"value": strconv.FormatUint(capacity, 10)}, nil
}

// lookupBlockVolume looks up a block volume via master block API.
// Params: name. Sets save_as_capacity, save_as_iscsi_addr, save_as_iqn, save_as_iscsi_host, save_as_iscsi_port.
func lookupBlockVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("lookup_block_volume: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("lookup_block_volume: name param required")
	}

	info, err := client.LookupVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("lookup_block_volume: %w", err)
	}

	if act.SaveAs != "" {
		setISCSIVars(actx, act.SaveAs, info)
		// Also save replica data/ctrl addresses + ports for fault injection.
		if info.ReplicaDataAddr != "" {
			actx.Vars[act.SaveAs+"_replica_data_addr"] = info.ReplicaDataAddr
			_, dataPort, _ := net.SplitHostPort(info.ReplicaDataAddr)
			actx.Vars[act.SaveAs+"_replica_data_port"] = dataPort
		}
		if info.ReplicaCtrlAddr != "" {
			actx.Vars[act.SaveAs+"_replica_ctrl_addr"] = info.ReplicaCtrlAddr
			_, ctrlPort, _ := net.SplitHostPort(info.ReplicaCtrlAddr)
			actx.Vars[act.SaveAs+"_replica_ctrl_port"] = ctrlPort
		}
		// Save primary server host for iptables targeting.
		if info.VolumeServer != "" {
			host, _, _ := net.SplitHostPort(info.VolumeServer)
			actx.Vars[act.SaveAs+"_primary_host"] = host
		}
	}

	actx.Log("  looked up %s: size=%d iscsi=%s repl_data=%s", name, info.SizeBytes, info.ISCSIAddr, info.ReplicaDataAddr)
	return map[string]string{"value": strconv.FormatUint(info.SizeBytes, 10)}, nil
}

// deleteBlockVolume deletes a block volume via master block API.
func deleteBlockVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("delete_block_volume: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("delete_block_volume: name param required")
	}

	if err := client.DeleteVolume(ctx, name); err != nil {
		return nil, fmt.Errorf("delete_block_volume: %w", err)
	}

	actx.Log("  deleted block volume %s", name)
	return nil, nil
}

// waitBlockServers polls master until N block-capable servers are registered.
// Params: count (default 1), timeout (default 60s).
func waitBlockServers(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("wait_block_servers: %w", err)
	}

	want := ParseInt(act.Params["count"], 1)

	timeout := 60 * time.Second
	if t, ok := act.Params["timeout"]; ok {
		if d, err := ParseDuration(t); err == nil {
			timeout = d
		}
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("wait_block_servers: timeout waiting for %d servers after %s (polled %d times)", want, timeout, pollCount)
		case <-ticker.C:
			pollCount++
			servers, err := client.ListServers(timeoutCtx)
			if err != nil {
				actx.Log("  poll %d: error: %v", pollCount, err)
				continue
			}
			capable := 0
			for _, s := range servers {
				if s.BlockCapable {
					capable++
				}
			}
			if pollCount <= 3 || pollCount%10 == 0 {
				actx.Log("  poll %d: %d/%d block-capable servers (total %d)", pollCount, capable, want, len(servers))
			}
			if capable >= want {
				actx.Log("  %d block-capable servers ready", capable)
				return map[string]string{"value": strconv.Itoa(capable)}, nil
			}
		}
	}
}

// waitBlockPrimary polls lookup until the volume's primary server matches (or differs from) expected.
// Params: name, expected (server addr to wait for) OR not (server addr to wait to change from), timeout (default 60s).
// Sets save_as vars from the final lookup.
func waitBlockPrimary(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("wait_block_primary: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("wait_block_primary: name param required")
	}
	expected := act.Params["expected"]
	notServer := act.Params["not"]
	if expected == "" && notServer == "" {
		return nil, fmt.Errorf("wait_block_primary: expected or not param required")
	}

	timeout := 60 * time.Second
	if t, ok := act.Params["timeout"]; ok {
		if d, err := ParseDuration(t); err == nil {
			timeout = d
		}
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	pollCount := 0
	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("wait_block_primary: timeout after %s waiting for primary change on %s", timeout, name)
		case <-ticker.C:
			pollCount++
			info, err := client.LookupVolume(timeoutCtx, name)
			if err != nil {
				if pollCount <= 3 {
					actx.Log("  poll %d: lookup error: %v", pollCount, err)
				}
				continue
			}
			if pollCount <= 3 || pollCount%10 == 0 {
				actx.Log("  poll %d: %s primary=%s role=%s", pollCount, name, info.VolumeServer, info.Role)
			}

			match := false
			if expected != "" && info.VolumeServer == expected {
				match = true
			}
			if notServer != "" && info.VolumeServer != notServer && info.VolumeServer != "" {
				match = true
			}
			if match {
				actx.Log("  primary for %s is now %s (epoch=%d)", name, info.VolumeServer, info.Epoch)
				if act.SaveAs != "" {
					setISCSIVars(actx, act.SaveAs, info)
					actx.Vars[act.SaveAs+"_server"] = info.VolumeServer
					actx.Vars[act.SaveAs+"_epoch"] = strconv.FormatUint(info.Epoch, 10)
					actx.Vars[act.SaveAs+"_role"] = info.Role
				}
				return map[string]string{"value": info.VolumeServer}, nil
			}
		}
	}
}

// assertBlockField looks up a block volume and asserts a specific field matches the expected value.
// Params: name, field (one of: volume_server, role, status, epoch, size_bytes, replica_server,
//   replica_factor, health_score, replica_degraded, durability_mode, iscsi_addr, iqn), expected.
func assertBlockField(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("assert_block_field: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("assert_block_field: name param required")
	}
	field := act.Params["field"]
	if field == "" {
		return nil, fmt.Errorf("assert_block_field: field param required")
	}
	expected := act.Params["expected"]
	if expected == "" {
		return nil, fmt.Errorf("assert_block_field: expected param required")
	}

	info, err := client.LookupVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("assert_block_field: lookup %s: %w", name, err)
	}

	actual, err := extractVolumeField(info, field)
	if err != nil {
		return nil, fmt.Errorf("assert_block_field: %w", err)
	}

	if actual != expected {
		return nil, fmt.Errorf("assert_block_field: %s.%s = %q, expected %q", name, field, actual, expected)
	}
	actx.Log("  assert %s.%s == %q OK", name, field, expected)
	return map[string]string{"value": actual}, nil
}

// extractVolumeField extracts a named field from VolumeInfo as a string.
func extractVolumeField(info *blockapi.VolumeInfo, field string) (string, error) {
	switch field {
	case "volume_server":
		return info.VolumeServer, nil
	case "role":
		return info.Role, nil
	case "status":
		return info.Status, nil
	case "epoch":
		return strconv.FormatUint(info.Epoch, 10), nil
	case "size_bytes":
		return strconv.FormatUint(info.SizeBytes, 10), nil
	case "replica_server":
		return info.ReplicaServer, nil
	case "replica_factor":
		return strconv.Itoa(info.ReplicaFactor), nil
	case "health_score":
		return fmt.Sprintf("%.2f", info.HealthScore), nil
	case "replica_degraded":
		return strconv.FormatBool(info.ReplicaDegraded), nil
	case "durability_mode":
		return info.DurabilityMode, nil
	case "iscsi_addr":
		return info.ISCSIAddr, nil
	case "iqn":
		return info.IQN, nil
	case "name":
		return info.Name, nil
	case "replica_iscsi_addr":
		return info.ReplicaISCSIAddr, nil
	case "replica_iqn":
		return info.ReplicaIQN, nil
	case "replica_data_addr":
		return info.ReplicaDataAddr, nil
	case "replica_ctrl_addr":
		return info.ReplicaCtrlAddr, nil
	default:
		return "", fmt.Errorf("unknown field %q", field)
	}
}

// blockStatus fetches block registry status metrics from master.
// Sets save_as_promotions_total, save_as_failovers_total, etc.
func blockStatus(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("block_status: %w", err)
	}

	status, err := client.BlockStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("block_status: %w", err)
	}

	actx.Log("  block status: volumes=%d servers=%d promotions=%d failovers=%d rebuilds=%d",
		status.VolumeCount, status.ServerCount, status.PromotionsTotal, status.FailoversTotal, status.RebuildsTotal)

	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_volume_count"] = strconv.Itoa(status.VolumeCount)
		actx.Vars[act.SaveAs+"_server_count"] = strconv.Itoa(status.ServerCount)
		actx.Vars[act.SaveAs+"_promotions_total"] = strconv.FormatInt(status.PromotionsTotal, 10)
		actx.Vars[act.SaveAs+"_failovers_total"] = strconv.FormatInt(status.FailoversTotal, 10)
		actx.Vars[act.SaveAs+"_rebuilds_total"] = strconv.FormatInt(status.RebuildsTotal, 10)
		actx.Vars[act.SaveAs+"_queue_depth"] = strconv.Itoa(status.AssignmentQueueDepth)
	}

	jsonBytes, _ := json.Marshal(status)
	return map[string]string{"value": string(jsonBytes)}, nil
}

// blockPromote triggers a manual promotion for a block volume.
// Params: name, target_server (optional, empty=auto), force (optional bool), reason (optional).
func blockPromote(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("block_promote: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		return nil, fmt.Errorf("block_promote: name param required")
	}

	force := false
	if f := act.Params["force"]; f == "true" || f == "1" {
		force = true
	}

	resp, err := client.PromoteVolume(ctx, name, blockapi.PromoteVolumeRequest{
		TargetServer: act.Params["target_server"],
		Force:        force,
		Reason:       act.Params["reason"],
	})
	if err != nil {
		return nil, fmt.Errorf("block_promote: %w", err)
	}

	actx.Log("  promoted %s -> primary=%s epoch=%d", name, resp.NewPrimary, resp.Epoch)
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs+"_server"] = resp.NewPrimary
		actx.Vars[act.SaveAs+"_epoch"] = strconv.FormatUint(resp.Epoch, 10)
		actx.Vars[act.SaveAs+"_reason"] = resp.Reason
		actx.Vars[act.SaveAs+"_rejections_count"] = strconv.Itoa(len(resp.Rejections))
		if raw, err := json.Marshal(resp.Rejections); err == nil {
			actx.Vars[act.SaveAs+"_rejections_json"] = string(raw)
		}
	}
	return map[string]string{"value": resp.NewPrimary}, nil
}

// waitVolumeHealthy polls until a block volume is healthy:
// - not degraded (all replicas connected)
// - RF replicas present (if RF > 1)
// Useful after create_block_volume to wait for shipper bootstrap before
// operations that require sync_all barrier success (mkfs, pgbench).
//
// Params:
//   - name: volume name (required)
//   - master_url: master API (or from var)
//   - timeout: max wait duration (default: "60s")
//   - poll_interval: poll interval (default: "2s")
//
// Returns: value = "healthy" on success
func waitVolumeHealthy(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("wait_volume_healthy: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		name = actx.Vars["volume_name"]
	}
	if name == "" {
		return nil, fmt.Errorf("wait_volume_healthy: name param required")
	}

	timeoutStr := act.Params["timeout"]
	if timeoutStr == "" {
		timeoutStr = "60s"
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, fmt.Errorf("wait_volume_healthy: invalid timeout %q: %w", timeoutStr, err)
	}

	intervalStr := act.Params["poll_interval"]
	if intervalStr == "" {
		intervalStr = "2s"
	}
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("wait_volume_healthy: invalid poll_interval %q: %w", intervalStr, err)
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	poll := 0
	for {
		select {
		case <-deadline:
			return nil, fmt.Errorf("wait_volume_healthy: %q not healthy after %s (polled %d times)", name, timeout, poll)
		case <-ctx.Done():
			return nil, fmt.Errorf("wait_volume_healthy: context cancelled")
		case <-ticker.C:
			poll++
			info, err := client.LookupVolume(ctx, name)
			if err != nil {
				actx.Log("  poll %d: lookup error: %v", poll, err)
				continue
			}

			// Check RF > 1 volumes have replicas assigned.
			if info.ReplicaFactor > 1 && len(info.Replicas) == 0 {
				actx.Log("  poll %d: waiting for replica assignment (RF=%d, replicas=0)", poll, info.ReplicaFactor)
				continue
			}

			if info.ReplicaFactor > 1 && !info.ReplicaReady {
				actx.Log("  poll %d: replica assigned but not publish-ready yet", poll)
				continue
			}

			// Check not degraded.
			if info.ReplicaDegraded {
				actx.Log("  poll %d: replica degraded, waiting...", poll)
				continue
			}

			if ready, reason := volumeHealthyReady(info); !ready {
				actx.Log("  poll %d: %s", poll, reason)
				continue
			}

			actx.Log("  volume %q healthy after %d polls (RF=%d, mode=%s, degraded=%v)",
				name, poll, info.ReplicaFactor, info.VolumeMode, info.ReplicaDegraded)
			return map[string]string{"value": "healthy"}, nil
		}
	}
}

func volumeHealthyReady(info *blockapi.VolumeInfo) (bool, string) {
	if info == nil {
		return false, "volume info missing"
	}
	if info.ReplicaFactor > 1 && info.DurabilityMode == "sync_all" && info.VolumeMode != "publish_healthy" {
		mode := info.VolumeMode
		if mode == "" {
			mode = "unknown"
		}
		if info.VolumeModeReason != "" {
			return false, fmt.Sprintf("volume_mode=%s (%s), waiting for publish_healthy", mode, info.VolumeModeReason)
		}
		return false, fmt.Sprintf("volume_mode=%s, waiting for publish_healthy", mode)
	}
	return true, ""
}

// discoverPrimary looks up a block volume and maps the primary's IP to a topology node name.
// This solves the "which node to kill?" problem for degraded-mode and failover scenarios.
//
// Params:
//   - name: volume name (required)
//   - master_url: master API (or from var)
//
// Saves (using save_as prefix):
//   - save_as = node name (e.g. "m01")
//   - save_as_server = full server address (e.g. "10.0.0.3:18480")
//   - save_as_host = server IP (e.g. "10.0.0.3")
//   - save_as_replica_node = replica node name (if RF>1 and replica found)
func discoverPrimary(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("discover_primary: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		name = actx.Vars["volume_name"]
	}
	if name == "" {
		return nil, fmt.Errorf("discover_primary: name param required")
	}

	info, err := client.LookupVolume(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("discover_primary: lookup %s: %w", name, err)
	}

	primaryHost, _, _ := net.SplitHostPort(info.VolumeServer)
	if primaryHost == "" {
		return nil, fmt.Errorf("discover_primary: volume_server %q has no host", info.VolumeServer)
	}

	// Match primary host to topology node (check Host and AltIPs).
	primaryNode := ""
	if actx.Scenario != nil {
		for nodeName, nodeSpec := range actx.Scenario.Topology.Nodes {
			if nodeSpec.Host == primaryHost {
				primaryNode = nodeName
				break
			}
			for _, altIP := range nodeSpec.AltIPs {
				if altIP == primaryHost {
					primaryNode = nodeName
					break
				}
			}
			if primaryNode != "" {
				break
			}
		}
	}
	if primaryNode == "" {
		return nil, fmt.Errorf("discover_primary: no topology node matches primary host %s", primaryHost)
	}

	result := map[string]string{"value": primaryNode}
	if act.SaveAs != "" {
		actx.Vars[act.SaveAs] = primaryNode
		actx.Vars[act.SaveAs+"_server"] = info.VolumeServer
		actx.Vars[act.SaveAs+"_host"] = primaryHost
	}

	// Also discover replica node if available.
	if info.ReplicaServer != "" {
		replicaHost, _, _ := net.SplitHostPort(info.ReplicaServer)
		for nodeName, nodeSpec := range actx.Scenario.Topology.Nodes {
			match := nodeSpec.Host == replicaHost
			if !match {
				for _, altIP := range nodeSpec.AltIPs {
					if altIP == replicaHost {
						match = true
						break
					}
				}
			}
			if match {
				if act.SaveAs != "" {
					actx.Vars[act.SaveAs+"_replica_node"] = nodeName
				}
				actx.Log("  primary=%s (node %s), replica=%s (node %s)", info.VolumeServer, primaryNode, info.ReplicaServer, nodeName)
				return result, nil
			}
		}
	}

	actx.Log("  primary=%s (node %s)", info.VolumeServer, primaryNode)
	return result, nil
}

// clusterStatus fetches the full cluster status JSON.
func clusterStatus(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("cluster_status: %w", err)
	}

	masterURL := act.Params["master_url"]
	if masterURL == "" {
		return nil, fmt.Errorf("cluster_status: master_url param required")
	}

	cmd := fmt.Sprintf("curl -s %s/cluster/status 2>/dev/null", masterURL)
	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("cluster_status: code=%d stderr=%s err=%v", code, stderr, err)
	}

	// Validate it's JSON.
	var js json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &js); err != nil {
		return nil, fmt.Errorf("cluster_status: invalid JSON response: %s", stdout)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// collectGlog downloads glog files from a remote node matching a pattern.
//
//	- action: collect_glog
//	  node: m01
//	  pattern: "/tmp/weed.*.INFO.*"
//	  save_dir: glog/m01
//	  max_files: "5"
func collectGlog(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}
	pattern := act.Params["pattern"]
	if pattern == "" {
		pattern = "/tmp/weed.*.INFO.*"
	}
	saveDir := act.Params["save_dir"]
	if saveDir == "" {
		saveDir = "glog/" + act.Node
	}
	maxFiles := act.Params["max_files"]
	if maxFiles == "" {
		maxFiles = "5"
	}

	// List matching files (newest first).
	stdout, _, _, err := node.Run(ctx, fmt.Sprintf("ls -t %s 2>/dev/null | head -%s", pattern, maxFiles))
	if err != nil {
		return nil, fmt.Errorf("collect_glog: list: %v", err)
	}

	files := strings.Split(strings.TrimSpace(stdout), "\n")
	collected := 0
	for _, f := range files {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		localPath := saveDir + "/" + act.Node + "-" + filepath.Base(f)
		if err := node.Download(f, localPath); err != nil {
			actx.Log("  collect_glog: download %s: %v (skipping)", f, err)
			continue
		}
		collected++
	}

	actx.Log("  collected %d glog files from %s", collected, act.Node)
	return map[string]string{"count": strconv.Itoa(collected)}, nil
}

// collectDebug fetches a debug endpoint via curl from a node and saves to file.
//
//	- action: collect_debug
//	  node: m02
//	  url: "http://10.0.0.3:18480/debug/block/shipper"
//	  save_as: debug-m02-18480.json
func collectDebug(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, err
	}
	url := act.Params["url"]
	if url == "" {
		return nil, fmt.Errorf("collect_debug: url param required")
	}
	savePath := act.Params["save_as"]
	if savePath == "" {
		savePath = "debug.json"
	}

	stdout, _, code, err := node.Run(ctx, fmt.Sprintf("curl -s --max-time 3 %s 2>/dev/null", url))
	if err != nil || code != 0 || len(stdout) < 2 {
		actx.Log("  collect_debug: %s unavailable (code=%d)", url, code)
		return nil, nil // not an error — endpoint may be down after test
	}

	// Write to local file.
	if err := os.MkdirAll(filepath.Dir(savePath), 0755); err != nil {
		return nil, fmt.Errorf("collect_debug: mkdir %s: %v", filepath.Dir(savePath), err)
	}
	if err := os.WriteFile(savePath, []byte(stdout), 0644); err != nil {
		return nil, fmt.Errorf("collect_debug: write %s: %v", savePath, err)
	}

	actx.Log("  captured debug from %s → %s", url, savePath)
	return map[string]string{"path": savePath}, nil
}
