package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
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

// startWeedMaster starts a weed master process on the given node.
func startWeedMaster(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
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
	node, err := getNode(actx, act.Node)
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
	node, err := getNode(actx, act.Node)
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
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("wait_cluster_ready: %w", err)
	}

	masterURL := act.Params["master_url"]
	if masterURL == "" {
		return nil, fmt.Errorf("wait_cluster_ready: master_url param required")
	}

	timeout := 30 * time.Second
	if t, ok := act.Params["timeout"]; ok {
		if d, err := parseDuration(t); err == nil {
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
		sizeBytes, err = parseSizeBytes(size)
		if err != nil {
			return nil, fmt.Errorf("create_block_volume: %w", err)
		}
	}

	rf := parseInt(act.Params["replica_factor"], 1)

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:          name,
		SizeBytes:     sizeBytes,
		ReplicaFactor: rf,
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
		newSizeBytes, err = parseSizeBytes(ns)
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
	}

	actx.Log("  looked up %s: size=%d iscsi=%s", name, info.SizeBytes, info.ISCSIAddr)
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

	want := parseInt(act.Params["count"], 1)

	timeout := 60 * time.Second
	if t, ok := act.Params["timeout"]; ok {
		if d, err := parseDuration(t); err == nil {
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
		if d, err := parseDuration(t); err == nil {
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
	}
	return map[string]string{"value": resp.NewPrimary}, nil
}

// clusterStatus fetches the full cluster status JSON.
func clusterStatus(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
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
