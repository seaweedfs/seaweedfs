package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	r.RegisterFunc("cluster_status", tr.TierDevOps, clusterStatus)
}

// buildDeployWeed cross-compiles the weed binary and uploads to all nodes.
func buildDeployWeed(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	repoDir := actx.Vars["repo_dir"]
	if repoDir == "" {
		return nil, fmt.Errorf("build_deploy_weed: repo_dir not set in env")
	}

	actx.Log("  cross-compiling weed binary...")
	localBin := repoDir + "/weed-linux"
	buildCmd := fmt.Sprintf("cd %s && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o weed-linux ./weed/command", repoDir)

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

	cmd := fmt.Sprintf("setsid %sweed master -port=%s -mdir=%s %s </dev/null >%s/master.log 2>&1 & echo $!",
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

	cmd := fmt.Sprintf("setsid %sweed volume -port=%s -mserver=%s -dir=%s %s </dev/null >%s/volume.log 2>&1 & echo $!",
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
			_, _, code, _ := node.Run(ctx, fmt.Sprintf("kill -0 %s 2>/dev/null", pid))
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

// createBlockVolume creates a block volume via the master assign API.
func createBlockVolume(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("create_block_volume: %w", err)
	}

	masterURL := act.Params["master_url"]
	if masterURL == "" {
		return nil, fmt.Errorf("create_block_volume: master_url param required")
	}
	size := act.Params["size"]
	if size == "" {
		size = "1g"
	}

	cmd := fmt.Sprintf("curl -s -X POST '%s/vol/assign?type=block&size=%s' 2>/dev/null", masterURL, size)
	stdout, stderr, code, err := node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("create_block_volume: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
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
