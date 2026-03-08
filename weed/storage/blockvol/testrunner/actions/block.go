package actions

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterBlockActions registers all block/target lifecycle actions.
func RegisterBlockActions(r *tr.Registry) {
	r.RegisterFunc("build_deploy", tr.TierBlock, buildDeploy)
	r.RegisterFunc("start_target", tr.TierBlock, startTarget)
	r.RegisterFunc("stop_target", tr.TierBlock, stopTarget)
	r.RegisterFunc("kill_target", tr.TierBlock, killTarget)
	r.RegisterFunc("kill_stale", tr.TierBlock, killStale)
	r.RegisterFunc("stop_all_targets", tr.TierBlock, stopAllTargets)
	r.RegisterFunc("assign", tr.TierBlock, assign)
	r.RegisterFunc("set_replica", tr.TierBlock, setReplica)
	r.RegisterFunc("wait_role", tr.TierBlock, waitRole)
	r.RegisterFunc("wait_lsn", tr.TierBlock, waitLSN)
	r.RegisterFunc("status", tr.TierBlock, statusAction)
	r.RegisterFunc("start_rebuild_server", tr.TierBlock, startRebuildServer)
	r.RegisterFunc("start_rebuild_client", tr.TierBlock, startRebuildClient)
}

func buildDeploy(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	repoDir := actx.Vars["repo_dir"]
	if repoDir == "" {
		return nil, fmt.Errorf("build_deploy: repo_dir not set in env")
	}

	// Coordinator mode: cross-compile locally, upload to agents via HTTP.
	if actx.Coordinator != nil {
		return buildDeployCoordinator(ctx, actx, repoDir)
	}

	// Agent mode: build locally if Go+source are available, or check binary exists.
	if actx.Scenario != nil && len(actx.Scenario.Topology.Agents) > 0 {
		return buildDeployAgent(ctx, actx, repoDir)
	}

	// SSH mode: cross-compile locally, deploy to nodes via SSH/SCP.
	return buildDeploySSH(ctx, actx, repoDir)
}

// buildDeployAgent handles build_deploy on agent nodes.
// Checks for pre-deployed binary first (common in iterative dev: build on
// Windows, SCP to nodes). Only builds from local source if no binary exists
// or "force_build" param is set.
func buildDeployAgent(ctx context.Context, actx *tr.ActionContext, repoDir string) (map[string]string, error) {
	binPath := "/tmp/iscsi-target-test"
	forceBuild := actx.Vars["force_build"] == "true"

	node, _ := getNode(actx, "")

	// Check for pre-deployed binary (preferred: avoids stale source issues).
	if node != nil && !forceBuild {
		_, _, code, _ := node.Run(ctx, fmt.Sprintf("test -x %s", binPath))
		if code == 0 {
			actx.Log("  using pre-deployed binary at %s", binPath)
			return nil, nil
		}
	}

	// Build from source if Go is available.
	if node != nil {
		_, _, goCode, _ := node.Run(ctx, "which go")
		srcExists := false
		if goCode == 0 {
			_, _, srcCode, _ := node.Run(ctx, fmt.Sprintf("test -d %s/weed/storage/blockvol/iscsi", repoDir))
			srcExists = srcCode == 0
		}

		if goCode == 0 && srcExists {
			actx.Log("  building iscsi-target on agent from %s ...", repoDir)
			buildCmd := fmt.Sprintf("cd %s && CGO_ENABLED=0 go build -o %s ./weed/storage/blockvol/iscsi/cmd/iscsi-target/", repoDir, binPath)
			_, stderr, code, err := node.Run(ctx, buildCmd)
			if err != nil || code != 0 {
				return nil, fmt.Errorf("agent build failed: code=%d stderr=%s err=%v", code, stderr, err)
			}
			actx.Log("  build OK")
			return nil, nil
		}
	}

	return nil, fmt.Errorf("build_deploy (agent): no pre-deployed binary at %s and Go not available", binPath)
}

func buildDeployCoordinator(ctx context.Context, actx *tr.ActionContext, repoDir string) (map[string]string, error) {
	// Build locally (Windows cross-compile).
	actx.Log("  building iscsi-target binary (coordinator mode)...")
	var node *infra.Node
	for _, n := range actx.Nodes {
		if nn, ok := n.(*infra.Node); ok {
			node = nn
			break
		}
	}
	if node == nil {
		// In coordinator mode we might not have infra.Nodes.
		// Build using local exec (bash -c via Git Bash on Windows).
		actx.Log("  cross-compiling with local exec...")
		buildCmd := fmt.Sprintf("cd %s && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o iscsi-target-linux ./weed/storage/blockvol/iscsi/cmd/iscsi-target/", repoDir)
		ln := tr.NewLocalNode("build-host")
		_, stderr, code, err := ln.Run(ctx, buildCmd)
		if err != nil || code != 0 {
			return nil, fmt.Errorf("build failed: code=%d stderr=%s err=%v", code, stderr, err)
		}

		// Convert bash path to native OS path for os.Open in UploadToAgent.
		localBin := repoDir + "/iscsi-target-linux"
		localBin = toNativePath(localBin)

		// Upload to each agent that hosts targets.
		return uploadToTargetAgents(ctx, actx, localBin)
	}

	tgt := infra.NewTarget(node, infra.DefaultTargetConfig())
	if err := tgt.Build(ctx, repoDir); err != nil {
		return nil, fmt.Errorf("build: %w", err)
	}
	localBin := repoDir + "/iscsi-target-linux"

	return uploadToTargetAgents(ctx, actx, localBin)
}

func uploadToTargetAgents(ctx context.Context, actx *tr.ActionContext, localBin string) (map[string]string, error) {
	// Find agents that host targets.
	targetAgents := make(map[string]bool)
	for _, spec := range actx.Scenario.Targets {
		nodeSpec, ok := actx.Scenario.Topology.Nodes[spec.Node]
		if ok && nodeSpec.Agent != "" {
			targetAgents[nodeSpec.Agent] = true
		}
	}

	for agentName := range targetAgents {
		actx.Log("  uploading to agent %s...", agentName)
		if err := actx.Coordinator.UploadToAgent(ctx, agentName, localBin, tr.UploadBasePath+"iscsi-target-test"); err != nil {
			return nil, fmt.Errorf("upload to agent %s: %w", agentName, err)
		}
	}

	return nil, nil
}

func buildDeploySSH(ctx context.Context, actx *tr.ActionContext, repoDir string) (map[string]string, error) {
	// Use the first available node to determine local vs remote.
	// Build always happens locally (Windows cross-compile).
	var node *infra.Node
	for _, n := range actx.Nodes {
		if nn, ok := n.(*infra.Node); ok {
			node = nn
			break
		}
	}
	if node == nil {
		return nil, fmt.Errorf("build_deploy: no nodes available")
	}

	localBin := repoDir + "/iscsi-target-linux"

	// Skip build if binary already exists (pre-built deployment).
	if _, err := os.Stat(localBin); err != nil {
		tgt := infra.NewTarget(node, infra.DefaultTargetConfig())
		actx.Log("  building iscsi-target binary...")
		if err := tgt.Build(ctx, repoDir); err != nil {
			return nil, fmt.Errorf("build: %w", err)
		}
	} else {
		actx.Log("  using pre-built binary: %s", localBin)
	}

	// Deploy only to nodes that host targets (not client-only nodes).
	targetNodes := make(map[string]bool)
	for _, spec := range actx.Scenario.Targets {
		targetNodes[spec.Node] = true
	}

	deployed := make(map[string]bool)
	for nodeName := range targetNodes {
		if deployed[nodeName] {
			continue
		}
		nodeRunner, ok := actx.Nodes[nodeName]
		if !ok {
			continue
		}
		n, ok := nodeRunner.(*infra.Node)
		if !ok {
			continue
		}
		actx.Log("  deploying to node %s...", nodeName)
		deployTgt := infra.NewTarget(n, infra.DefaultTargetConfig())
		if err := deployTgt.Deploy(localBin); err != nil {
			return nil, fmt.Errorf("deploy to %s: %w", nodeName, err)
		}
		deployed[nodeName] = true
	}

	return nil, nil
}

func startTarget(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	create := act.Params["create"] == "true"
	if err := tgt.Start(ctx, create); err != nil {
		return nil, fmt.Errorf("start_target %s: %w", act.Target, err)
	}

	return map[string]string{"value": fmt.Sprintf("pid=%d", tgt.PID())}, nil
}

func stopTarget(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}
	return nil, tgt.Stop(ctx)
}

func killTarget(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}
	return nil, tgt.Kill9()
}

func stopAllTargets(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	var lastErr error
	for name, tgt := range actx.Targets {
		if ht, ok := tgt.(*infra.HATarget); ok {
			if err := ht.Stop(ctx); err != nil {
				actx.Log("  stop %s: %v", name, err)
				lastErr = err
			}
			ht.Cleanup(ctx)
		}
	}

	// Aggressive: also kill stray iscsi-target processes on all nodes.
	if act.Params["aggressive"] == "true" {
		for _, n := range actx.Nodes {
			if node, ok := n.(*infra.Node); ok {
				node.Run(ctx, "pkill -9 iscsi-target 2>/dev/null")
			}
		}
		actx.Log("  killed stray iscsi-target processes")
	}

	return nil, lastErr
}

// killStale kills all iscsi-target-test processes on the node, regardless of
// whether they are tracked. Used at the start of scenarios to clean up
// leftovers from previous crashed runs.
func killStale(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("kill_stale: %w", err)
	}

	// Also clean up iSCSI sessions if this is a client node.
	process := act.Params["process"]
	if process == "" {
		process = "iscsi-target-test"
	}

	// Kill all matching processes.
	cmd := fmt.Sprintf("pkill -9 -f '%s' 2>/dev/null; sleep 0.5; pgrep -f '%s' || echo 'all_killed'", process, process)
	stdout, _, _, _ := node.Run(ctx, cmd)
	actx.Log("  kill_stale %s: %s", process, strings.TrimSpace(stdout))

	// If iscsi cleanup requested, clean up stale iSCSI sessions.
	if act.Params["iscsi_cleanup"] == "true" {
		node.Run(ctx, "sudo iscsiadm -m session -u 2>/dev/null; sudo iscsiadm -m node -o delete 2>/dev/null")
		actx.Log("  cleaned stale iSCSI sessions")
	}

	return nil, nil
}

func assign(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	epoch, _ := strconv.ParseUint(act.Params["epoch"], 10, 64)
	role := parseRole(act.Params["role"])
	leaseTTL := uint32(30000) // default 30s
	if ttlStr, ok := act.Params["lease_ttl"]; ok {
		if ms, err := parseDurationMs(ttlStr); err == nil {
			leaseTTL = ms
		}
	}

	if err := tgt.Assign(ctx, epoch, role, leaseTTL); err != nil {
		return nil, fmt.Errorf("assign %s: %w", act.Target, err)
	}
	return nil, nil
}

func setReplica(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	primaryTgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	replicaTgt, err := getHATarget(actx, act.Replica)
	if err != nil {
		return nil, fmt.Errorf("set_replica: replica %q: %w", act.Replica, err)
	}

	host := replicaTgt.HostAddr()
	dataAddr := fmt.Sprintf("%s:%d", host, replicaTgt.ReplicaData)
	ctrlAddr := fmt.Sprintf("%s:%d", host, replicaTgt.ReplicaCtrl)

	return nil, primaryTgt.SetReplica(ctx, dataAddr, ctrlAddr)
}

func waitRole(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	role := act.Params["role"]
	if role == "" {
		return nil, fmt.Errorf("wait_role: role param required")
	}

	timeoutCtx := ctx
	if t, ok := act.Params["timeout"]; ok {
		if d, err := parseDuration(t); err == nil {
			var cancel context.CancelFunc
			timeoutCtx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	return nil, tgt.WaitForRole(timeoutCtx, role)
}

func waitLSN(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	minLSN, _ := strconv.ParseUint(act.Params["min_lsn"], 10, 64)

	timeoutCtx := ctx
	if t, ok := act.Params["timeout"]; ok {
		if d, err := parseDuration(t); err == nil {
			var cancel context.CancelFunc
			timeoutCtx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}

	return nil, tgt.WaitForLSN(timeoutCtx, minLSN)
}

func statusAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	st, err := tgt.Status(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"value": fmt.Sprintf("epoch=%d role=%s lsn=%d lease=%v healthy=%v",
			st.Epoch, st.Role, st.WALHeadLSN, st.HasLease, st.Healthy),
	}, nil
}

func startRebuildServer(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	host := tgt.HostAddr()
	listenAddr := fmt.Sprintf("%s:%d", host, tgt.RebuildPort)
	return nil, tgt.StartRebuildEndpoint(ctx, listenAddr)
}

func startRebuildClient(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	// Get primary's rebuild address from the "primary" param or the replica target.
	primaryName := act.Params["primary"]
	if primaryName == "" {
		primaryName = act.Replica
	}
	primaryTgt, err := getHATarget(actx, primaryName)
	if err != nil {
		return nil, fmt.Errorf("start_rebuild_client: primary %q: %w", primaryName, err)
	}

	host := primaryTgt.HostAddr()
	rebuildAddr := fmt.Sprintf("%s:%d", host, primaryTgt.RebuildPort)

	epoch, _ := strconv.ParseUint(act.Params["epoch"], 10, 64)

	return nil, tgt.StartRebuildClient(ctx, rebuildAddr, epoch)
}

// getHATarget looks up the named target from the action context.
func getHATarget(actx *tr.ActionContext, name string) (*infra.HATarget, error) {
	if name == "" {
		return nil, fmt.Errorf("target name is required")
	}
	tgt, ok := actx.Targets[name]
	if !ok {
		return nil, fmt.Errorf("target %q not found", name)
	}
	ht, ok := tgt.(*infra.HATarget)
	if !ok {
		return nil, fmt.Errorf("target %q is not an HATarget", name)
	}
	return ht, nil
}

func parseRole(s string) uint32 {
	switch strings.ToLower(s) {
	case "primary":
		return 1 // RolePrimary
	case "replica":
		return 2 // RoleReplica
	case "stale":
		return 3 // RoleStale
	case "rebuilding":
		return 4 // RoleRebuilding
	case "draining":
		return 5 // RoleDraining
	default:
		return 0
	}
}

// toNativePath converts a Git Bash path (e.g. /c/work/foo) to the native OS path
// (e.g. C:\work\foo on Windows). No-op on Linux.
func toNativePath(p string) string {
	if runtime.GOOS != "windows" {
		return p
	}
	// Convert /c/work/foo → C:/work/foo
	if len(p) >= 3 && p[0] == '/' && p[2] == '/' {
		p = strings.ToUpper(string(p[1])) + ":" + p[2:]
	}
	return strings.ReplaceAll(p, "/", "\\")
}
