package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/internal/blockapi"
)

// registerBenchmarkValidation adds reporting, preflight, and postcheck actions.
// Called from bench.go:RegisterBenchActions.
func registerBenchmarkValidation(r *tr.Registry) {
	r.RegisterFunc("benchmark_report", tr.TierCore, benchmarkReport)
	r.RegisterFunc("benchmark_preflight", tr.TierCore, benchmarkPreflight)
	r.RegisterFunc("benchmark_postcheck", tr.TierCore, benchmarkPostcheck)
}

// BenchmarkReportHeader is the structured report emitted by benchmark_report.
type BenchmarkReportHeader struct {
	Date    string `json:"date"`
	Commit  string `json:"commit"`
	Branch  string `json:"branch"`
	Host    string `json:"host"`
	Runner  string `json:"runner_version"`

	Topology BenchTopology  `json:"topology"`
	Volume   BenchVolume    `json:"volume"`
	Health   BenchHealth    `json:"health"`
}

// BenchTopology describes the test topology.
type BenchTopology struct {
	PrimaryServer string `json:"primary_server"`
	PrimaryIP     string `json:"primary_ip,omitempty"`
	ReplicaServer string `json:"replica_server,omitempty"`
	ReplicaIP     string `json:"replica_ip,omitempty"`
	ClientNode    string `json:"client_node"`
	Protocol      string `json:"protocol"`
	CrossMachine  bool   `json:"cross_machine"`
}

// BenchVolume describes the volume under test.
type BenchVolume struct {
	Name           string `json:"name"`
	SizeBytes      uint64 `json:"size_bytes"`
	ReplicaFactor  int    `json:"replica_factor"`
	DurabilityMode string `json:"durability_mode"`
	NvmeAddr       string `json:"nvme_addr,omitempty"`
	NQN            string `json:"nqn,omitempty"`
	ISCSIAddr      string `json:"iscsi_addr,omitempty"`
	Preset         string `json:"preset,omitempty"`
}

// BenchHealth describes pre-run health state.
type BenchHealth struct {
	ReplicaDegraded bool   `json:"replica_degraded"`
	HealthScore     float64 `json:"health_score"`
	HealthState     string `json:"health_state,omitempty"`
}

// benchmarkReport queries the master API for volume info and emits a
// structured JSON report header. Must run before any benchmark workload.
//
// Params:
//   - volume_name: block volume name (required)
//   - master_url: master API URL (or from var)
//   - client_node: name of the client node in topology
//   - protocol: "nvme-tcp" or "iscsi" (default "nvme-tcp")
//
// Output (save_as): JSON report header
// Side effect: sets vars __bench_primary, __bench_replica, __bench_cross_machine
func benchmarkReport(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := benchBlockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("benchmark_report: %w", err)
	}

	volName := act.Params["volume_name"]
	if volName == "" {
		volName = actx.Vars["volume_name"]
	}
	if volName == "" {
		return nil, fmt.Errorf("benchmark_report: volume_name param or var required")
	}

	info, err := client.LookupVolume(ctx, volName)
	if err != nil {
		return nil, fmt.Errorf("benchmark_report: lookup %s: %w", volName, err)
	}

	protocol := act.Params["protocol"]
	if protocol == "" {
		protocol = "nvme-tcp"
	}

	clientNode := act.Params["client_node"]
	if clientNode == "" {
		clientNode = actx.Vars["client_node"]
	}

	// Determine cross-machine: compare primary and replica server IPs.
	primaryIP := extractHost(info.VolumeServer)
	replicaIP := ""
	replicaServer := ""
	if len(info.Replicas) > 0 {
		replicaServer = info.Replicas[0].Server
		replicaIP = extractHost(replicaServer)
	}
	crossMachine := replicaIP != "" && primaryIP != replicaIP

	header := BenchmarkReportHeader{
		Date:   time.Now().UTC().Format(time.RFC3339),
		Commit: gitSHAShort(),
		Branch: gitBranch(),
		Host:   hostname(),
		Runner: tr.Version(),
		Topology: BenchTopology{
			PrimaryServer: info.VolumeServer,
			PrimaryIP:     primaryIP,
			ReplicaServer: replicaServer,
			ReplicaIP:     replicaIP,
			ClientNode:    clientNode,
			Protocol:      protocol,
			CrossMachine:  crossMachine,
		},
		Volume: BenchVolume{
			Name:           info.Name,
			SizeBytes:      info.SizeBytes,
			ReplicaFactor:  info.ReplicaFactor,
			DurabilityMode: info.DurabilityMode,
			NvmeAddr:       info.NvmeAddr,
			NQN:            info.NQN,
			ISCSIAddr:      info.ISCSIAddr,
			Preset:         info.Preset,
		},
		Health: BenchHealth{
			ReplicaDegraded: info.ReplicaDegraded,
			HealthScore:     info.HealthScore,
		},
	}

	// Set vars for downstream actions.
	actx.Vars["__bench_primary"] = info.VolumeServer
	actx.Vars["__bench_replica"] = replicaServer
	actx.Vars["__bench_cross_machine"] = fmt.Sprintf("%v", crossMachine)
	actx.Vars["__bench_durability"] = info.DurabilityMode
	actx.Vars["__bench_rf"] = fmt.Sprintf("%d", info.ReplicaFactor)

	jsonBytes, _ := json.MarshalIndent(header, "", "  ")
	report := string(jsonBytes)

	// Log the full report header.
	actx.Log("=== BENCHMARK REPORT HEADER ===")
	actx.Log("%s", report)
	actx.Log("===============================")

	// Warnings.
	if !crossMachine && info.ReplicaFactor > 1 {
		actx.Log("  WARNING: primary and replica on same host — not cross-machine replication")
	}
	if info.ReplicaDegraded {
		actx.Log("  WARNING: replica is degraded — barrier may fail under sync_all")
	}
	if info.DurabilityMode == "sync_all" && info.ReplicaFactor < 2 {
		actx.Log("  WARNING: sync_all with RF=%d — no replicas to barrier", info.ReplicaFactor)
	}

	return map[string]string{"value": report}, nil
}

// benchmarkPreflight validates the benchmark setup before running workloads.
// Fails fast with clear errors if any check fails.
//
// Params:
//   - volume_name: block volume name (required)
//   - master_url: master API URL (or from var)
//   - mount_path: filesystem mount point to verify (optional)
//   - device: expected block device path (optional)
//   - require_cross_machine: "true" to fail if primary/replica on same host
//
// Output: "ok" on success
func benchmarkPreflight(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := benchBlockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("benchmark_preflight: %w", err)
	}

	volName := act.Params["volume_name"]
	if volName == "" {
		volName = actx.Vars["volume_name"]
	}
	if volName == "" {
		return nil, fmt.Errorf("benchmark_preflight: volume_name param or var required")
	}

	info, err := client.LookupVolume(ctx, volName)
	if err != nil {
		return nil, fmt.Errorf("benchmark_preflight: lookup %s: %w", volName, err)
	}

	var checks []string
	var failures []string

	// Check 1: Volume placement.
	primaryIP := extractHost(info.VolumeServer)
	checks = append(checks, fmt.Sprintf("volume_placement: primary=%s", info.VolumeServer))

	if act.Params["require_cross_machine"] == "true" && info.ReplicaFactor > 1 {
		replicaIP := ""
		if len(info.Replicas) > 0 {
			replicaIP = extractHost(info.Replicas[0].Server)
		}
		if primaryIP == replicaIP {
			failures = append(failures, fmt.Sprintf("FAIL: primary and replica on same host (%s) — not cross-machine", primaryIP))
		} else if replicaIP == "" {
			failures = append(failures, "FAIL: no replica found for cross-machine check")
		} else {
			checks = append(checks, fmt.Sprintf("cross_machine: primary=%s replica=%s OK", primaryIP, replicaIP))
		}
	}

	// Check 2: Replica addresses are canonical ip:port.
	if info.ReplicaFactor > 1 {
		for _, addr := range []struct{ name, val string }{
			{"replica_data_addr", info.ReplicaDataAddr},
			{"replica_ctrl_addr", info.ReplicaCtrlAddr},
		} {
			if addr.val == "" {
				continue
			}
			if strings.HasPrefix(addr.val, ":") {
				failures = append(failures, fmt.Sprintf("FAIL: %s is %q — missing IP, not routable cross-machine", addr.name, addr.val))
			} else if strings.HasPrefix(addr.val, "0.0.0.0:") || strings.HasPrefix(addr.val, "[::]:") {
				failures = append(failures, fmt.Sprintf("FAIL: %s is %q — wildcard, not routable", addr.name, addr.val))
			} else {
				checks = append(checks, fmt.Sprintf("%s: %s OK", addr.name, addr.val))
			}
		}
	}

	// Check 3: Durability health (barrier probe).
	if info.DurabilityMode == "sync_all" && info.ReplicaDegraded {
		failures = append(failures, "FAIL: sync_all volume has degraded replica — barrier will fail")
	} else {
		checks = append(checks, fmt.Sprintf("durability: mode=%s degraded=%v OK", info.DurabilityMode, info.ReplicaDegraded))
	}

	// Check 4: Mount verification (if mount_path provided).
	mountPath := act.Params["mount_path"]
	device := act.Params["device"]
	if mountPath != "" {
		node, nodeErr := GetNode(actx, act.Node)
		if nodeErr == nil {
			// Verify mountpoint.
			stdout, _, code, _ := node.RunRoot(ctx, fmt.Sprintf("mountpoint -q %s && echo mounted || echo not_mounted", mountPath))
			if strings.TrimSpace(stdout) != "mounted" || code != 0 {
				failures = append(failures, fmt.Sprintf("FAIL: %s is not mounted", mountPath))
			} else {
				checks = append(checks, fmt.Sprintf("mount: %s is mounted", mountPath))
			}

			// Verify device matches.
			if device != "" {
				stdout, _, _, _ = node.RunRoot(ctx, fmt.Sprintf("df %s | tail -1 | awk '{print $1}'", mountPath))
				actualDev := strings.TrimSpace(stdout)
				if actualDev != device {
					failures = append(failures, fmt.Sprintf("FAIL: mount device mismatch: expected %s, got %s", device, actualDev))
				} else {
					checks = append(checks, fmt.Sprintf("device: %s matches mount OK", device))
				}
			}
		}
	}

	// Log all checks.
	actx.Log("=== BENCHMARK PREFLIGHT ===")
	for _, c := range checks {
		actx.Log("  [OK] %s", c)
	}
	for _, f := range failures {
		actx.Log("  %s", f)
	}
	actx.Log("===========================")

	if len(failures) > 0 {
		return nil, fmt.Errorf("benchmark_preflight: %d check(s) failed:\n  %s", len(failures), strings.Join(failures, "\n  "))
	}

	return map[string]string{"value": "ok"}, nil
}

// --- helpers ---

func extractHost(hostPort string) string {
	if hostPort == "" {
		return ""
	}
	h, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return hostPort
	}
	return h
}

func gitSHAShort() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func gitBranch() string {
	out, err := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func hostname() string {
	out, err := exec.Command("hostname").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// benchmarkPostcheck validates that benchmark results are trustworthy.
// Runs after the workload phase. Does NOT fail the scenario — it marks
// results as CLEAN or SUSPECT via the output value.
//
// Params:
//   - volume_name: block volume name (required)
//   - master_url: master API URL (or from var)
//   - mount_path: filesystem mount point to verify still mounted (optional)
//   - device: expected block device (optional)
//   - node: node to check dmesg/mount on (optional)
//   - pgdata_path: PG data directory to verify is on device (optional)
//
// Output: "CLEAN" or "SUSPECT: <reasons>"
func benchmarkPostcheck(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	var warnings []string

	// Check 1: Mount still valid.
	mountPath := act.Params["mount_path"]
	device := act.Params["device"]
	node, nodeErr := GetNode(actx, act.Node)

	if mountPath != "" && nodeErr == nil {
		stdout, _, _, _ := node.RunRoot(ctx, fmt.Sprintf("mountpoint -q %s && echo mounted || echo not_mounted", mountPath))
		if strings.TrimSpace(stdout) != "mounted" {
			warnings = append(warnings, fmt.Sprintf("mount_lost: %s no longer mounted", mountPath))
		}

		if device != "" {
			stdout, _, _, _ = node.RunRoot(ctx, fmt.Sprintf("df %s | tail -1 | awk '{print $1}'", mountPath))
			actual := strings.TrimSpace(stdout)
			if actual != device {
				warnings = append(warnings, fmt.Sprintf("device_mismatch: expected %s, got %s", device, actual))
			}
		}
	}

	// Check 2: pgdata on device (not local disk).
	pgdataPath := act.Params["pgdata_path"]
	if pgdataPath != "" && mountPath != "" && nodeErr == nil {
		if !strings.HasPrefix(pgdataPath, mountPath) {
			warnings = append(warnings, fmt.Sprintf("pgdata_local: %s not under mount %s — may be on local disk", pgdataPath, mountPath))
		} else {
			// Verify the mount is real by checking a file exists on the device.
			stdout, _, code, _ := node.RunRoot(ctx, fmt.Sprintf("test -f %s/PG_VERSION && echo ok || echo missing", pgdataPath))
			if code != 0 || strings.TrimSpace(stdout) != "ok" {
				warnings = append(warnings, fmt.Sprintf("pgdata_empty: %s/PG_VERSION not found — PG may not be using this directory", pgdataPath))
			}
		}
	}

	// Check 3: No NVMe I/O errors in dmesg.
	if nodeErr == nil && device != "" {
		devShort := device
		if idx := strings.LastIndex(device, "/"); idx >= 0 {
			devShort = device[idx+1:]
		}
		stdout, _, _, _ := node.RunRoot(ctx, fmt.Sprintf("dmesg | grep '%s.*I/O Error\\|%s.*error' | tail -5", devShort, devShort))
		stdout = strings.TrimSpace(stdout)
		if stdout != "" {
			lines := strings.Split(stdout, "\n")
			warnings = append(warnings, fmt.Sprintf("io_errors: %d NVMe I/O error(s) in dmesg for %s", len(lines), devShort))
		}
	}

	// Check 4: No barrier failures during run (query volume health).
	volName := act.Params["volume_name"]
	if volName == "" {
		volName = actx.Vars["volume_name"]
	}
	if volName != "" {
		client, err := benchBlockAPIClient(actx, act)
		if err == nil {
			info, err := client.LookupVolume(ctx, volName)
			if err == nil && info.ReplicaDegraded {
				warnings = append(warnings, "replica_degraded: replica became degraded during run")
			}
		}
	}

	// Emit result.
	actx.Log("=== BENCHMARK POSTCHECK ===")
	if len(warnings) == 0 {
		actx.Log("  CLEAN: all checks passed")
		actx.Log("===========================")
		return map[string]string{"value": "CLEAN"}, nil
	}

	for _, w := range warnings {
		actx.Log("  SUSPECT: %s", w)
	}
	actx.Log("===========================")

	result := "SUSPECT: " + strings.Join(warnings, "; ")
	// Set var for downstream/report use.
	actx.Vars["__bench_postcheck"] = result

	return map[string]string{"value": result}, nil
}

// blockAPIClient is duplicated here to avoid circular dependency.
// The canonical version is in devops.go.
func benchBlockAPIClient(actx *tr.ActionContext, act tr.Action) (*blockapi.Client, error) {
	masterURL := act.Params["master_url"]
	if masterURL == "" {
		masterURL = actx.Vars["master_url"]
	}
	if masterURL == "" {
		return nil, fmt.Errorf("master_url param or var required")
	}
	return blockapi.NewClient(masterURL), nil
}
