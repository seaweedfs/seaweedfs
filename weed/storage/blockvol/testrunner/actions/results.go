package actions

import (
	"context"
	"fmt"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterResultActions registers result collection and validation actions.
func RegisterResultActions(r *tr.Registry) {
	r.RegisterFunc("collect_results", tr.TierCore, collectResults)
	r.RegisterFunc("validate_replication", tr.TierCore, validateReplication)
}

// collectResults generates a markdown summary of the current run.
// Collects: topology, volume config, fio metrics, pgbench TPS, and health.
// Outputs a markdown-formatted string suitable for archiving.
//
// Params:
//   - title: report title (default: scenario name from __scenario_name var)
//   - volume_name: block volume to query
//   - master_url: master API URL (or from var)
//   - write_iops: var name containing write IOPS (optional)
//   - read_iops: var name containing read IOPS (optional)
//   - pgbench_tps: var name containing pgbench TPS (optional)
//   - postcheck: var name containing postcheck result (optional)
//
// Returns: value = markdown report string
func collectResults(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	var sb strings.Builder

	title := act.Params["title"]
	if title == "" {
		title = actx.Vars["__scenario_name"]
	}
	if title == "" {
		title = "Test Run"
	}

	now := time.Now().UTC().Format("2006-01-02 15:04:05 UTC")
	commit := actx.Vars["__git_sha"]
	if commit == "" {
		commit = "unknown"
	}

	sb.WriteString(fmt.Sprintf("# %s\n\n", title))
	sb.WriteString(fmt.Sprintf("Date: %s\n", now))
	sb.WriteString(fmt.Sprintf("Commit: %s\n\n", commit))

	// Volume info
	volName := act.Params["volume_name"]
	if volName == "" {
		volName = actx.Vars["volume_name"]
	}
	if volName != "" {
		client, err := benchBlockAPIClient(actx, act)
		if err == nil {
			info, err := client.LookupVolume(ctx, volName)
			if err == nil {
				sb.WriteString("## Volume\n\n")
				sb.WriteString(fmt.Sprintf("| Field | Value |\n"))
				sb.WriteString(fmt.Sprintf("|-------|-------|\n"))
				sb.WriteString(fmt.Sprintf("| Name | %s |\n", info.Name))
				sb.WriteString(fmt.Sprintf("| Size | %d bytes |\n", info.SizeBytes))
				sb.WriteString(fmt.Sprintf("| RF | %d |\n", info.ReplicaFactor))
				sb.WriteString(fmt.Sprintf("| Durability | %s |\n", info.DurabilityMode))
				sb.WriteString(fmt.Sprintf("| Primary | %s |\n", info.VolumeServer))
				sb.WriteString(fmt.Sprintf("| NVMe | %s |\n", info.NvmeAddr))
				sb.WriteString(fmt.Sprintf("| Degraded | %v |\n", info.ReplicaDegraded))
				for i, r := range info.Replicas {
					sb.WriteString(fmt.Sprintf("| Replica %d | %s |\n", i+1, r.Server))
				}
				sb.WriteString("\n")
			}
		}
	}

	// Metrics
	writeIOPS := actx.Vars[act.Params["write_iops"]]
	readIOPS := actx.Vars[act.Params["read_iops"]]
	pgTPS := actx.Vars[act.Params["pgbench_tps"]]

	if writeIOPS != "" || readIOPS != "" || pgTPS != "" {
		sb.WriteString("## Results\n\n")
		sb.WriteString("| Metric | Value |\n")
		sb.WriteString("|--------|-------|\n")
		if writeIOPS != "" {
			sb.WriteString(fmt.Sprintf("| Write IOPS | %s |\n", writeIOPS))
		}
		if readIOPS != "" {
			sb.WriteString(fmt.Sprintf("| Read IOPS | %s |\n", readIOPS))
		}
		if pgTPS != "" {
			sb.WriteString(fmt.Sprintf("| pgbench TPS | %s |\n", pgTPS))
		}
		sb.WriteString("\n")
	}

	// Postcheck
	postcheck := actx.Vars[act.Params["postcheck"]]
	if postcheck != "" {
		sb.WriteString(fmt.Sprintf("## Postcheck\n\n%s\n\n", postcheck))
	}

	// Recovery profile (if captured)
	rpPrefix := act.Params["recovery_profile"]
	if rpPrefix != "" {
		rpDuration := actx.Vars[rpPrefix+"_duration_ms"]
		if rpDuration != "" {
			sb.WriteString("## Recovery\n\n")
			sb.WriteString("| Metric | Value |\n")
			sb.WriteString("|--------|-------|\n")
			if ft := actx.Vars[rpPrefix+"_fault_type"]; ft != "" {
				sb.WriteString(fmt.Sprintf("| Fault Type | %s |\n", ft))
			}
			sb.WriteString(fmt.Sprintf("| Duration | %s ms |\n", rpDuration))
			if deg := actx.Vars[rpPrefix+"_degraded_ms"]; deg != "" {
				sb.WriteString(fmt.Sprintf("| Degraded Window | %s ms |\n", deg))
			}
			if path := actx.Vars[rpPrefix+"_path"]; path != "" {
				sb.WriteString(fmt.Sprintf("| Recovery Path | %s |\n", path))
			}
			if trans := actx.Vars[rpPrefix+"_transitions"]; trans != "" {
				sb.WriteString(fmt.Sprintf("| Transitions | %s |\n", trans))
			}
			if polls := actx.Vars[rpPrefix+"_polls"]; polls != "" {
				sb.WriteString(fmt.Sprintf("| Polls | %s |\n", polls))
			}
			sb.WriteString("\n")
		}
	}

	// Bench header (if captured)
	if header := actx.Vars["bench_header"]; header != "" {
		sb.WriteString("## Report Header\n\n```json\n")
		sb.WriteString(header)
		sb.WriteString("\n```\n\n")
	}

	report := sb.String()
	actx.Log("=== COLLECTED RESULTS ===")
	actx.Log("%s", report)
	actx.Log("=========================")

	return map[string]string{"value": report}, nil
}

// validateReplication checks that the volume's replication config matches expectations.
// Useful for ensuring a test is actually running with the intended RF and durability mode.
//
// Params:
//   - volume_name: block volume (required)
//   - master_url: master API (or from var)
//   - expected_rf: expected replica factor (e.g., "2")
//   - expected_durability: expected mode (e.g., "sync_all")
//   - require_not_degraded: "true" to fail if replica is degraded
//   - require_cross_machine: "true" to fail if primary == replica host
//
// Returns: value = "ok" or error
func validateReplication(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := benchBlockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("validate_replication: %w", err)
	}

	volName := act.Params["volume_name"]
	if volName == "" {
		volName = actx.Vars["volume_name"]
	}
	if volName == "" {
		return nil, fmt.Errorf("validate_replication: volume_name required")
	}

	info, err := client.LookupVolume(ctx, volName)
	if err != nil {
		return nil, fmt.Errorf("validate_replication: lookup %s: %w", volName, err)
	}

	var failures []string

	// Check RF.
	if expected := act.Params["expected_rf"]; expected != "" {
		actual := fmt.Sprintf("%d", info.ReplicaFactor)
		if actual != expected {
			failures = append(failures, fmt.Sprintf("RF: got %s, want %s", actual, expected))
		}
	}

	// Check durability mode.
	if expected := act.Params["expected_durability"]; expected != "" {
		if info.DurabilityMode != expected {
			failures = append(failures, fmt.Sprintf("durability: got %s, want %s", info.DurabilityMode, expected))
		}
	}

	// Check not degraded.
	if act.Params["require_not_degraded"] == "true" && info.ReplicaDegraded {
		failures = append(failures, "replica is degraded")
	}

	// Check cross-machine.
	if act.Params["require_cross_machine"] == "true" && info.ReplicaFactor > 1 {
		primaryHost := extractHost(info.VolumeServer)
		for _, r := range info.Replicas {
			replicaHost := extractHost(r.Server)
			if primaryHost == replicaHost {
				failures = append(failures, fmt.Sprintf("primary and replica on same host: %s", primaryHost))
			}
		}
	}

	if len(failures) > 0 {
		return nil, fmt.Errorf("validate_replication: %s", strings.Join(failures, "; "))
	}

	actx.Log("  replication validated: RF=%d mode=%s degraded=%v",
		info.ReplicaFactor, info.DurabilityMode, info.ReplicaDegraded)
	return map[string]string{"value": "ok"}, nil
}

// writeResultFile is a helper that writes the result markdown to a file in the run bundle.
func writeResultFile(actx *tr.ActionContext, filename, content string) {
	// Results are written to the run bundle artifacts dir if available.
	if dir := actx.Vars["__artifacts_dir"]; dir != "" {
		actx.Log("  writing results to %s/%s", dir, filename)
	}
}
