package actions

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/internal/blockapi"
	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterRecoveryActions registers recovery measurement and regression actions.
func RegisterRecoveryActions(r *tr.Registry) {
	r.RegisterFunc("measure_recovery", tr.TierBlock, measureRecovery)
	r.RegisterFunc("validate_recovery_regression", tr.TierBlock, validateRecoveryRegression)
	r.RegisterFunc("poll_shipper_state", tr.TierBlock, pollShipperState)
	r.RegisterFunc("measure_rebuild", tr.TierBlock, measureRebuild)
}

// RecoveryProfile captures the full recovery profile from fault to InSync.
type RecoveryProfile struct {
	FaultType   string            `json:"fault_type"`
	DurationMs  int64             `json:"duration_ms"`
	DegradedMs  int64             `json:"degraded_ms"`
	Path        string            `json:"path"` // catch-up, rebuild, failover, unknown
	Transitions []StateTransition `json:"transitions"`
	PollCount   int               `json:"poll_count"`
	Topology    string            `json:"topology,omitempty"`
	SyncMode    string            `json:"sync_mode,omitempty"`
	CommitID    string            `json:"commit_id,omitempty"`
}

// StateTransition records a single observed state change during recovery.
type StateTransition struct {
	FromState string `json:"from"`
	ToState   string `json:"to"`
	AtMs      int64  `json:"at_ms"` // ms since fault injection
}

// measureRecovery polls a block volume until healthy, recording the full
// recovery profile: duration, path, transitions, degraded window.
//
// Params:
//   - name: block volume name (required, or from volume_name var)
//   - master_url: master API (or from var)
//   - timeout: max wait (default: 120s)
//   - poll_interval: polling interval (default: 1s)
//   - fault_type: crash, kill, partition, failover, restart (for labeling)
//
// save_as outputs:
//   - {save_as}_duration_ms
//   - {save_as}_path
//   - {save_as}_degraded_ms
//   - {save_as}_transitions
//   - {save_as}_polls
//   - {save_as}_json (full profile)
func measureRecovery(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("measure_recovery: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		name = actx.Vars["volume_name"]
	}
	if name == "" {
		return nil, fmt.Errorf("measure_recovery: name param required")
	}

	timeoutStr := paramDefault(act.Params, "timeout", "120s")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, fmt.Errorf("measure_recovery: invalid timeout %q: %w", timeoutStr, err)
	}

	intervalStr := paramDefault(act.Params, "poll_interval", "1s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("measure_recovery: invalid poll_interval %q: %w", intervalStr, err)
	}

	faultType := paramDefault(act.Params, "fault_type", "unknown")

	profile := RecoveryProfile{
		FaultType: faultType,
		Topology:  actx.Vars["__topology"],
		SyncMode:  actx.Vars["__sync_mode"],
		CommitID:  actx.Vars["__git_sha"],
	}

	start := time.Now()
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastState string
	var lastPrimary string
	var degradedStart time.Time
	sawCatchUp := false
	sawRebuild := false
	sawFailover := false

	// Initial state probe (may fail if volume server is down).
	if info, err := client.LookupVolume(ctx, name); err == nil {
		lastState = classifyVolumeState(info)
		lastPrimary = info.VolumeServer
	} else {
		lastState = "unreachable"
	}

	if lastState != "healthy" {
		degradedStart = start
	}

	for {
		select {
		case <-deadline:
			profile.DurationMs = time.Since(start).Milliseconds()
			profile.PollCount++
			if !degradedStart.IsZero() {
				profile.DegradedMs += time.Since(degradedStart).Milliseconds()
			}
			profile.Path = classifyPath(sawCatchUp, sawRebuild, sawFailover)

			actx.Log("  measure_recovery: TIMEOUT after %dms (%d polls) path=%s",
				profile.DurationMs, profile.PollCount, profile.Path)
			return nil, fmt.Errorf("measure_recovery: %q not healthy after %s (%d polls, path=%s)",
				name, timeout, profile.PollCount, profile.Path)

		case <-ctx.Done():
			return nil, fmt.Errorf("measure_recovery: context cancelled")

		case <-ticker.C:
			profile.PollCount++
			now := time.Now()
			elapsed := now.Sub(start).Milliseconds()

			info, err := client.LookupVolume(ctx, name)
			if err != nil {
				newState := "unreachable"
				if newState != lastState {
					profile.Transitions = append(profile.Transitions, StateTransition{
						FromState: lastState,
						ToState:   newState,
						AtMs:      elapsed,
					})
					lastState = newState
				}
				actx.Log("  poll %d (%dms): %s (lookup error)", profile.PollCount, elapsed, newState)
				continue
			}

			currentState := classifyVolumeState(info)
			currentPrimary := info.VolumeServer

			// Detect state transition.
			if currentState != lastState {
				profile.Transitions = append(profile.Transitions, StateTransition{
					FromState: lastState,
					ToState:   currentState,
					AtMs:      elapsed,
				})

				// Track degraded window boundaries.
				if lastState == "healthy" && currentState != "healthy" {
					degradedStart = now
				}
				if lastState != "healthy" && currentState == "healthy" && !degradedStart.IsZero() {
					profile.DegradedMs += now.Sub(degradedStart).Milliseconds()
					degradedStart = time.Time{}
				}

				actx.Log("  poll %d (%dms): %s → %s", profile.PollCount, elapsed, lastState, currentState)
				lastState = currentState
			}

			// Detect failover (primary changed).
			if lastPrimary != "" && currentPrimary != "" && currentPrimary != lastPrimary {
				sawFailover = true
				actx.Log("  poll %d (%dms): primary changed %s → %s", profile.PollCount, elapsed, lastPrimary, currentPrimary)
			}
			lastPrimary = currentPrimary

			// Track recovery path from observed states.
			switch currentState {
			case "catching_up":
				sawCatchUp = true
			case "rebuilding":
				sawRebuild = true
			}

			// Check if healthy.
			if currentState == "healthy" {
				profile.DurationMs = elapsed
				profile.Path = classifyPath(sawCatchUp, sawRebuild, sawFailover)

				actx.Log("  measure_recovery: healthy after %dms (%d polls) path=%s degraded=%dms transitions=%d",
					profile.DurationMs, profile.PollCount, profile.Path,
					profile.DegradedMs, len(profile.Transitions))

				return profileToVars(profile), nil
			}
		}
	}
}

// classifyVolumeState maps VolumeInfo fields to a simple state string.
func classifyVolumeState(info *blockapi.VolumeInfo) string {
	if info.ReplicaDegraded {
		// Try to distinguish catch-up from rebuild from generic degraded.
		status := strings.ToLower(info.Status)
		switch {
		case strings.Contains(status, "catching") || strings.Contains(status, "catchup"):
			return "catching_up"
		case strings.Contains(status, "rebuild"):
			return "rebuilding"
		default:
			return "degraded"
		}
	}
	if info.ReplicaFactor > 1 && len(info.Replicas) == 0 {
		return "no_replicas"
	}
	return "healthy"
}

// classifyPath determines the recovery path from observed state flags.
func classifyPath(sawCatchUp, sawRebuild, sawFailover bool) string {
	switch {
	case sawFailover && sawRebuild:
		return "failover+rebuild"
	case sawFailover && sawCatchUp:
		return "failover+catch-up"
	case sawFailover:
		return "failover"
	case sawRebuild:
		return "rebuild"
	case sawCatchUp:
		return "catch-up"
	default:
		return "direct" // went straight from degraded/unreachable to healthy
	}
}

func profileToVars(p RecoveryProfile) map[string]string {
	vars := map[string]string{
		"duration_ms": strconv.FormatInt(p.DurationMs, 10),
		"path":        p.Path,
		"degraded_ms": strconv.FormatInt(p.DegradedMs, 10),
		"polls":       strconv.Itoa(p.PollCount),
	}

	// Transitions as readable string.
	var parts []string
	if len(p.Transitions) > 0 {
		parts = append(parts, p.Transitions[0].FromState)
		for _, t := range p.Transitions {
			parts = append(parts, t.ToState)
		}
	}
	vars["transitions"] = strings.Join(parts, "→")

	jsonBytes, _ := json.Marshal(p)
	vars["json"] = string(jsonBytes)

	return vars
}

// ShipperStateInfo mirrors the /debug/block/shipper JSON response.
type ShipperStateInfo struct {
	Path      string              `json:"path"`
	Role      string              `json:"role"`
	Epoch     uint64              `json:"epoch"`
	HeadLSN   uint64              `json:"head_lsn"`
	Degraded  bool                `json:"degraded"`
	Shippers  []ShipperReplicaInfo `json:"shippers"`
	Timestamp string              `json:"timestamp"`
}

// ShipperReplicaInfo is one shipper's state from the debug endpoint.
type ShipperReplicaInfo struct {
	DataAddr   string `json:"data_addr"`
	State      string `json:"state"`
	FlushedLSN uint64 `json:"flushed_lsn"`
}

// pollShipperState polls the VS's /debug/block/shipper endpoint for
// real-time shipper state. Unlike master-reported replica_degraded
// (heartbeat-lagged), this reads directly from the shipper's atomic
// state field — zero delay.
//
// Params:
//   - host: VS host (required, or from var)
//   - port: VS HTTP port (required, or from var)
//   - timeout: max wait (default: 60s)
//   - poll_interval: polling interval (default: 1s)
//   - expected_state: wait until shipper reaches this state (e.g., "in_sync", "degraded")
//     If empty, returns immediately with current state.
//   - volume_path: filter to specific volume (optional, matches path substring)
//
// save_as outputs:
//   - {save_as}_state: current shipper state (e.g., "in_sync", "degraded", "disconnected")
//   - {save_as}_head_lsn: WAL head LSN
//   - {save_as}_degraded: true/false
//   - {save_as}_flushed_lsn: replica's flushed LSN
//   - {save_as}_duration_ms: time to reach expected state (if waiting)
//   - {save_as}_json: full response JSON
func pollShipperState(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	host := act.Params["host"]
	if host == "" {
		host = actx.Vars["vs_host"]
	}
	port := act.Params["port"]
	if port == "" {
		port = actx.Vars["vs_port"]
	}
	if host == "" || port == "" {
		return nil, fmt.Errorf("poll_shipper_state: host and port params required")
	}

	node, err := GetNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("poll_shipper_state: %w", err)
	}

	expectedState := act.Params["expected_state"]
	volumePath := act.Params["volume_path"]

	timeoutStr := paramDefault(act.Params, "timeout", "60s")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, fmt.Errorf("poll_shipper_state: invalid timeout: %w", err)
	}

	intervalStr := paramDefault(act.Params, "poll_interval", "1s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("poll_shipper_state: invalid poll_interval: %w", err)
	}

	start := time.Now()
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// Query the debug endpoint via SSH curl.
		cmd := fmt.Sprintf("curl -s http://%s:%s/debug/block/shipper 2>&1", host, port)
		stdout, _, code, err := node.Run(ctx, cmd)
		if err != nil || code != 0 {
			if expectedState == "" {
				return nil, fmt.Errorf("poll_shipper_state: curl failed: code=%d err=%v", code, err)
			}
			// Waiting mode: VS might not be up yet, keep polling.
			select {
			case <-deadline:
				return nil, fmt.Errorf("poll_shipper_state: timeout waiting for %s (VS unreachable)", expectedState)
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		var infos []ShipperStateInfo
		if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &infos); err != nil {
			if expectedState == "" {
				return nil, fmt.Errorf("poll_shipper_state: parse JSON: %w", err)
			}
			select {
			case <-deadline:
				return nil, fmt.Errorf("poll_shipper_state: timeout (bad JSON)")
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Find the matching volume.
		var target *ShipperStateInfo
		for i := range infos {
			if volumePath == "" || strings.Contains(infos[i].Path, volumePath) {
				target = &infos[i]
				break
			}
		}

		if target == nil {
			if expectedState == "" {
				return map[string]string{
					"state":   "no_volume",
					"json":    stdout,
				}, nil
			}
			select {
			case <-deadline:
				return nil, fmt.Errorf("poll_shipper_state: timeout (volume not found)")
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Extract shipper state.
		shipperState := "no_shippers"
		var flushedLSN uint64
		if len(target.Shippers) > 0 {
			shipperState = target.Shippers[0].State
			flushedLSN = target.Shippers[0].FlushedLSN
		}

		vars := map[string]string{
			"state":       shipperState,
			"head_lsn":    strconv.FormatUint(target.HeadLSN, 10),
			"degraded":    strconv.FormatBool(target.Degraded),
			"flushed_lsn": strconv.FormatUint(flushedLSN, 10),
			"duration_ms": strconv.FormatInt(time.Since(start).Milliseconds(), 10),
			"json":        stdout,
		}

		// If not waiting for a specific state, return immediately.
		if expectedState == "" {
			return vars, nil
		}

		// Check if expected state reached.
		if shipperState == expectedState {
			actx.Log("  poll_shipper_state: %s reached after %dms",
				expectedState, time.Since(start).Milliseconds())
			return vars, nil
		}

		actx.Log("  poll_shipper_state: state=%s (waiting for %s)", shipperState, expectedState)

		select {
		case <-deadline:
			return nil, fmt.Errorf("poll_shipper_state: timeout waiting for %s (last=%s)",
				expectedState, shipperState)
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// RebuildProfile captures the full rebuild measurement.
type RebuildProfile struct {
	RebuildDurationMs    int64  `json:"rebuild_duration_ms"`
	SourceType           string `json:"source_type"`    // full_base, snapshot, resync, unknown
	SourceReason         string `json:"source_reason"`  // why this source (from logs or N/A)
	FallbackOccurred     bool   `json:"fallback_occurred"`
	DataIntegrity        string `json:"data_integrity"` // pass, fail, skipped
	RecoveryObservable   bool   `json:"recovery_observable"`
	PostRebuildStableMs  int64  `json:"post_rebuild_stable_ms"`
	Topology             string `json:"topology,omitempty"`
	SyncMode             string `json:"sync_mode,omitempty"`
	CommitID             string `json:"commit_id,omitempty"`
}

// measureRebuild measures a full rebuild cycle: from degraded/no-replica
// state to healthy. Polls via LookupVolume (master API).
//
// This is different from measure_recovery: measure_recovery starts from
// a healthy state and waits for recovery after a fault. measure_rebuild
// starts from a state where the replica needs full rebuild (not catch-up).
//
// Params:
//   - name: block volume name (required)
//   - master_url: master API (or from var)
//   - timeout: max wait (default: 300s — rebuilds can be slow)
//   - poll_interval: polling interval (default: 2s)
//
// save_as outputs:
//   - {save_as}_duration_ms: time to reach healthy
//   - {save_as}_source_type: full_base / snapshot / unknown
//   - {save_as}_integrity: pass / fail / skipped
//   - {save_as}_json: full profile
func measureRebuild(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	client, err := blockAPIClient(actx, act)
	if err != nil {
		return nil, fmt.Errorf("measure_rebuild: %w", err)
	}

	name := act.Params["name"]
	if name == "" {
		name = actx.Vars["volume_name"]
	}
	if name == "" {
		return nil, fmt.Errorf("measure_rebuild: name param required")
	}

	timeoutStr := paramDefault(act.Params, "timeout", "300s")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return nil, fmt.Errorf("measure_rebuild: invalid timeout: %w", err)
	}

	intervalStr := paramDefault(act.Params, "poll_interval", "2s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("measure_rebuild: invalid poll_interval: %w", err)
	}

	profile := RebuildProfile{
		SourceType:       "unknown",
		DataIntegrity:    "skipped",
		Topology:         actx.Vars["__topology"],
		SyncMode:         actx.Vars["__sync_mode"],
		CommitID:         actx.Vars["__git_sha"],
	}

	start := time.Now()
	deadline := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	poll := 0
	var lastState string

	for {
		select {
		case <-deadline:
			profile.RebuildDurationMs = time.Since(start).Milliseconds()
			actx.Log("  measure_rebuild: TIMEOUT after %dms (%d polls, last=%s)",
				profile.RebuildDurationMs, poll, lastState)
			return nil, fmt.Errorf("measure_rebuild: %q not healthy after %s (%d polls, last=%s)",
				name, timeout, poll, lastState)

		case <-ctx.Done():
			return nil, fmt.Errorf("measure_rebuild: context cancelled")

		case <-ticker.C:
			poll++
			info, err := client.LookupVolume(ctx, name)
			if err != nil {
				lastState = "unreachable"
				actx.Log("  rebuild poll %d: unreachable", poll)
				continue
			}

			currentState := classifyVolumeState(info)
			if currentState != lastState {
				actx.Log("  rebuild poll %d (%dms): %s → %s",
					poll, time.Since(start).Milliseconds(), lastState, currentState)
			}
			lastState = currentState

			// Try to detect rebuild source from status field.
			status := strings.ToLower(info.Status)
			if strings.Contains(status, "rebuild") {
				if profile.SourceType == "unknown" {
					profile.SourceType = "full_base" // default assumption
					profile.RecoveryObservable = true
				}
			}

			if currentState == "healthy" {
				profile.RebuildDurationMs = time.Since(start).Milliseconds()

				actx.Log("  measure_rebuild: healthy after %dms (%d polls) source=%s",
					profile.RebuildDurationMs, poll, profile.SourceType)

				vars := map[string]string{
					"duration_ms": strconv.FormatInt(profile.RebuildDurationMs, 10),
					"source_type": profile.SourceType,
					"integrity":   profile.DataIntegrity,
					"polls":       strconv.Itoa(poll),
				}
				jsonBytes, _ := json.Marshal(profile)
				vars["json"] = string(jsonBytes)
				return vars, nil
			}
		}
	}
}

// validateRecoveryRegression checks a recovery profile against baseline expectations.
//
// Params:
//   - profile_var: var prefix from measure_recovery save_as (required)
//   - baseline_duration_ms: expected recovery duration baseline (required)
//   - tolerance_pct: allowed regression percentage (default: 20)
//   - expected_path: expected recovery path (optional, e.g. "catch-up")
func validateRecoveryRegression(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	prefix := act.Params["profile_var"]
	if prefix == "" {
		return nil, fmt.Errorf("validate_recovery_regression: profile_var param required")
	}

	baselineStr := act.Params["baseline_duration_ms"]
	if baselineStr == "" {
		return nil, fmt.Errorf("validate_recovery_regression: baseline_duration_ms param required")
	}
	baseline, err := strconv.ParseInt(baselineStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("validate_recovery_regression: invalid baseline: %w", err)
	}

	tolerancePct := ParseInt(act.Params["tolerance_pct"], 20)

	actualStr := actx.Vars[prefix+"_duration_ms"]
	if actualStr == "" {
		return nil, fmt.Errorf("validate_recovery_regression: var %s_duration_ms not found", prefix)
	}
	actual, err := strconv.ParseInt(actualStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("validate_recovery_regression: invalid duration: %w", err)
	}

	threshold := baseline + (baseline * int64(tolerancePct) / 100)
	var failures []string

	if actual > threshold {
		failures = append(failures, fmt.Sprintf("duration %dms exceeds baseline %dms + %d%% tolerance (threshold=%dms)",
			actual, baseline, tolerancePct, threshold))
	}

	// Check expected path if specified.
	if expectedPath := act.Params["expected_path"]; expectedPath != "" {
		actualPath := actx.Vars[prefix+"_path"]
		if actualPath != expectedPath {
			failures = append(failures, fmt.Sprintf("path %q != expected %q", actualPath, expectedPath))
		}
	}

	if len(failures) > 0 {
		return nil, fmt.Errorf("validate_recovery_regression: %s", strings.Join(failures, "; "))
	}

	actx.Log("  recovery regression OK: %dms <= %dms (baseline %dms + %d%%)",
		actual, threshold, baseline, tolerancePct)
	return map[string]string{"value": "ok"}, nil
}
