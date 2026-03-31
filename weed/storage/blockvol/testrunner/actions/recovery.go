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
