package testrunner

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

var varPattern = regexp.MustCompile(`\{\{\s*(\w+)\s*\}\}`)

// Engine executes a Scenario using the given Registry.
type Engine struct {
	registry *Registry
	log      func(format string, args ...interface{})
}

// NewEngine creates an engine with the given registry and logger.
func NewEngine(registry *Registry, log func(format string, args ...interface{})) *Engine {
	if log == nil {
		log = func(string, ...interface{}) {}
	}
	return &Engine{registry: registry, log: log}
}

// Run executes the scenario end-to-end and returns the result.
func (e *Engine) Run(ctx context.Context, s *Scenario, actx *ActionContext) *ScenarioResult {
	start := time.Now()
	result := &ScenarioResult{
		Name:   s.Name,
		Status: StatusPass,
	}

	// Apply scenario timeout.
	if s.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Timeout.Duration)
		defer cancel()
	}

	// Seed vars from env (merge: env provides defaults, existing vars win).
	if actx.Vars == nil {
		actx.Vars = make(map[string]string)
	}
	for k, v := range s.Env {
		if _, exists := actx.Vars[k]; !exists {
			actx.Vars[k] = v
		}
	}

	// Allocate a unique per-run temp directory (T6).
	if actx.TempRoot == "" {
		actx.TempRoot = fmt.Sprintf("/tmp/sw-run-%s-%d", s.Name, start.UnixMilli())
	}
	actx.Vars["__temp_dir"] = actx.TempRoot

	// Separate always-phases for deferred cleanup.
	var normalPhases, alwaysPhases []Phase
	for _, p := range s.Phases {
		if p.Always {
			alwaysPhases = append(alwaysPhases, p)
		} else {
			normalPhases = append(normalPhases, p)
		}
	}

	// Execute normal phases sequentially, expanding repeat.
	failed := false
	for _, phase := range normalPhases {
		count := phase.Repeat
		if count <= 0 {
			count = 1
		}

		// Collect save_as values across iterations for aggregation.
		var iterValues map[string][]float64
		if count > 1 && phase.Aggregate != "none" {
			iterValues = make(map[string][]float64)
		}

		for iter := 1; iter <= count; iter++ {
			iterPhase := phase
			if phase.Repeat > 1 {
				iterPhase.Name = fmt.Sprintf("%s[%d/%d]", phase.Name, iter, count)
			}
			pr := e.runPhase(ctx, actx, iterPhase)
			result.Phases = append(result.Phases, pr)

			// Collect numeric save_as values for aggregation.
			if iterValues != nil {
				for _, act := range phase.Actions {
					if act.SaveAs != "" {
						if v, ok := actx.Vars[act.SaveAs]; ok {
							if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
								iterValues[act.SaveAs] = append(iterValues[act.SaveAs], f)
							}
						}
					}
				}
			}

			if pr.Status == StatusFail {
				failed = true
				result.Status = StatusFail
				result.Error = fmt.Sprintf("phase %q failed: %s", iterPhase.Name, pr.Error)
				break
			}
		}

		// Aggregate collected values across iterations.
		if iterValues != nil && !failed {
			trimPct := phase.TrimPct
			// 0 means no trimming (explicit or default). Only auto-default
			// when repeat >= 5 and trim_pct was not set.
			if trimPct == 0 && count >= 5 {
				trimPct = 20
			}
			agg := phase.Aggregate
			if agg == "" {
				agg = "median" // default aggregation method
			}
			for varName, values := range iterValues {
				if len(values) < 2 {
					continue
				}
				trimmed := trimOutliers(values, trimPct)
				stats := ComputeStats(trimmed)

				// Store aggregate results as vars.
				switch agg {
				case "median":
					actx.Vars[varName] = strconv.FormatFloat(stats.P50, 'f', 2, 64)
				case "mean":
					actx.Vars[varName] = strconv.FormatFloat(stats.Mean, 'f', 2, 64)
				}
				actx.Vars[varName+"_median"] = strconv.FormatFloat(stats.P50, 'f', 2, 64)
				actx.Vars[varName+"_mean"] = strconv.FormatFloat(stats.Mean, 'f', 2, 64)
				actx.Vars[varName+"_stddev"] = strconv.FormatFloat(stats.StdDev, 'f', 2, 64)
				actx.Vars[varName+"_min"] = strconv.FormatFloat(stats.Min, 'f', 2, 64)
				actx.Vars[varName+"_max"] = strconv.FormatFloat(stats.Max, 'f', 2, 64)
				actx.Vars[varName+"_n"] = strconv.Itoa(stats.Count)

				// Store all raw values as comma-separated string.
				parts := make([]string, len(values))
				for i, v := range values {
					parts[i] = strconv.FormatFloat(v, 'f', 2, 64)
				}
				actx.Vars[varName+"_all"] = strings.Join(parts, ",")

				e.log("  [aggregate] %s: n=%d median=%.2f mean=%.2f stddev=%.2f (trimmed %d%% from %d samples)",
					varName, stats.Count, stats.P50, stats.Mean, stats.StdDev, trimPct, len(values))
			}
		}

		if failed {
			break
		}
	}

	// Always-phases run regardless of failure, with a fresh 60s context
	// so they can complete even if the main context was canceled.
	cleanupCtx := context.Background()
	cleanupCtx, cleanupCancel := context.WithTimeout(cleanupCtx, 60*time.Second)
	defer cleanupCancel()
	for _, phase := range alwaysPhases {
		pr := e.runPhase(cleanupCtx, actx, phase)
		result.Phases = append(result.Phases, pr)
	}

	result.Duration = time.Since(start)
	if !failed {
		result.Status = StatusPass
	}

	// Preserve all final vars in the result for downstream reporting.
	if len(actx.Vars) > 0 {
		result.Vars = make(map[string]string, len(actx.Vars))
		for k, v := range actx.Vars {
			result.Vars[k] = v
		}
	}

	return result
}

// runPhase executes a single phase (sequential or parallel).
func (e *Engine) runPhase(ctx context.Context, actx *ActionContext, phase Phase) PhaseResult {
	start := time.Now()
	pr := PhaseResult{
		Name:   phase.Name,
		Status: StatusPass,
	}

	e.log("[phase] %s", phase.Name)

	if phase.Parallel {
		pr = e.runPhaseParallel(ctx, actx, phase)
	} else {
		pr = e.runPhaseSequential(ctx, actx, phase)
	}

	pr.Duration = time.Since(start)
	return pr
}

func (e *Engine) runPhaseSequential(ctx context.Context, actx *ActionContext, phase Phase) PhaseResult {
	pr := PhaseResult{
		Name:   phase.Name,
		Status: StatusPass,
	}

	for i, act := range phase.Actions {
		ar := e.runAction(ctx, actx, act)
		pr.Actions = append(pr.Actions, ar)
		if ar.Status == StatusFail && !act.IgnoreError {
			pr.Status = StatusFail
			pr.Error = fmt.Sprintf("action %d (%s) failed: %s", i, act.Action, ar.Error)
			return pr
		}
	}
	return pr
}

func (e *Engine) runPhaseParallel(ctx context.Context, actx *ActionContext, phase Phase) PhaseResult {
	pr := PhaseResult{
		Name:   phase.Name,
		Status: StatusPass,
	}

	results := make([]ActionResult, len(phase.Actions))
	var wg sync.WaitGroup
	for i, act := range phase.Actions {
		wg.Add(1)
		go func(idx int, a Action) {
			defer wg.Done()
			results[idx] = e.runAction(ctx, actx, a)
		}(i, act)
	}
	wg.Wait()

	var errors []string
	for i, ar := range results {
		pr.Actions = append(pr.Actions, ar)
		if ar.Status == StatusFail && !phase.Actions[i].IgnoreError {
			pr.Status = StatusFail
			errors = append(errors, fmt.Sprintf("action %d (%s): %s", i, phase.Actions[i].Action, ar.Error))
		}
	}
	if len(errors) == 1 {
		pr.Error = errors[0]
	} else if len(errors) > 1 {
		pr.Error = fmt.Sprintf("%d actions failed: [1] %s", len(errors), strings.Join(errors, "; "))
	}
	return pr
}

// runAction resolves variables and executes a single action.
func (e *Engine) runAction(ctx context.Context, actx *ActionContext, act Action) ActionResult {
	start := time.Now()

	// Resolve variables in the action.
	resolved := resolveAction(act, actx.Vars)

	// Serialize resolved action to YAML for report display.
	yamlDef := marshalActionYAML(resolved)

	handler, err := e.registry.Get(resolved.Action)
	if err != nil {
		return ActionResult{
			Action:   resolved.Action,
			Status:   StatusFail,
			Duration: time.Since(start),
			Error:    err.Error(),
		}
	}

	// Handle delay param.
	if d, ok := resolved.Params["delay"]; ok {
		dur, err := time.ParseDuration(d)
		if err == nil {
			e.log("  [delay] %s", d)
			select {
			case <-time.After(dur):
			case <-ctx.Done():
				return ActionResult{
					Action:   resolved.Action,
					Status:   StatusFail,
					Duration: time.Since(start),
					Error:    ctx.Err().Error(),
				}
			}
		}
	}

	// Enforce action-level timeout if specified.
	var actionTimeout time.Duration
	if resolved.Timeout != "" {
		if dur, err := time.ParseDuration(resolved.Timeout); err == nil && dur > 0 {
			actionTimeout = dur
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, dur)
			defer cancel()
		}
	}

	// Log action start with context (node/target if available).
	actionLabel := resolved.Action
	if resolved.Node != "" {
		actionLabel += " @" + resolved.Node
	} else if resolved.Target != "" {
		actionLabel += " >" + resolved.Target
	}
	e.log("  [action] %s", actionLabel)

	output, err := handler.Execute(ctx, actx, resolved)
	elapsed := time.Since(start)

	// Enrich timeout errors with action-specific context.
	if err != nil && ctx.Err() != nil && actionTimeout > 0 {
		err = fmt.Errorf("action %q timed out after %s: %w", resolved.Action, actionTimeout, err)
	}

	ar := ActionResult{
		Action:   resolved.Action,
		Duration: elapsed,
		YAML:     yamlDef,
	}

	if err != nil {
		ar.Status = StatusFail
		ar.Error = err.Error()
		if act.IgnoreError {
			ar.Status = StatusPass
			e.log("  [done] %s (ignored error, %s): %v", actionLabel, fmtDuration(elapsed), err)
		} else {
			e.log("  [FAIL] %s (%s): %v", actionLabel, fmtDuration(elapsed), err)
		}
	} else {
		ar.Status = StatusPass
		// Only log completion for slow actions (>1s) to avoid noise on quick ones.
		if elapsed >= time.Second {
			e.log("  [done] %s (%s)", actionLabel, fmtDuration(elapsed))
		}
	}

	// Store output as var if save_as is set.
	if resolved.SaveAs != "" && output != nil {
		if v, ok := output["value"]; ok {
			actx.Vars[resolved.SaveAs] = v
			e.log("  [var] %s = %s", resolved.SaveAs, truncate(v, 60))
		}
	}

	// Store all output keys with double-underscore prefix for cleanup vars.
	if output != nil {
		for k, v := range output {
			if strings.HasPrefix(k, "__") {
				actx.Vars[k] = v
			}
		}
	}

	if output != nil {
		if v, ok := output["value"]; ok {
			ar.Output = truncate(v, 65536)
		}
	}

	return ar
}

// resolveAction substitutes {{ var }} references in the action's fields.
func resolveAction(act Action, vars map[string]string) Action {
	resolved := Action{
		Action:      act.Action,
		Target:      act.Target,
		Replica:     act.Replica,
		Node:        act.Node,
		SaveAs:      act.SaveAs,
		IgnoreError: act.IgnoreError,
		Retry:       act.Retry,
		Timeout:     act.Timeout,
		Params:      make(map[string]string),
	}

	// Copy and resolve params.
	for k, v := range act.Params {
		resolved.Params[k] = resolveVars(v, vars)
	}

	return resolved
}

// resolveVars replaces {{ name }} with the value from vars.
func resolveVars(s string, vars map[string]string) string {
	return varPattern.ReplaceAllStringFunc(s, func(match string) string {
		sub := varPattern.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		name := sub[1]
		if v, ok := vars[name]; ok {
			return v
		}
		return match // leave unresolved
	})
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("...[truncated, %d/%d bytes]", max, len(s))
}

// fmtDuration formats a duration as a human-readable string.
func fmtDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
}

// marshalActionYAML serializes a resolved action to YAML for report display.
func marshalActionYAML(act Action) string {
	data, err := yaml.Marshal(act)
	if err != nil {
		return ""
	}
	return string(data)
}

// trimOutliers removes the top and bottom pct% of values.
// E.g. pct=20 on 10 values removes the 2 lowest and 2 highest, returning 6.
// Returns a copy; does not modify the input.
func trimOutliers(values []float64, pct int) []float64 {
	if len(values) <= 2 || pct <= 0 {
		return values
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	trim := int(math.Round(float64(len(sorted)) * float64(pct) / 100.0))
	if trim*2 >= len(sorted) {
		// Can't trim more than half from each end; keep at least 1.
		trim = (len(sorted) - 1) / 2
	}
	return sorted[trim : len(sorted)-trim]
}

