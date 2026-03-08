package testrunner

import (
	"context"
	"fmt"
	"regexp"
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

	// Seed vars from env.
	if actx.Vars == nil {
		actx.Vars = make(map[string]string)
	}
	for k, v := range s.Env {
		actx.Vars[k] = v
	}

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
		for iter := 1; iter <= count; iter++ {
			iterPhase := phase
			if phase.Repeat > 1 {
				iterPhase.Name = fmt.Sprintf("%s[%d/%d]", phase.Name, iter, count)
			}
			pr := e.runPhase(ctx, actx, iterPhase)
			result.Phases = append(result.Phases, pr)
			if pr.Status == StatusFail {
				failed = true
				result.Status = StatusFail
				result.Error = fmt.Sprintf("phase %q failed: %s", iterPhase.Name, pr.Error)
				break
			}
		}
		if failed {
			break
		}
	}

	// Always-phases run regardless of failure.
	for _, phase := range alwaysPhases {
		pr := e.runPhase(ctx, actx, phase)
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

	for i, ar := range results {
		pr.Actions = append(pr.Actions, ar)
		if ar.Status == StatusFail && !phase.Actions[i].IgnoreError {
			pr.Status = StatusFail
			if pr.Error == "" {
				pr.Error = fmt.Sprintf("action %d (%s) failed: %s", i, phase.Actions[i].Action, ar.Error)
			}
		}
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

	e.log("  [action] %s", resolved.Action)

	output, err := handler.Execute(ctx, actx, resolved)

	ar := ActionResult{
		Action:   resolved.Action,
		Duration: time.Since(start),
		YAML:     yamlDef,
	}

	if err != nil {
		ar.Status = StatusFail
		ar.Error = err.Error()
		if act.IgnoreError {
			ar.Status = StatusPass
			e.log("  [action] %s failed (ignored): %v", resolved.Action, err)
		}
	} else {
		ar.Status = StatusPass
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
			ar.Output = truncate(v, 4096)
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
	return s[:max] + "..."
}

// marshalActionYAML serializes a resolved action to YAML for report display.
func marshalActionYAML(act Action) string {
	data, err := yaml.Marshal(act)
	if err != nil {
		return ""
	}
	return string(data)
}
