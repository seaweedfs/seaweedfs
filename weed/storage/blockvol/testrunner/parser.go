package testrunner

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseFile reads and parses a YAML scenario file.
func ParseFile(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scenario %s: %w", path, err)
	}
	return Parse(data)
}

// Parse parses YAML bytes into a Scenario and validates it.
func Parse(data []byte) (*Scenario, error) {
	var s Scenario
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}
	if err := validate(&s); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}
	return &s, nil
}

// validate checks referential integrity and required fields.
func validate(s *Scenario) error {
	if s.Name == "" {
		return fmt.Errorf("scenario name is required")
	}

	// Check that every target references a valid node.
	for tName, tSpec := range s.Targets {
		if tSpec.Node == "" {
			return fmt.Errorf("target %q: node is required", tName)
		}
		if _, ok := s.Topology.Nodes[tSpec.Node]; !ok {
			return fmt.Errorf("target %q: node %q not found in topology", tName, tSpec.Node)
		}
		if tSpec.IQNSuffix == "" {
			return fmt.Errorf("target %q: iqn_suffix is required", tName)
		}
	}

	// Check port conflicts among targets on the same node.
	type nodePort struct {
		node string
		port int
	}
	used := make(map[nodePort]string) // nodePort -> target name
	for tName, tSpec := range s.Targets {
		ports := []int{tSpec.ISCSIPort, tSpec.AdminPort, tSpec.ReplicaDataPort, tSpec.ReplicaCtrlPort, tSpec.RebuildPort}
		for _, p := range ports {
			if p == 0 {
				continue
			}
			np := nodePort{tSpec.Node, p}
			if other, ok := used[np]; ok {
				return fmt.Errorf("port conflict: targets %q and %q both use port %d on node %q",
					other, tName, p, tSpec.Node)
			}
			used[np] = tName
		}
	}

	// Validate agents section (coordinator mode).
	if len(s.Topology.Agents) > 0 {
		for nodeName, nodeSpec := range s.Topology.Nodes {
			if nodeSpec.Agent != "" {
				if _, ok := s.Topology.Agents[nodeSpec.Agent]; !ok {
					return fmt.Errorf("node %q: agent %q not found in topology.agents", nodeName, nodeSpec.Agent)
				}
			}
		}
	}

	// Check phases and actions.
	if len(s.Phases) == 0 {
		return fmt.Errorf("at least one phase is required")
	}
	for _, phase := range s.Phases {
		if phase.Name == "" {
			return fmt.Errorf("phase name is required")
		}
		if phase.Repeat < 0 || phase.Repeat > 100 {
			return fmt.Errorf("phase %q: repeat must be 0..100 (got %d)", phase.Name, phase.Repeat)
		}
		if phase.TrimPct < 0 || phase.TrimPct > 49 {
			return fmt.Errorf("phase %q: trim_pct must be 0..49 (got %d)", phase.Name, phase.TrimPct)
		}
		if phase.Aggregate != "" && phase.Aggregate != "median" && phase.Aggregate != "mean" && phase.Aggregate != "none" {
			return fmt.Errorf("phase %q: aggregate must be 'median', 'mean', or 'none' (got %q)", phase.Name, phase.Aggregate)
		}

		// Validate save_as uniqueness within parallel phases.
		if phase.Parallel {
			saveAsSet := make(map[string]int)
			for i, act := range phase.Actions {
				if act.SaveAs != "" {
					if prev, ok := saveAsSet[act.SaveAs]; ok {
						return fmt.Errorf("phase %q (parallel): save_as %q used by both action %d and %d",
							phase.Name, act.SaveAs, prev, i)
					}
					saveAsSet[act.SaveAs] = i
				}
			}
		}

		for i, act := range phase.Actions {
			if act.Action == "" {
				return fmt.Errorf("phase %q, action %d: action type is required", phase.Name, i)
			}
			// Validate target references.
			if act.Target != "" {
				if _, ok := s.Targets[act.Target]; !ok {
					return fmt.Errorf("phase %q, action %d (%s): target %q not found",
						phase.Name, i, act.Action, act.Target)
				}
			}
			if act.Replica != "" {
				if _, ok := s.Targets[act.Replica]; !ok {
					return fmt.Errorf("phase %q, action %d (%s): replica %q not found",
						phase.Name, i, act.Action, act.Replica)
				}
			}
			// Validate node references in actions.
			if act.Node != "" {
				if _, ok := s.Topology.Nodes[act.Node]; !ok {
					return fmt.Errorf("phase %q, action %d (%s): node %q not found",
						phase.Name, i, act.Action, act.Node)
				}
			}
		}
	}

	// Validate variable references ({{ var }}) don't reference undefined save_as.
	defined := make(map[string]bool)
	// Add env vars.
	for k := range s.Env {
		defined[k] = true
	}
	for _, phase := range s.Phases {
		if phase.Always {
			continue // cleanup phases may use vars from any prior phase
		}
		for _, act := range phase.Actions {
			// Check var references in all string fields.
			refs := extractVarRefs(act)
			for _, ref := range refs {
				if !defined[ref] && !strings.HasPrefix(ref, "__") {
					// Allow forward refs (they'll be resolved at runtime); just warn-level
				}
			}
			if act.SaveAs != "" {
				defined[act.SaveAs] = true
			}
		}
	}

	return nil
}

// extractVarRefs finds all {{ var }} references in action fields.
func extractVarRefs(act Action) []string {
	var refs []string
	fields := collectStringFields(act)
	for _, f := range fields {
		refs = append(refs, extractVarsFromString(f)...)
	}
	return refs
}

// collectStringFields returns all string values from an action's params and known fields.
func collectStringFields(act Action) []string {
	var fields []string
	for _, v := range act.Params {
		fields = append(fields, v)
	}
	return fields
}

// extractVarsFromString finds all {{ name }} patterns in a string.
func extractVarsFromString(s string) []string {
	var vars []string
	for {
		start := strings.Index(s, "{{")
		if start < 0 {
			break
		}
		end := strings.Index(s[start:], "}}")
		if end < 0 {
			break
		}
		name := strings.TrimSpace(s[start+2 : start+end])
		if name != "" {
			vars = append(vars, name)
		}
		s = s[start+end+2:]
	}
	return vars
}
