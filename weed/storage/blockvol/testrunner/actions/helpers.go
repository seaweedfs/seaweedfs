package actions

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// getNode retrieves the infra.Node for the named node from the action context.
func getNode(actx *tr.ActionContext, name string) (*infra.Node, error) {
	if name == "" {
		// Try to get the first available node.
		for _, n := range actx.Nodes {
			if nn, ok := n.(*infra.Node); ok {
				return nn, nil
			}
		}
		return nil, fmt.Errorf("no nodes available")
	}
	n, ok := actx.Nodes[name]
	if !ok {
		return nil, fmt.Errorf("node %q not found", name)
	}
	nn, ok := n.(*infra.Node)
	if !ok {
		return nil, fmt.Errorf("node %q is not an infra.Node", name)
	}
	return nn, nil
}

// getTargetNode retrieves the node associated with a target.
func getTargetNode(actx *tr.ActionContext, targetName string) (*infra.Node, error) {
	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("target %q not in scenario", targetName)
	}
	return getNode(actx, spec.Node)
}

// getTargetHost returns the host address for a target's node.
func getTargetHost(actx *tr.ActionContext, targetName string) (string, error) {
	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return "", fmt.Errorf("target %q not in scenario", targetName)
	}
	nodeSpec, ok := actx.Scenario.Topology.Nodes[spec.Node]
	if !ok {
		return "", fmt.Errorf("node %q not in topology", spec.Node)
	}
	if nodeSpec.IsLocal {
		return "127.0.0.1", nil
	}
	return nodeSpec.Host, nil
}

func parseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}

func parseDurationMs(s string) (uint32, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		// Try parsing as plain number (milliseconds).
		ms, err2 := strconv.ParseUint(s, 10, 32)
		if err2 != nil {
			return 0, err
		}
		return uint32(ms), nil
	}
	return uint32(d.Milliseconds()), nil
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func parseIntSlice(s string) []int {
	var result []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if v, err := strconv.Atoi(part); err == nil {
			result = append(result, v)
		}
	}
	return result
}
