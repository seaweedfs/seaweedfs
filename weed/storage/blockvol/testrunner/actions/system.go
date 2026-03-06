package actions

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterSystemActions registers system/assert actions.
func RegisterSystemActions(r *tr.Registry) {
	r.RegisterFunc("exec", tr.TierCore, execAction)
	r.RegisterFunc("sleep", tr.TierCore, sleepAction)
	r.RegisterFunc("assert_equal", tr.TierCore, assertEqual)
	r.RegisterFunc("assert_greater", tr.TierCore, assertGreater)
	r.RegisterFunc("assert_status", tr.TierCore, assertStatus)
	r.RegisterFunc("assert_contains", tr.TierCore, assertContains)
	r.RegisterFunc("print", tr.TierCore, printAction)
}

func execAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	cmd := act.Params["cmd"]
	if cmd == "" {
		return nil, fmt.Errorf("exec: cmd param required")
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	root := act.Params["root"] == "true"
	var stdout, stderr string
	var code int
	if root {
		stdout, stderr, code, err = node.RunRoot(ctx, cmd)
	} else {
		stdout, stderr, code, err = node.Run(ctx, cmd)
	}
	if err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("exec: code=%d stderr=%s", code, stderr)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

func sleepAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	d := act.Params["duration"]
	if d == "" {
		d = "1s"
	}

	dur, err := time.ParseDuration(d)
	if err != nil {
		return nil, fmt.Errorf("sleep: invalid duration %q: %w", d, err)
	}

	select {
	case <-time.After(dur):
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func assertEqual(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	actual := act.Params["actual"]
	expected := act.Params["expected"]

	if actual != expected {
		return nil, fmt.Errorf("assert_equal: %q != %q", actual, expected)
	}
	return nil, nil
}

func assertGreater(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	actualStr := act.Params["actual"]
	expectedStr := act.Params["expected"]

	actual, err := strconv.ParseInt(actualStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("assert_greater: cannot parse actual %q as int: %w", actualStr, err)
	}
	expected, err := strconv.ParseInt(expectedStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("assert_greater: cannot parse expected %q as int: %w", expectedStr, err)
	}

	if actual <= expected {
		return nil, fmt.Errorf("assert_greater: %d <= %d", actual, expected)
	}
	return nil, nil
}

func assertStatus(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	tgt, err := getHATarget(actx, act.Target)
	if err != nil {
		return nil, err
	}

	st, err := tgt.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("assert_status: %w", err)
	}

	if role, ok := act.Params["role"]; ok {
		if st.Role != role {
			return nil, fmt.Errorf("assert_status: role %q != expected %q", st.Role, role)
		}
	}
	if healthy, ok := act.Params["healthy"]; ok {
		expectedHealthy := healthy == "true"
		if st.Healthy != expectedHealthy {
			return nil, fmt.Errorf("assert_status: healthy=%v != expected=%v", st.Healthy, expectedHealthy)
		}
	}
	if hasLease, ok := act.Params["has_lease"]; ok {
		expectedLease := hasLease == "true"
		if st.HasLease != expectedLease {
			return nil, fmt.Errorf("assert_status: has_lease=%v != expected=%v", st.HasLease, expectedLease)
		}
	}

	return nil, nil
}

func assertContains(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	haystack := act.Params["value"]
	needle := act.Params["contains"]

	if !strings.Contains(haystack, needle) {
		return nil, fmt.Errorf("assert_contains: %q not found in %q", needle, haystack)
	}
	return nil, nil
}

func printAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	msg := act.Params["msg"]
	if msg == "" {
		msg = act.Params["message"]
	}
	actx.Log("  [print] %s", msg)
	return nil, nil
}
