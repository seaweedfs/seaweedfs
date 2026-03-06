package testrunner

import (
	"context"
	"sort"
	"testing"
)

func TestRegistry_TierGating(t *testing.T) {
	r := NewRegistry()
	noop := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	r.RegisterFunc("exec", TierCore, noop)
	r.RegisterFunc("dd_write", TierBlock, noop)
	r.RegisterFunc("inject_netem", TierChaos, noop)
	r.RegisterFunc("start_weed_master", TierDevOps, noop)

	// No gating — all accessible.
	if _, err := r.Get("exec"); err != nil {
		t.Errorf("exec ungated: %v", err)
	}
	if _, err := r.Get("dd_write"); err != nil {
		t.Errorf("dd_write ungated: %v", err)
	}

	// Enable only core.
	r.EnableTiers([]string{TierCore})
	if _, err := r.Get("exec"); err != nil {
		t.Errorf("exec (core enabled): %v", err)
	}
	if _, err := r.Get("dd_write"); err == nil {
		t.Error("dd_write should be blocked when only core is enabled")
	}
	if _, err := r.Get("inject_netem"); err == nil {
		t.Error("inject_netem should be blocked")
	}

	// List should only return core actions.
	names := r.List()
	if len(names) != 1 || names[0] != "exec" {
		t.Errorf("List() = %v, want [exec]", names)
	}

	// Reset gating.
	r.EnableTiers(nil)
	names = r.List()
	if len(names) != 4 {
		t.Errorf("List() after reset = %d, want 4", len(names))
	}
}

func TestRegistry_ListByTier(t *testing.T) {
	r := NewRegistry()
	noop := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	r.RegisterFunc("a", TierCore, noop)
	r.RegisterFunc("b", TierCore, noop)
	r.RegisterFunc("c", TierBlock, noop)

	byTier := r.ListByTier()
	coreActions := byTier[TierCore]
	sort.Strings(coreActions)
	if len(coreActions) != 2 || coreActions[0] != "a" || coreActions[1] != "b" {
		t.Errorf("core = %v, want [a b]", coreActions)
	}
	if len(byTier[TierBlock]) != 1 || byTier[TierBlock][0] != "c" {
		t.Errorf("block = %v, want [c]", byTier[TierBlock])
	}
}

func TestRegistry_ActionTier(t *testing.T) {
	r := NewRegistry()
	noop := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	r.RegisterFunc("foo", TierChaos, noop)

	if tier := r.ActionTier("foo"); tier != TierChaos {
		t.Errorf("tier = %q, want chaos", tier)
	}
	if tier := r.ActionTier("nonexistent"); tier != "" {
		t.Errorf("nonexistent tier = %q, want empty", tier)
	}
}

func TestRegistry_EmptyTiersAllowsAll(t *testing.T) {
	r := NewRegistry()
	noop := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	r.RegisterFunc("x", TierDevOps, noop)

	// Empty slice should not restrict.
	r.EnableTiers([]string{})
	if _, err := r.Get("x"); err != nil {
		t.Errorf("empty tiers should allow all: %v", err)
	}
}
