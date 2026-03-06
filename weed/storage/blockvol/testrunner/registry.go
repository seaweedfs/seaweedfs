package testrunner

import (
	"context"
	"fmt"
)

// ActionContext provides action handlers with access to the scenario's
// infrastructure (nodes, targets) and variable store.
type ActionContext struct {
	Scenario    *Scenario
	Nodes       map[string]NodeRunner
	Targets     map[string]TargetRunner
	Vars        map[string]string
	Log         func(format string, args ...interface{})
	Coordinator *Coordinator // non-nil when running in coordinator mode
}

// NodeRunner abstracts remote command execution (implemented by infra.Node).
type NodeRunner interface {
	Run(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error)
	RunRoot(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error)
	Upload(local, remote string) error
	Close()
}

// TargetRunner abstracts iSCSI target lifecycle (implemented by infra.HATarget).
type TargetRunner interface {
	Start(ctx context.Context, create bool) error
	Stop(ctx context.Context) error
	Kill9() error
	CollectLog() (string, error)
	Cleanup(ctx context.Context)
	PID() int
	VolFilePath() string
}

// HATargetRunner extends TargetRunner with HA admin operations.
type HATargetRunner interface {
	TargetRunner
	Assign(ctx context.Context, epoch uint64, role uint32, leaseTTLMs uint32) error
	Status(ctx context.Context) (*StatusResult, error)
	SetReplica(ctx context.Context, dataAddr, ctrlAddr string) error
	WaitForRole(ctx context.Context, expectedRole string) error
	WaitForLSN(ctx context.Context, minLSN uint64) error
	StartRebuildEndpoint(ctx context.Context, listenAddr string) error
	StartRebuildClient(ctx context.Context, rebuildAddr string, epoch uint64) error
	StopRebuildEndpoint(ctx context.Context) error
}

// StatusResult mirrors the JSON from GET /status.
type StatusResult struct {
	Epoch      uint64 `json:"epoch"`
	Role       string `json:"role"`
	WALHeadLSN uint64 `json:"wal_head_lsn"`
	HasLease   bool   `json:"has_lease"`
	Healthy    bool   `json:"healthy"`
}

// ActionHandler executes a single action type.
type ActionHandler interface {
	Execute(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error)
}

// ActionHandlerFunc adapts a function to the ActionHandler interface.
type ActionHandlerFunc func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error)

func (f ActionHandlerFunc) Execute(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
	return f(ctx, actx, act)
}

// Tier constants for action classification.
const (
	TierCore   = "core"   // exec, sleep, assert_*, print
	TierBlock  = "block"  // start_target, iscsi_*, dd_*, fio_*, assign, metrics...
	TierDevOps = "devops" // start_weed_master, start_weed_volume, cluster_status...
	TierChaos  = "chaos"  // inject_netem, inject_partition, fill_disk, corrupt_wal
)

type actionEntry struct {
	handler ActionHandler
	tier    string
}

// Registry maps action names to handlers with tier-based gating.
type Registry struct {
	handlers     map[string]actionEntry
	EnabledTiers map[string]bool // nil or empty = all tiers allowed
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]actionEntry)}
}

// Register adds a handler for an action name with a tier.
func (r *Registry) Register(name, tier string, h ActionHandler) {
	r.handlers[name] = actionEntry{handler: h, tier: tier}
}

// RegisterFunc adds a function handler for an action name with a tier.
func (r *Registry) RegisterFunc(name, tier string, f ActionHandlerFunc) {
	r.handlers[name] = actionEntry{handler: f, tier: tier}
}

// EnableTiers sets which tiers are allowed. Pass nil or empty to allow all.
func (r *Registry) EnableTiers(tiers []string) {
	if len(tiers) == 0 {
		r.EnabledTiers = nil
		return
	}
	r.EnabledTiers = make(map[string]bool, len(tiers))
	for _, t := range tiers {
		r.EnabledTiers[t] = true
	}
}

// Get returns the handler for an action name, or an error.
// Returns an error if the action's tier is not enabled.
func (r *Registry) Get(name string) (ActionHandler, error) {
	entry, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown action: %q", name)
	}
	if len(r.EnabledTiers) > 0 && !r.EnabledTiers[entry.tier] {
		return nil, fmt.Errorf("action %q requires tier %q (enabled: %v)", name, entry.tier, r.tierList())
	}
	return entry.handler, nil
}

// List returns all registered action names (respecting tier gating).
func (r *Registry) List() []string {
	names := make([]string, 0, len(r.handlers))
	for name, entry := range r.handlers {
		if len(r.EnabledTiers) > 0 && !r.EnabledTiers[entry.tier] {
			continue
		}
		names = append(names, name)
	}
	return names
}

// ListByTier returns action names grouped by tier.
func (r *Registry) ListByTier() map[string][]string {
	result := make(map[string][]string)
	for name, entry := range r.handlers {
		result[entry.tier] = append(result[entry.tier], name)
	}
	return result
}

// ActionTier returns the tier for a registered action, or "" if not found.
func (r *Registry) ActionTier(name string) string {
	if entry, ok := r.handlers[name]; ok {
		return entry.tier
	}
	return ""
}

func (r *Registry) tierList() []string {
	list := make([]string, 0, len(r.EnabledTiers))
	for t := range r.EnabledTiers {
		list = append(list, t)
	}
	return list
}
