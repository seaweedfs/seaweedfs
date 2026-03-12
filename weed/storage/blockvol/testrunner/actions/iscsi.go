package actions

import (
	"context"
	"fmt"
	"strconv"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterISCSIActions registers iSCSI client actions.
func RegisterISCSIActions(r *tr.Registry) {
	r.RegisterFunc("iscsi_login", tr.TierBlock, iscsiLogin)
	r.RegisterFunc("iscsi_login_direct", tr.TierBlock, iscsiLoginDirect)
	r.RegisterFunc("iscsi_logout", tr.TierBlock, iscsiLogout)
	r.RegisterFunc("iscsi_discover", tr.TierBlock, iscsiDiscover)
	r.RegisterFunc("iscsi_cleanup", tr.TierBlock, iscsiCleanup)
}

// iscsiLogin discovers + logs into the target, returns the device path.
func iscsiLogin(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("iscsi_login: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("iscsi_login: target %q not in scenario", targetName)
	}

	host, err := getTargetHost(actx, targetName)
	if err != nil {
		return nil, err
	}

	// Get the initiator node (first available or explicit).
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_login: %w", err)
	}

	client := infra.NewISCSIClient(node)
	iqn := spec.IQN()
	port := spec.ISCSIPort

	actx.Log("  discovering %s:%d ...", host, port)
	iqns, err := client.Discover(ctx, host, port)
	if err != nil {
		return nil, fmt.Errorf("iscsi_login discover: %w", err)
	}

	// Find matching IQN.
	found := false
	for _, q := range iqns {
		if q == iqn {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("iscsi_login: IQN %s not found in discovery (got %v)", iqn, iqns)
	}

	actx.Log("  logging in to %s ...", iqn)
	dev, err := client.Login(ctx, iqn)
	if err != nil {
		return nil, fmt.Errorf("iscsi_login: %w", err)
	}

	actx.Log("  device: %s", dev)
	return map[string]string{"value": dev}, nil
}

// iscsiLoginDirect discovers + logs into a target using explicit host, port, iqn params.
// Unlike iscsi_login, it does not require a target spec — useful for cluster-provisioned
// volumes whose iSCSI address comes from the master API response.
func iscsiLoginDirect(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	host := act.Params["host"]
	if host == "" {
		return nil, fmt.Errorf("iscsi_login_direct: host param required")
	}
	portStr := act.Params["port"]
	if portStr == "" {
		return nil, fmt.Errorf("iscsi_login_direct: port param required")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("iscsi_login_direct: invalid port %q: %w", portStr, err)
	}
	iqn := act.Params["iqn"]
	if iqn == "" {
		return nil, fmt.Errorf("iscsi_login_direct: iqn param required")
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_login_direct: %w", err)
	}

	client := infra.NewISCSIClient(node)

	actx.Log("  discovering %s:%d ...", host, port)
	iqns, derr := client.Discover(ctx, host, port)
	if derr != nil {
		return nil, fmt.Errorf("iscsi_login_direct discover: %w", derr)
	}

	found := false
	for _, q := range iqns {
		if q == iqn {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("iscsi_login_direct: IQN %s not found in discovery (got %v)", iqn, iqns)
	}

	actx.Log("  logging in to %s ...", iqn)
	dev, lerr := client.Login(ctx, iqn)
	if lerr != nil {
		return nil, fmt.Errorf("iscsi_login_direct: %w", lerr)
	}

	actx.Log("  device: %s", dev)
	return map[string]string{"value": dev}, nil
}

func iscsiLogout(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("iscsi_logout: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("iscsi_logout: target %q not in scenario", targetName)
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_logout: %w", err)
	}

	client := infra.NewISCSIClient(node)
	return nil, client.Logout(ctx, spec.IQN())
}

func iscsiDiscover(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("iscsi_discover: target is required")
	}

	spec, ok := actx.Scenario.Targets[targetName]
	if !ok {
		return nil, fmt.Errorf("iscsi_discover: target %q not in scenario", targetName)
	}

	host, err := getTargetHost(actx, targetName)
	if err != nil {
		return nil, err
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_discover: %w", err)
	}

	client := infra.NewISCSIClient(node)
	iqns, err := client.Discover(ctx, host, spec.ISCSIPort)
	if err != nil {
		return nil, err
	}

	return map[string]string{"value": fmt.Sprintf("%v", iqns)}, nil
}

func iscsiCleanup(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("iscsi_cleanup: %w", err)
	}

	client := infra.NewISCSIClient(node)
	return nil, client.CleanupAll(ctx, "iqn.2024-01.com.seaweedfs:")
}
