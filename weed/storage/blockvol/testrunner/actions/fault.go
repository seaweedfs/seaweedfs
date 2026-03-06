package actions

import (
	"context"
	"fmt"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// RegisterFaultActions registers fault injection actions.
func RegisterFaultActions(r *tr.Registry) {
	r.RegisterFunc("inject_netem", tr.TierChaos, injectNetemAction)
	r.RegisterFunc("inject_partition", tr.TierChaos, injectPartitionAction)
	r.RegisterFunc("fill_disk", tr.TierChaos, fillDiskAction)
	r.RegisterFunc("corrupt_wal", tr.TierChaos, corruptWALAction)
	r.RegisterFunc("clear_fault", tr.TierChaos, clearFaultAction)
}

func injectNetemAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("inject_netem: %w", err)
	}

	targetIP := act.Params["target_ip"]
	if targetIP == "" {
		return nil, fmt.Errorf("inject_netem: target_ip param required")
	}
	delayMs := parseInt(act.Params["delay_ms"], 200)

	cleanupCmd, err := infra.InjectNetem(ctx, node, targetIP, delayMs)
	if err != nil {
		return nil, err
	}

	// Store cleanup cmd for later clear_fault.
	varKey := "__cleanup_netem"
	if act.Node != "" {
		varKey += "_" + act.Node
	}
	return map[string]string{varKey: cleanupCmd}, nil
}

func injectPartitionAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("inject_partition: %w", err)
	}

	targetIP := act.Params["target_ip"]
	if targetIP == "" {
		return nil, fmt.Errorf("inject_partition: target_ip param required")
	}
	ports := parseIntSlice(act.Params["ports"])
	if len(ports) == 0 {
		return nil, fmt.Errorf("inject_partition: ports param required")
	}

	cleanupCmd, err := infra.InjectIptablesDrop(ctx, node, targetIP, ports)
	if err != nil {
		return nil, err
	}

	varKey := "__cleanup_partition"
	if act.Node != "" {
		varKey += "_" + act.Node
	}
	return map[string]string{varKey: cleanupCmd}, nil
}

func fillDiskAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("fill_disk: %w", err)
	}

	dir := act.Params["dir"]
	if dir == "" {
		dir = "/tmp"
	}

	cleanupCmd, err := infra.FillDisk(ctx, node, dir)
	if err != nil {
		return nil, err
	}

	varKey := "__cleanup_fill_disk"
	if act.Node != "" {
		varKey += "_" + act.Node
	}
	return map[string]string{varKey: cleanupCmd}, nil
}

func corruptWALAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	targetName := act.Target
	if targetName == "" {
		return nil, fmt.Errorf("corrupt_wal: target param required")
	}

	tgt, err := getHATarget(actx, targetName)
	if err != nil {
		return nil, err
	}

	nBytes := parseInt(act.Params["bytes"], 4096)

	return nil, infra.CorruptWALRegion(ctx, tgt.Node, tgt.VolFilePath(), nBytes)
}

func clearFaultAction(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	faultType := act.Params["type"]
	if faultType == "" {
		return nil, fmt.Errorf("clear_fault: type param required (netem, partition, fill_disk)")
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, fmt.Errorf("clear_fault: %w", err)
	}

	varKey := "__cleanup_" + faultType
	if act.Node != "" {
		varKey += "_" + act.Node
	}

	cleanupCmd := actx.Vars[varKey]
	if cleanupCmd == "" {
		// Try without node suffix.
		cleanupCmd = actx.Vars["__cleanup_"+faultType]
	}

	return nil, infra.ClearFault(ctx, node, cleanupCmd)
}
