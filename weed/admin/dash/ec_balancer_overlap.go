package dash

import (
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

const (
	ecBalancerPlugin      = "plugin ec_balance job"
	ecBalancerLegacy      = "legacy maintenance ec_balance task (turn off with enabled = false under [maintenance] in admin.toml)"
	ecBalancerAdminScript = "admin_script job running ec.balance -apply"

	pluginECBalanceJobType   = "ec_balance"
	pluginAdminScriptJobType = "admin_script"
)

type ecBalancerState struct {
	PluginECBalanceEnabled bool
	LegacyECBalanceEnabled bool
	AdminScriptEnabled     bool
	AdminScript            string
}

func activeECBalancers(state ecBalancerState) []string {
	var active []string
	if state.PluginECBalanceEnabled {
		active = append(active, ecBalancerPlugin)
	}
	if state.LegacyECBalanceEnabled {
		active = append(active, ecBalancerLegacy)
	}
	if state.AdminScriptEnabled && scriptAppliesECBalance(state.AdminScript) {
		active = append(active, ecBalancerAdminScript)
	}
	return active
}

func scriptAppliesECBalance(script string) bool {
	for _, line := range strings.Split(strings.ReplaceAll(script, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, command := range strings.Split(line, ";") {
			fields := strings.Fields(command)
			if len(fields) == 0 || fields[0] != "ec.balance" {
				continue
			}
			for _, arg := range fields[1:] {
				if strings.HasPrefix(arg, "#") {
					break
				}
				name, value, hasValue := strings.Cut(strings.TrimLeft(strings.Trim(arg, "\"'"), "-"), "=")
				if name != "apply" && name != "force" {
					continue
				}
				if !hasValue {
					return true
				}
				if on, err := strconv.ParseBool(value); err == nil && on {
					return true
				}
			}
		}
	}
	return false
}

func (s *AdminServer) collectECBalancerState() ecBalancerState {
	var state ecBalancerState
	if s.maintenanceManager != nil {
		state.LegacyECBalanceEnabled = s.maintenanceManager.IsTaskTypeEnabled(maintenance.MaintenanceTaskType(types.TaskTypeECBalance))
	}
	pluginSvc := s.GetPlugin()
	if pluginSvc == nil {
		return state
	}
	states, err := pluginSvc.ListSchedulerStates()
	if err != nil {
		glog.V(1).Infof("ec balancer check: list scheduler states: %v", err)
		return state
	}
	for _, jobState := range states {
		switch jobState.JobType {
		case pluginECBalanceJobType:
			state.PluginECBalanceEnabled = jobState.Enabled
		case pluginAdminScriptJobType:
			state.AdminScriptEnabled = jobState.Enabled
		}
	}
	if state.AdminScriptEnabled {
		cfg, err := pluginSvc.LoadJobTypeConfig(pluginAdminScriptJobType)
		if err != nil {
			glog.V(1).Infof("ec balancer check: load %s config: %v", pluginAdminScriptJobType, err)
		} else if cfg != nil {
			state.AdminScript = cfg.GetAdminConfigValues()["script"].GetStringValue()
		}
	}
	return state
}

// ActiveECBalancers lists the EC balancers that would move shards on their own.
// More than one entry means they plan moves from the same topology independently.
func (s *AdminServer) ActiveECBalancers() []string {
	return activeECBalancers(s.collectECBalancerState())
}

func (s *AdminServer) warnOnECBalancerOverlap(previous string) string {
	active := s.ActiveECBalancers()
	if len(active) < 2 {
		return ""
	}
	current := strings.Join(active, ", ")
	if current != previous {
		glog.Warningf("%d EC balancers are active and plan shard moves independently: %s. Keep only one enabled.", len(active), current)
	}
	return current
}
