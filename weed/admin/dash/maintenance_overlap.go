package dash

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

const pluginAdminScriptJobType = "admin_script"

type maintenanceWork struct {
	name           string
	pluginJobType  string
	legacyTaskType types.TaskType
	shellCommand   string
}

// duplicableMaintenanceWork lists work that the plugin scheduler, the legacy
// maintenance manager, and an admin_script can each run on their own.
var duplicableMaintenanceWork = []maintenanceWork{
	{"volume balance", "volume_balance", types.TaskTypeBalance, "volume.balance"},
	{"EC balance", "ec_balance", types.TaskTypeECBalance, "ec.balance"},
	{"EC encoding", "erasure_coding", types.TaskTypeErasureCoding, "ec.encode"},
	{"vacuum", "vacuum", types.TaskTypeVacuum, "volume.vacuum"},
}

// MaintenanceOverlap names work that more than one scheduler runs.
type MaintenanceOverlap struct {
	Work    string   `json:"work"`
	Runners []string `json:"runners"`
}

type maintenanceRunnerState struct {
	PluginEnabled      map[string]bool
	LegacyEnabled      map[types.TaskType]bool
	AdminScriptEnabled bool
	AdminScript        string
}

func maintenanceOverlaps(state maintenanceRunnerState) []MaintenanceOverlap {
	var scriptCommands map[string]bool
	if state.AdminScriptEnabled {
		scriptCommands = scriptAppliedCommands(state.AdminScript)
	}
	var overlaps []MaintenanceOverlap
	for _, work := range duplicableMaintenanceWork {
		var runners []string
		if state.PluginEnabled[work.pluginJobType] {
			runners = append(runners, fmt.Sprintf("plugin %s job", work.pluginJobType))
		}
		if state.LegacyEnabled[work.legacyTaskType] {
			runners = append(runners, fmt.Sprintf("legacy maintenance %s task (turn off with enabled = false under [maintenance] in admin.toml)", work.legacyTaskType))
		}
		if scriptCommands[work.shellCommand] {
			runners = append(runners, fmt.Sprintf("%s job running %s", pluginAdminScriptJobType, work.shellCommand))
		}
		if len(runners) > 1 {
			overlaps = append(overlaps, MaintenanceOverlap{Work: work.name, Runners: runners})
		}
	}
	return overlaps
}

func scriptAppliedCommands(script string) map[string]bool {
	applied := make(map[string]bool)
	for _, line := range strings.Split(strings.ReplaceAll(script, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, chunk := range strings.Split(line, ";") {
			name, args, ok := shell.SplitScriptCommand(strings.TrimSpace(chunk))
			if ok && shell.CommandArgsApply(name, args) {
				applied[name] = true
			}
		}
	}
	return applied
}

func (s *AdminServer) collectMaintenanceRunnerState() maintenanceRunnerState {
	state := maintenanceRunnerState{
		PluginEnabled: make(map[string]bool),
		LegacyEnabled: make(map[types.TaskType]bool),
	}
	if s.maintenanceManager != nil {
		for _, work := range duplicableMaintenanceWork {
			state.LegacyEnabled[work.legacyTaskType] = s.maintenanceManager.IsTaskTypeEnabled(maintenance.MaintenanceTaskType(work.legacyTaskType))
		}
	}
	pluginSvc := s.GetPlugin()
	if pluginSvc == nil {
		return state
	}
	states, err := pluginSvc.ListSchedulerStates()
	if err != nil {
		glog.V(1).Infof("maintenance overlap check: list scheduler states: %v", err)
		return state
	}
	for _, jobState := range states {
		state.PluginEnabled[jobState.JobType] = jobState.Enabled
	}
	state.AdminScriptEnabled = state.PluginEnabled[pluginAdminScriptJobType]
	if state.AdminScriptEnabled {
		cfg, err := pluginSvc.LoadJobTypeConfig(pluginAdminScriptJobType)
		if err != nil {
			glog.V(1).Infof("maintenance overlap check: load %s config: %v", pluginAdminScriptJobType, err)
		} else if cfg != nil {
			state.AdminScript = cfg.GetAdminConfigValues()["script"].GetStringValue()
		}
	}
	return state
}

// MaintenanceOverlaps lists the work that more than one scheduler runs on its own.
// Each runner plans from the same topology without knowing about the others.
func (s *AdminServer) MaintenanceOverlaps() []MaintenanceOverlap {
	return maintenanceOverlaps(s.collectMaintenanceRunnerState())
}

func (s *AdminServer) warnOnMaintenanceOverlap(previous string) string {
	var lines []string
	for _, overlap := range s.MaintenanceOverlaps() {
		lines = append(lines, fmt.Sprintf("%s has %d independent runners: %s", overlap.Work, len(overlap.Runners), strings.Join(overlap.Runners, ", ")))
	}
	current := strings.Join(lines, "; ")
	if current != previous {
		for _, line := range lines {
			glog.Warningf("%s. Keep only one enabled.", line)
		}
	}
	return current
}
