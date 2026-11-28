package dash

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

func TestScriptAppliedCommands(t *testing.T) {
	cases := []struct {
		script string
		want   bool
	}{
		{"", false},
		{"fs.log.purge -daysAgo=7\nvolume.fix.replication -apply", false},
		{"ec.balance", false},
		{"ec.balance -collection=x", false},
		{"ec.balance -apply", true},
		{"ec.balance -force", true},
		{"ec.balance --apply", true},
		{"ec.balance -apply=true", true},
		{"ec.balance -apply=false", false},
		{"ec.balance -apply=FALSE", false},
		{"ec.balance -apply=T", true},
		{"ec.balance -apply=maybe", false},
		{"ec.balance -collection=a # add -apply later", false},
		{"ec.balance -apply=false # use -apply for writes", false},
		{"# ec.balance -apply", false},
		{"volume.list; ec.balance -collection=a -apply", true},
		{"ec.rebuild -apply\r\nec.balance -apply\r\n", true},
		{"ec.balancer -apply", false},
		{"ec.balance -apply=maybe -force", false},
		{"ec.balance -apply -force=maybe", false},
		{"ec.balance -apply -force=false", false},
		{"ec.balance -force=false -apply", false},
		{"ec.balance -apply -apply=false", false},
		{"ec.balance -collection x -apply", true},
		{"ec.balance -collection -apply", false},
		{"ec.balance x -apply", false},
		{"ec.balance -collection \"a b\" -apply", true},
		{"ec.balance -collection 'a b' -force", true},
		{"ec.balance -collection #x -apply", true},
		{"ec.balance -bogus -apply", false},
		{"ec.balance -maxParallelization=x -apply", false},
		{"ec.balance -maxParallelization 4 -apply", true},
		{"ec.balance -volumeIds=x -apply", false},
	}
	for _, c := range cases {
		if got := scriptAppliedCommands(c.script)["ec.balance"]; got != c.want {
			t.Errorf("scriptAppliedCommands(%q) has ec.balance = %v, want %v", c.script, got, c.want)
		}
	}
}

func TestScriptAppliedCommandsAcrossWork(t *testing.T) {
	got := scriptAppliedCommands("volume.balance -apply\nec.encode -collection=a\nvolume.vacuum -garbageThreshold=0.2\nvolume.fix.replication -apply\nvolume.list")
	want := map[string]bool{"volume.balance": true, "ec.encode": true, "volume.vacuum": true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("scriptAppliedCommands = %v, want %v", got, want)
	}
	if got := scriptAppliedCommands("volume.balance\nec.encode -quietFor=soon"); len(got) != 0 {
		t.Errorf("dry-run and rejected commands counted: %v", got)
	}
}

func TestMaintenanceOverlaps(t *testing.T) {
	legacyEC := "legacy maintenance ec_balance task (turn off with enabled = false under [maintenance] in admin.toml)"
	cases := []struct {
		name  string
		state maintenanceRunnerState
		want  []MaintenanceOverlap
	}{
		{"none", maintenanceRunnerState{}, nil},
		{"plugin only", maintenanceRunnerState{PluginEnabled: map[string]bool{"ec_balance": true, "vacuum": true}}, nil},
		{"disabled script ignored", maintenanceRunnerState{
			PluginEnabled: map[string]bool{"ec_balance": true},
			AdminScript:   "ec.balance -apply",
		}, nil},
		{"plugin and legacy on different work", maintenanceRunnerState{
			PluginEnabled: map[string]bool{"vacuum": true},
			LegacyEnabled: map[types.TaskType]bool{types.TaskTypeECBalance: true},
		}, nil},
		{"all three on EC balance", maintenanceRunnerState{
			PluginEnabled:      map[string]bool{"ec_balance": true, "admin_script": true},
			LegacyEnabled:      map[types.TaskType]bool{types.TaskTypeECBalance: true},
			AdminScriptEnabled: true,
			AdminScript:        "ec.balance -apply",
		}, []MaintenanceOverlap{{Work: "EC balance", Runners: []string{"plugin ec_balance job", legacyEC, "admin_script job running ec.balance"}}}},
		{"script overlaps two plugin jobs", maintenanceRunnerState{
			PluginEnabled:      map[string]bool{"volume_balance": true, "vacuum": true, "erasure_coding": true, "admin_script": true},
			AdminScriptEnabled: true,
			AdminScript:        "volume.balance -force\nvolume.vacuum",
		}, []MaintenanceOverlap{
			{Work: "volume balance", Runners: []string{"plugin volume_balance job", "admin_script job running volume.balance"}},
			{Work: "vacuum", Runners: []string{"plugin vacuum job", "admin_script job running volume.vacuum"}},
		}},
		{"legacy and plugin encoding", maintenanceRunnerState{
			PluginEnabled: map[string]bool{"erasure_coding": true},
			LegacyEnabled: map[types.TaskType]bool{types.TaskTypeErasureCoding: true},
		}, []MaintenanceOverlap{{Work: "EC encoding", Runners: []string{"plugin erasure_coding job", "legacy maintenance erasure_coding task (turn off with enabled = false under [maintenance] in admin.toml)"}}}},
	}
	for _, c := range cases {
		if got := maintenanceOverlaps(c.state); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: maintenanceOverlaps = %v, want %v", c.name, got, c.want)
		}
	}
}
