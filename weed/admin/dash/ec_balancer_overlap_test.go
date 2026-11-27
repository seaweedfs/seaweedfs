package dash

import (
	"reflect"
	"testing"
)

func TestScriptAppliesECBalance(t *testing.T) {
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
	}
	for _, c := range cases {
		if got := scriptAppliesECBalance(c.script); got != c.want {
			t.Errorf("scriptAppliesECBalance(%q) = %v, want %v", c.script, got, c.want)
		}
	}
}

func TestActiveECBalancers(t *testing.T) {
	cases := []struct {
		name  string
		state ecBalancerState
		want  []string
	}{
		{"none", ecBalancerState{}, nil},
		{"plugin only", ecBalancerState{PluginECBalanceEnabled: true}, []string{ecBalancerPlugin}},
		{"disabled script ignored", ecBalancerState{PluginECBalanceEnabled: true, AdminScript: "ec.balance -apply"}, []string{ecBalancerPlugin}},
		{"script without ec.balance", ecBalancerState{PluginECBalanceEnabled: true, AdminScriptEnabled: true, AdminScript: "volume.fix.replication -apply"}, []string{ecBalancerPlugin}},
		{"all three", ecBalancerState{PluginECBalanceEnabled: true, LegacyECBalanceEnabled: true, AdminScriptEnabled: true, AdminScript: "ec.balance -apply"}, []string{ecBalancerPlugin, ecBalancerLegacy, ecBalancerAdminScript}},
		{"legacy and script", ecBalancerState{LegacyECBalanceEnabled: true, AdminScriptEnabled: true, AdminScript: "ec.balance -force"}, []string{ecBalancerLegacy, ecBalancerAdminScript}},
	}
	for _, c := range cases {
		if got := activeECBalancers(c.state); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: activeECBalancers = %v, want %v", c.name, got, c.want)
		}
	}
}
