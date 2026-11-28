package shell

import "testing"

func TestCommandArgsApply(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"ec.balance", nil, false},
		{"ec.balance", []string{"-apply"}, true},
		{"ec.balance", []string{"-volumeIds=1,2", "-apply"}, true},
		{"ec.balance", []string{"-volumeIds=not-a-volume-id", "-apply"}, false},
		{"ec.balance", []string{"-bogus", "-apply"}, false},
		{"volume.balance", nil, false},
		{"volume.balance", []string{"-force"}, true},
		{"volume.balance", []string{"-volumeBy=FULL", "-apply"}, true},
		{"volume.balance", []string{"-volumeBy=SOME", "-apply"}, false},
		{"volume.balance", []string{"-maxParallelization=0", "-apply"}, false},
		{"volume.balance", []string{"-apply", "-force=false"}, false},
		{"ec.encode", nil, true},
		{"ec.encode", []string{"-collection=a", "-quietFor=1h"}, true},
		{"ec.encode", []string{"-quietFor=soon"}, false},
		{"volume.vacuum", nil, true},
		{"volume.vacuum", []string{"-garbageThreshold=0.2"}, true},
		{"volume.vacuum", []string{"-apply"}, false},
		{"volume.list", []string{"-apply"}, false},
		{"no.such.command", []string{"-apply"}, false},
	}
	for _, c := range cases {
		if got := CommandArgsApply(c.name, c.args); got != c.want {
			t.Errorf("CommandArgsApply(%q, %q) = %v, want %v", c.name, c.args, got, c.want)
		}
	}
}
