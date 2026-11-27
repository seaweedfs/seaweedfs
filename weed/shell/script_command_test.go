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
		{"volume.list", []string{"-apply"}, false},
		{"no.such.command", []string{"-apply"}, false},
	}
	for _, c := range cases {
		if got := CommandArgsApply(c.name, c.args); got != c.want {
			t.Errorf("CommandArgsApply(%q, %q) = %v, want %v", c.name, c.args, got, c.want)
		}
	}
}
