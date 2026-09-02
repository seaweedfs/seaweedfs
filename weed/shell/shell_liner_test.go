package shell

import (
	"strings"
	"testing"
)

// Non-interactive shell runs must be able to report failure: processEachCmd
// returns the command error so RunShell can propagate it to a non-zero process
// exit (previously a failed piped run printed "error: ..." and exited 0).
func TestProcessEachCmdReturnsErrors(t *testing.T) {

	// An unknown command is an error a script must see.
	exit, err := processEachCmd("definitely.not.a.command", nil)
	if exit {
		t.Errorf("unknown command must not request exit")
	}
	if err == nil {
		t.Errorf("unknown command must return an error")
	}
	if err != nil && !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("unexpected error for unknown command: %v", err)
	}

	// "exit"/"quit" end the session without an error.
	exit, err = processEachCmd("exit", nil)
	if !exit {
		t.Errorf("exit must request exit")
	}
	if err != nil {
		t.Errorf("exit must not return an error, got: %v", err)
	}

	// A blank line is a no-op.
	exit, err = processEachCmd("   ", nil)
	if exit || err != nil {
		t.Errorf("blank input must be a no-op, got exit=%v err=%v", exit, err)
	}
}
