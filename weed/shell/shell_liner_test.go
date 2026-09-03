package shell

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// fakeCommand lets the tests drive the REGISTERED-command path of
// processEachCmd with a controlled Do result.
type fakeCommand struct {
	name string
	err  error
}

func (c *fakeCommand) Name() string                              { return c.name }
func (c *fakeCommand) Help() string                              { return "test helper" }
func (c *fakeCommand) HasTag(CommandTag) bool                    { return false }
func (c *fakeCommand) Do([]string, *CommandEnv, io.Writer) error { return c.err }

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

// The registered-command path: a command whose Do fails must surface that
// exact error (this is what RunShell propagates and the weed shell command
// turns into a non-zero process exit); a succeeding command must not.
func TestProcessEachCmdPreservesRegisteredCommandError(t *testing.T) {
	doErr := errors.New("daily_run: shard=3: recovery walk failed")
	failing := &fakeCommand{name: "test.failing.command", err: doErr}
	succeeding := &fakeCommand{name: "test.succeeding.command"}
	Commands = append(Commands, failing, succeeding)
	t.Cleanup(func() { Commands = Commands[:len(Commands)-2] })

	exit, err := processEachCmd("test.failing.command -some -args", nil)
	if exit {
		t.Errorf("a failing command must not request exit")
	}
	if !errors.Is(err, doErr) {
		t.Errorf("the command's own error must be preserved, got: %v", err)
	}

	exit, err = processEachCmd("test.succeeding.command", nil)
	if exit || err != nil {
		t.Errorf("a succeeding command must return no error, got exit=%v err=%v", exit, err)
	}
}
