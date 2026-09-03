package shell

import (
	"errors"
	"io"
	"strings"
	"testing"
)

type fakeCommand struct {
	name string
	err  error
}

func (c *fakeCommand) Name() string                              { return c.name }
func (c *fakeCommand) Help() string                              { return "test helper" }
func (c *fakeCommand) HasTag(CommandTag) bool                    { return false }
func (c *fakeCommand) Do([]string, *CommandEnv, io.Writer) error { return c.err }

func TestProcessEachCmdReturnsErrors(t *testing.T) {

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

	exit, err = processEachCmd("exit", nil)
	if !exit {
		t.Errorf("exit must request exit")
	}
	if err != nil {
		t.Errorf("exit must not return an error, got: %v", err)
	}

	exit, err = processEachCmd("   ", nil)
	if exit || err != nil {
		t.Errorf("blank input must be a no-op, got exit=%v err=%v", exit, err)
	}
}

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
