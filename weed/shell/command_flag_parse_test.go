package shell

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

// A command that parses flags must report a bad flag as an error. Returning
// nil makes a typo in a weed shell script or a master maintenance script look
// like a successful run that did nothing.
func TestCommandsReturnFlagParseErrors(t *testing.T) {
	for _, c := range Commands {
		t.Run(c.Name(), func(t *testing.T) {
			// fs.meta.load parses every argument but the last as flags.
			err, needsEnv := doWithoutCommandEnv(c, []string{"-notAFlagOfAnyCommand", "arg"})
			if needsEnv {
				t.Skip("uses the command environment before parsing any flags")
			}
			if err == nil {
				t.Fatal("an unknown flag was reported as success")
			}
			if !strings.Contains(err.Error(), "flag provided but not defined") {
				t.Skipf("rejects its arguments without parsing flags: %v", err)
			}
		})
	}
}

// doWithoutCommandEnv runs c with a nil CommandEnv. A command that panics here
// used the environment before parsing flags, or takes no flags at all.
func doWithoutCommandEnv(c command, args []string) (err error, needsEnv bool) {
	defer func() {
		if recover() != nil {
			needsEnv = true
		}
	}()
	var out bytes.Buffer
	return c.Do(args, nil, &out), false
}

func TestProcessEachCmdTreatsHelpAsSuccess(t *testing.T) {
	Commands = append(Commands, &fakeCommand{name: "test.help.command", err: flag.ErrHelp})
	t.Cleanup(func() { Commands = Commands[:len(Commands)-1] })

	exit, err := processEachCmd("test.help.command -h", nil)
	if exit || err != nil {
		t.Errorf("-h must not count as a failure, got exit=%v err=%v", exit, err)
	}
}
