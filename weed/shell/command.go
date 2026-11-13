package shell

import (
	"errors"
	"flag"
	"io"
)

type command interface {
	Name() string
	Help() string
	Do([]string, *CommandEnv, io.Writer) error
	HasTag(tag CommandTag) bool
}

var (
	Commands = []command{}
)

type CommandTag string

const (
	ResourceHeavy CommandTag = "resourceHeavy"
	Hidden        CommandTag = "hidden"
)

// isHelpError reports whether a command only stopped because it was run
// with -h: the flag package has already printed its usage, so this is not a
// failure.
func isHelpError(err error) bool {
	return errors.Is(err, flag.ErrHelp)
}
